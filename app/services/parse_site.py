from __future__ import annotations

import asyncio
import itertools
import json
import logging
import math
import re
from typing import Any, Iterable, Optional, Sequence

from fastapi import HTTPException
from pydantic import BaseModel, Field
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from urllib.parse import urlparse as _urlparse

from app.config import settings
from app.db.bitrix import bitrix_session
from app.db.parsing import (
    get_clients_request_id as get_clients_request_id_pd,
    get_domains_by_inn as get_domains_by_inn_pd,
    pars_site_insert_chunks as pars_site_insert_chunks_pd,
    pars_site_update_vector,
    push_clients_request as push_clients_request_pd,
)
from app.db.parsing_mirror import (
    get_clients_request_id_pg,
    get_domains_by_inn_pg,
    get_ib_clients_domains_pg,
    pars_site_insert_chunks_pg,
    pars_site_update_metadata_pg,
    push_clients_request_pg,
    get_okved_main_pg,
)
from app.models.bitrix import DaDataResult
from app.repo.bitrix_repo import replace_dadata_raw, upsert_company_summary
from app.services.dadata_client import find_party_by_inn
from app.services.mapping import map_summary_from_dadata
from app.services.analyze_client import fetch_embedding, fetch_site_description
from app.services.scrape import FetchError, fetch_and_chunk, to_home_url

log = logging.getLogger("services.parse_site")


_DOMAIN_IN_TEXT_RE = re.compile(
    r"(?:https?://|http://|ftp://)?(?:www\.)?([a-z0-9.-]+\.[a-z]{2,})",
    re.IGNORECASE,
)

_EMAIL_RE = re.compile(r"[A-Za-z0-9._%+\-]+@[A-Za-z0-9.\-]+\.[A-Za-z]{2,}", re.IGNORECASE)

_PERSONAL_EMAIL_DOMAINS = {
    # RU
    "mail.ru",
    "inbox.ru",
    "bk.ru",
    "list.ru",
    "yandex.ru",
    "ya.ru",
    "yandex.com",
    "rambler.ru",
    "lenta.ru",
    "autorambler.ru",
    "ro.ru",
    # global
    "gmail.com",
    "hotmail.com",
    "outlook.com",
    "live.com",
    "msn.com",
    "icloud.com",
    "me.com",
    "aol.com",
    "protonmail.com",
    "proton.me",
}


_OKVED_ALERT_THRESHOLD = 0.5


class ParseSiteRequest(BaseModel):
    """Параметры запуска parse-site."""

    inn: str = Field(..., min_length=4, max_length=20, description="ИНН для clients_requests.inn")
    parse_domain: str | None = Field(
        None,
        description="Одиночный домен или URL (например, 'uniconf.ru' или 'https://uniconf.ru').",
    )
    parse_domains: list[str] | None = Field(
        None,
        description="Список доменов/URL для парсинга. Переданные значения дополняются доменами из БД.",
    )
    parse_emails: list[str] | None = Field(
        None,
        description="Список email-адресов. Домены из них будут использованы как кандидаты для парсинга.",
    )

    company_name: str | None = Field(
        None,
        description="Название компании; если не задано — подтянем из своей БД (DaDataResult)",
    )
    client_domain_1: str | None = Field(
        None,
        description="clients_requests.domain_1; если не задано — 'www.{первый_домен}'",
    )
    pars_site_domain_1: str | None = Field(
        None,
        description="pars_site.domain_1; если не задано — домен без 'www.'",
    )
    url_override: str | None = Field(
        None,
        description="pars_site.url; если не задано — главная страница",
    )
    save_client_request: bool = Field(
        True,
        description="Создавать запись в clients_requests (по умолчанию — да)",
    )


class ParsedSiteResult(BaseModel):
    requested_domain: str
    used_domain: str | None
    url: str | None
    chunks_inserted: int
    success: bool
    error: str | None = None
    description: str | None = None
    has_description_vector: bool = False
    okved_score: float | None = None


class OkvedAlert(BaseModel):
    domain: str
    score: float


class ParseSiteResponse(BaseModel):
    company_id: int
    domain_1: str | None
    domain_2: str | None
    url: str | None
    planned_domains: list[str]
    successful_domains: list[str]
    chunks_inserted: int
    results: list[ParsedSiteResult]
    okved_text: str | None = None
    okved_alerts: list[OkvedAlert] = Field(default_factory=list)


def _normalize_domain_candidate(domain: str) -> Optional[str]:
    try:
        _, normalized = _normalize_and_split_domain(domain)
        return normalized
    except Exception:
        return None


def _extract_domains_from_value(value: object) -> Iterable[str]:
    if value is None:
        return []

    if isinstance(value, (list, tuple, set)):
        domains: list[str] = []
        for item in value:
            domains.extend(_extract_domains_from_value(item))
        return domains

    if isinstance(value, dict):
        domains: list[str] = []
        for item in value.values():
            domains.extend(_extract_domains_from_value(item))
        return domains

    text = str(value).strip()
    if not text:
        return []

    try:
        parsed = json.loads(text)
    except Exception:  # noqa: BLE001
        parsed = None

    if isinstance(parsed, (list, dict)):
        return list(_extract_domains_from_value(parsed))

    matches = [m.group(1) for m in _DOMAIN_IN_TEXT_RE.finditer(text)]
    return matches


def _normalize_domains(values: Iterable[object]) -> list[str]:
    seen: set[str] = set()
    result: list[str] = []
    for value in values:
        for raw in _extract_domains_from_value(value):
            norm = _normalize_domain_candidate(raw)
            if not norm or norm in seen:
                continue
            seen.add(norm)
            result.append(norm)
    return result


def _parse_vector_literal(literal: str) -> Optional[list[float]]:
    cleaned = (literal or "").strip()
    if not cleaned:
        return None
    if cleaned.startswith("[") and cleaned.endswith("]"):
        cleaned = cleaned[1:-1]
    if not cleaned:
        return []
    parts = [p.strip() for p in cleaned.split(",") if p.strip()]
    values: list[float] = []
    for part in parts:
        try:
            values.append(float(part))
        except (TypeError, ValueError):
            return None
    return values


def _coerce_vector(value: Any) -> Optional[list[float]]:
    if value is None:
        return None
    if isinstance(value, (list, tuple)):
        result: list[float] = []
        try:
            for item in value:
                result.append(float(item))
        except (TypeError, ValueError):
            return None
        return result
    if isinstance(value, dict):
        if "values" in value:
            return _coerce_vector(value.get("values"))
        literal = value.get("literal")
        if isinstance(literal, str):
            return _parse_vector_literal(literal)
    if isinstance(value, str):
        return _parse_vector_literal(value)
    return None


def _vector_to_literal(vector: Sequence[float] | None) -> Optional[str]:
    if vector is None:
        return None
    try:
        values = [float(x) for x in vector]
    except (TypeError, ValueError):
        return None
    if not values:
        return None
    return "[" + ",".join(f"{x:.7f}" for x in values) + "]"


def _cosine_similarity(vec_a: Sequence[float], vec_b: Sequence[float]) -> Optional[float]:
    if not vec_a or not vec_b:
        return None
    if len(vec_a) != len(vec_b):
        return None
    dot = 0.0
    norm_a = 0.0
    norm_b = 0.0
    for a, b in zip(vec_a, vec_b):
        fa = float(a)
        fb = float(b)
        dot += fa * fb
        norm_a += fa * fa
        norm_b += fb * fb
    if norm_a <= 0.0 or norm_b <= 0.0:
        return None
    denom = math.sqrt(norm_a) * math.sqrt(norm_b)
    if denom == 0:
        return None
    return dot / denom


def _resolve_okved_text(main_okved: Optional[str], okveds: Any) -> Optional[str]:
    code = (main_okved or "").strip()
    if not code:
        return None

    if " " in code and code[0].isdigit():
        return code

    items: list[Any] = []
    if isinstance(okveds, dict):
        if isinstance(okveds.get("items"), list):
            items = list(okveds.get("items") or [])
        else:
            items = [
                {"code": k, "name": v}
                for k, v in okveds.items()
                if k != "main"
            ]
    elif isinstance(okveds, list):
        items = list(okveds)

    def _item_code(obj: Any) -> str:
        if isinstance(obj, dict):
            return str(
                obj.get("code")
                or obj.get("value")
                or obj.get("okved")
                or ""
            ).strip()
        if isinstance(obj, str):
            return obj.strip().split(" ", 1)[0]
        return ""

    def _item_name(obj: Any) -> Optional[str]:
        if isinstance(obj, dict):
            for key in ("name", "text", "label"):
                val = obj.get(key)
                if isinstance(val, str) and val.strip():
                    return val.strip()
        if isinstance(obj, str):
            parts = obj.strip().split(" ", 1)
            if len(parts) == 2:
                return parts[1].strip()
        return None

    for item in items:
        if isinstance(item, dict) and isinstance(item.get("items"), list):
            for nested in item.get("items") or []:
                if _item_code(nested) == code:
                    name = _item_name(nested)
                    if name:
                        return f"{code} — {name}" if code not in name else name
        if _item_code(item) == code:
            name = _item_name(item)
            if name:
                return f"{code} — {name}" if code not in name else name

    return code


def _format_score(value: float) -> float:
    try:
        return float(f"{value:.4f}")
    except (TypeError, ValueError):
        return value


async def _generate_site_description(
    text: str,
    *,
    domain: str,
    inn: str,
) -> tuple[Optional[str], Optional[list[float]]]:
    if not text.strip():
        return None, None

    description, vector = await fetch_site_description(
        text,
        embed_model=settings.embed_model,
        label=f"site:{inn}:{domain}",
    )
    if description and not vector:
        vector = await _embed_text(description, label=f"site-desc:{inn}:{domain}")
    return description, vector


async def _embed_text(text: str, *, label: str) -> Optional[list[float]]:
    if not text.strip():
        return None

    vector = await fetch_embedding(text, label=label)
    if vector:
        log.info("parse-site: embedding получен (%s, size=%s)", label, len(vector))
    else:
        log.info("parse-site: embedding отсутствует в ответе (%s)", label)
    return vector


def _extract_emails_from_value(value: object) -> Iterable[str]:
    if value is None:
        return []

    if isinstance(value, (list, tuple, set)):
        emails: list[str] = []
        for item in value:
            emails.extend(_extract_emails_from_value(item))
        return emails

    if isinstance(value, dict):
        emails: list[str] = []
        for item in value.values():
            emails.extend(_extract_emails_from_value(item))
        return emails

    text = str(value).strip()
    if not text:
        return []

    try:
        parsed = json.loads(text)
    except Exception:  # noqa: BLE001
        parsed = None

    if isinstance(parsed, (list, dict)):
        return list(_extract_emails_from_value(parsed))

    return [m.group(0) for m in _EMAIL_RE.finditer(text)]


def _normalize_email_domains(values: Iterable[object]) -> list[str]:
    seen: set[str] = set()
    result: list[str] = []
    for value in values:
        for raw_email in _extract_emails_from_value(value):
            domain_part = raw_email.split("@", 1)[1].lower()
            if domain_part in _PERSONAL_EMAIL_DOMAINS:
                continue
            norm = _normalize_domain_candidate(domain_part)
            if not norm or norm in seen:
                continue
            seen.add(norm)
            result.append(norm)
    return result


async def _collect_domains_by_inn(inn: str, session: AsyncSession) -> list[str]:
    """Возвращает уникальный список доменов для ИНН из разных источников."""

    candidates: list[object] = []
    email_candidates: list[object] = []

    log.info("parse-site: начинаем сбор доменов для ИНН %s", inn)

    try:
        domains_pg = await get_domains_by_inn_pg(inn)
        if domains_pg:
            log.info(
                "parse-site: найдены домены в POSTGRES.clients_requests по ИНН %s (%s шт.) → %s",
                inn,
                len(domains_pg),
                domains_pg,
            )
            candidates.extend(domains_pg)
        else:
            log.info("parse-site: в POSTGRES.clients_requests нет доменов для ИНН %s", inn)
    except Exception as e:  # noqa: BLE001
        log.warning("parse-site: ошибка чтения POSTGRES.clients_requests для ИНН %s: %s", inn, e)

    try:
        domains_pd = await get_domains_by_inn_pd(inn)
        if domains_pd:
            log.info(
                "parse-site: найдены домены в parsing_data.clients_requests по ИНН %s (%s шт.) → %s",
                inn,
                len(domains_pd),
                domains_pd,
            )
            candidates.extend(domains_pd)
        else:
            log.info("parse-site: в parsing_data.clients_requests нет доменов для ИНН %s", inn)
    except Exception as e:  # noqa: BLE001
        log.warning("parse-site: ошибка чтения parsing_data.clients_requests для ИНН %s: %s", inn, e)

    try:
        ib_domains = await get_ib_clients_domains_pg(inn)
        if ib_domains:
            log.info(
                "parse-site: найдены домены в POSTGRES.ib_clients по ИНН %s (%s шт.) → %s",
                inn,
                len(ib_domains),
                ib_domains,
            )
            candidates.extend(ib_domains)
        else:
            log.info("parse-site: в POSTGRES.ib_clients нет доменов для ИНН %s", inn)
    except Exception as e:  # noqa: BLE001
        log.warning("parse-site: ошибка чтения POSTGRES.ib_clients для ИНН %s: %s", inn, e)

    try:
        res = await session.execute(
            select(DaDataResult.web_sites, DaDataResult.emails).where(DaDataResult.inn == inn)
        )
        row = res.one_or_none()
        if row:
            web_sites = row.web_sites
            emails = row.emails
            if web_sites:
                log.info(
                    "parse-site: найдены сайты в bitrix_data.dadata_result по ИНН %s → %s",
                    inn,
                    web_sites,
                )
                candidates.append(web_sites)
            else:
                log.info("parse-site: в bitrix_data.dadata_result нет сайтов для ИНН %s", inn)

            if emails:
                log.info(
                    "parse-site: найдены email в bitrix_data.dadata_result по ИНН %s → %s",
                    inn,
                    emails,
                )
                email_candidates.append(emails)
            else:
                log.info("parse-site: в bitrix_data.dadata_result нет email для ИНН %s", inn)
        else:
            log.info("parse-site: запись bitrix_data.dadata_result не найдена для ИНН %s", inn)
    except Exception as e:  # noqa: BLE001
        log.warning(
            "parse-site: ошибка чтения bitrix_data.dadata_result (web_sites/emails) для ИНН %s: %s",
            inn,
            e,
        )

    normalized_domains = _normalize_domains(candidates)
    if normalized_domains:
        log.info(
            "parse-site: нормализованные домены из БД по ИНН %s → %s",
            inn,
            normalized_domains,
        )
    else:
        log.info("parse-site: нормализованные домены из БД по ИНН %s отсутствуют", inn)

    email_domains = _normalize_email_domains(email_candidates)
    if email_domains:
        log.info(
            "parse-site: домены, полученные из email bitrix_data.dadata_result по ИНН %s → %s",
            inn,
            email_domains,
        )

    combined: list[str] = []
    seen: set[str] = set()
    for dom in itertools.chain(normalized_domains, email_domains):
        if dom in seen:
            continue
        seen.add(dom)
        combined.append(dom)

    log.info(
        "parse-site: итоговый список доменов по ИНН %s (%s шт.) → %s",
        inn,
        len(combined),
        combined,
    )
    return combined


def _normalize_and_split_domain(domain_or_url: str) -> tuple[str, str]:
    """
    Возвращает (home_url, normalized_domain без www).
    """

    home_url = to_home_url(domain_or_url)
    normalized_domain = _urlparse(home_url).netloc.replace("www.", "")
    return home_url, normalized_domain


async def run_parse_site(payload: ParseSiteRequest, session: AsyncSession) -> ParseSiteResponse:
    """Основная реализация parse-site."""

    if not settings.SCRAPERAPI_KEY:
        raise HTTPException(status_code=400, detail="SCRAPERAPI_KEY is not configured on server")

    inn = (payload.inn or "").strip()
    if not inn.isdigit():
        raise HTTPException(status_code=400, detail="ИНН должен содержать только цифры")

    log.info(
        "parse-site: старт обработки (ИНН=%s, save_client_request=%s)",
        inn,
        payload.save_client_request,
    )

    manual_domains: list[object] = []
    manual_emails: list[object] = []
    dadata_main_okved: Optional[str] = None
    dadata_okveds: Any = None
    if payload.parse_domain:
        manual_domains.append(payload.parse_domain)
    if payload.parse_domains:
        manual_domains.extend(payload.parse_domains)
    if payload.parse_emails:
        manual_emails.extend(payload.parse_emails)

    if manual_domains:
        log.info("parse-site: переданные вручную домены → %s", manual_domains)
    else:
        log.info("parse-site: ручные домены не переданы")

    if manual_emails:
        log.info("parse-site: переданные вручную email → %s", manual_emails)
    else:
        log.info("parse-site: ручные email не переданы")

    normalized_manual_domains = _normalize_domains(manual_domains)
    manual_email_domains = _normalize_email_domains(manual_emails)
    if manual_email_domains:
        log.info(
            "parse-site: домены из ручных email (отфильтровано) → %s",
            manual_email_domains,
        )

    normalized_manual: list[str] = []
    normalized_manual_seen: set[str] = set()
    for dom in itertools.chain(normalized_manual_domains, manual_email_domains):
        if dom in normalized_manual_seen:
            continue
        normalized_manual_seen.add(dom)
        normalized_manual.append(dom)

    log.info("parse-site: нормализованные ручные домены → %s", normalized_manual)
    domains_to_process: list[str]
    if normalized_manual:
        domains_to_process = list(normalized_manual)
        seen = set(domains_to_process)
        extra = await _collect_domains_by_inn(inn, session)
        extra_unique = [dom for dom in extra if dom not in seen]
        if extra_unique:
            domains_to_process.extend(extra_unique)
            log.info(
                "parse-site: итоговые домены после объединения с БД (%s шт.) → %s",
                len(domains_to_process),
                domains_to_process,
            )
    else:
        domains_to_process = await _collect_domains_by_inn(inn, session)
        if domains_to_process:
            log.info(
                "parse-site: домены определены только из БД (%s шт.) → %s",
                len(domains_to_process),
                domains_to_process,
            )
        else:
            log.info("parse-site: домены не найдены для ИНН %s, возвращаем ошибку", inn)
            raise HTTPException(status_code=404, detail="Не удалось определить домены для парсинга")

    log.info("parse-site: итоговый набор доменов для обработки (до override) → %s", domains_to_process)

    company_name = (payload.company_name or "").strip() or None
    client_domain_override = (payload.client_domain_1 or "").strip() or None
    override_domain_for_pars = None
    override_url = (payload.url_override or "").strip() or None

    if company_name:
        log.info("parse-site: название компании передано в запросе → %s", company_name)

    try:
        res = await session.execute(
            select(
                DaDataResult.short_name,
                DaDataResult.main_okved,
                DaDataResult.okveds,
            ).where(DaDataResult.inn == inn)
        )
        row = res.one_or_none()
    except Exception as e:  # noqa: BLE001
        log.warning("parse-site: не удалось получить данные из DaDataResult: %s", e)
        row = None

    if row:
        if not company_name and row.short_name:
            company_name = row.short_name
            log.info(
                "parse-site: название компании найдено в bitrix_data.dadata_result → %s",
                company_name,
            )
        elif not company_name:
            log.info("parse-site: название компании в bitrix_data.dadata_result не найдено")

        if getattr(row, "main_okved", None):
            dadata_main_okved = str(row.main_okved)
        if getattr(row, "okveds", None) is not None:
            dadata_okveds = row.okveds
    else:
        if not company_name:
            log.info("parse-site: запись в bitrix_data.dadata_result не найдена")

    if client_domain_override:
        log.info("parse-site: override client_domain_1=%s", client_domain_override)
        normalized_client = _normalize_domain_candidate(client_domain_override)
        if not normalized_client:
            raise HTTPException(status_code=400, detail="client_domain_1 содержит некорректный домен")
        if normalized_client in domains_to_process:
            domains_to_process.remove(normalized_client)
        domains_to_process.insert(0, normalized_client)
        log.info("parse-site: домен override установлен первым → %s", normalized_client)

    log.info("parse-site: домены к обработке (после override) → %s", domains_to_process)

    client_domain_override_for_pars = (payload.pars_site_domain_1 or "").strip() or None
    if client_domain_override_for_pars:
        if len(domains_to_process) > 1:
            log.warning(
                "parse-site: pars_site_domain_1 override игнорируется для части доменов (ИНН %s)",
                inn,
            )
        _, override_domain_for_pars = _normalize_and_split_domain(client_domain_override_for_pars)
        log.info("parse-site: override pars_site_domain_1=%s", override_domain_for_pars)

    if override_url:
        if len(domains_to_process) > 1:
            log.warning(
                "parse-site: url_override игнорируется для части доменов (ИНН %s)",
                inn,
            )
        log.info("parse-site: override url=%s", override_url)

    log.info("parse-site: исходные кандидаты для clients_requests → %s", [f"www.{dom}" for dom in domains_to_process])

    company_id_pg = await get_clients_request_id_pg(inn)
    log.info("parse-site: текущий company_id в POSTGRES.clients_requests → %s", company_id_pg)
    if not payload.save_client_request and company_id_pg is None:
        raise HTTPException(
            status_code=404,
            detail="Запись в clients_requests не найдена; разрешите save_client_request, чтобы создать её",
        )

    summary_from_dadata: dict[str, Any] | None = None
    if payload.save_client_request and company_id_pg is None:
        log.info("parse-site: записей в clients_requests нет, запрашиваем DaData по ИНН %s", inn)
        try:
            suggestion = await find_party_by_inn(inn)
        except httpx.HTTPError as e:
            log.warning("DaData недоступна при parse-site для ИНН %s: %s", inn, e)
        else:
            if suggestion:
                summary_from_dadata = map_summary_from_dadata(suggestion)
                summary_from_dadata.setdefault("inn", inn)
                log.info("parse-site: DaData вернула данные для ИНН %s", inn)
                try:
                    await replace_dadata_raw(session, inn=inn, payload=suggestion)
                    await upsert_company_summary(session, data=summary_from_dadata)
                    await session.commit()
                    log.info("parse-site: данные DaData сохранены в bitrix_data")
                except Exception as e:  # noqa: BLE001
                    await session.rollback()
                    log.warning("Не удалось сохранить данные DaData для ИНН %s: %s", inn, e)
            else:
                log.info("DaData не вернула данных по ИНН %s", inn)

    minimal_summary: dict[str, Any]
    if summary_from_dadata is not None:
        minimal_summary = dict(summary_from_dadata)
    else:
        minimal_summary = {"short_name": company_name, "inn": inn}

    minimal_summary.setdefault("inn", inn)
    if company_name:
        minimal_summary.setdefault("short_name", company_name)
    else:
        short_name_from_summary = (minimal_summary.get("short_name") or "").strip()
        company_name = short_name_from_summary or company_name

    if dadata_main_okved and not minimal_summary.get("main_okved"):
        minimal_summary["main_okved"] = dadata_main_okved
    if dadata_okveds is not None and minimal_summary.get("okveds") is None:
        minimal_summary["okveds"] = dadata_okveds

    log.info("parse-site: итоговый summary для clients_requests → %s", minimal_summary)

    summary_for_client_base = dict(minimal_summary)

    okved_main_code = (
        summary_for_client_base.get("main_okved")
        or summary_for_client_base.get("okved_main")
    )
    okveds_source = summary_for_client_base.get("okveds") or dadata_okveds
    if not okved_main_code:
        okved_main_code = await get_okved_main_pg(inn)

    okved_text = _resolve_okved_text(okved_main_code, okveds_source)
    if okved_text:
        log.info("parse-site: основной ОКВЭД для сравнения → %s", okved_text)
    else:
        log.info("parse-site: основной ОКВЭД не определён")

    okved_vector: Optional[list[float]] = None
    if okved_text:
        okved_vector = await _embed_text(okved_text, label=f"okved:{inn}")


    company_id_pd: Optional[int] = None
    mirror_failed_logged = False
    try:
        company_id_pd = await get_clients_request_id_pd(inn)
    except Exception as e:  # noqa: BLE001
        log.warning("mirror parsing_data failed for INN=%s: %s", inn, e)
        mirror_failed_logged = True
        company_id_pd = None
    else:
        log.info(
            "parse-site: текущий company_id в parsing_data.clients_requests → %s",
            company_id_pd,
        )

    clients_request_synced = False
    synced_client_domain_2: Optional[str] = None
    client_domain_lookup: Optional[str] = None
    client_domains_success: list[str] = []

    planned_domains = list(domains_to_process)

    results: list[ParsedSiteResult] = []
    successes: list[ParsedSiteResult] = []
    total_inserted = 0
    successful_used_domains: list[str] = []
    okved_alerts: list[OkvedAlert] = []

    for idx, domain_candidate in enumerate(domains_to_process, start=1):
        log.info(
            "parse-site: начинаем обработку домена %s (%s/%s)",
            domain_candidate,
            idx,
            len(domains_to_process),
        )
        try:
            home_url, chunks, normalized_domain = await fetch_and_chunk(domain_candidate)
            log.info(
                "parse-site: получен контент для %s → %s чанков (url=%s, нормализованный домен=%s)",
                domain_candidate,
                len(chunks),
                home_url,
                normalized_domain,
            )
        except FetchError as e:
            log.warning(
                "parse-site: ошибка парсинга домена %s → %s",
                domain_candidate,
                e,
            )
            results.append(
                ParsedSiteResult(
                    requested_domain=domain_candidate,
                    used_domain=None,
                    url=None,
                    chunks_inserted=0,
                    success=False,
                    error=str(e),
                )
            )
            continue

        domain_for_pars = (
            override_domain_for_pars if override_domain_for_pars and len(domains_to_process) == 1 else normalized_domain
        )
        url_for_pars = override_url if override_url and len(domains_to_process) == 1 else home_url

        if domain_candidate not in client_domains_success:
            client_domains_success.append(domain_candidate)
        current_client_domain_1 = client_domains_success[0]
        current_client_domain_2 = client_domains_success[1] if len(client_domains_success) > 1 else None

        full_text = "\n\n".join(chunks or [])
        description: Optional[str] = None
        description_vector: Optional[list[float]] = None
        if full_text:
            description, description_vector = await _generate_site_description(
                full_text,
                domain=domain_for_pars or normalized_domain,
                inn=inn,
            )

        vector_literal = _vector_to_literal(description_vector)

        okved_score: Optional[float] = None
        if description_vector and okved_vector:
            raw_score = _cosine_similarity(description_vector, okved_vector)
            if raw_score is not None:
                okved_score = _format_score(raw_score)
                log.info(
                    "parse-site: okved score для %s → %s",
                    domain_for_pars,
                    okved_score,
                )
                if okved_score < _OKVED_ALERT_THRESHOLD:
                    alert_domain = domain_for_pars or normalized_domain
                    okved_alerts.append(
                        OkvedAlert(domain=alert_domain, score=okved_score)
                    )

        chunks_payload = [
            {"start": i, "end": i, "text": text}
            for i, text in enumerate(chunks or [])
            if text
        ]
        log.info(
            "parse-site: подготовлено чанков к вставке для %s → %s шт.",
            domain_candidate,
            len(chunks_payload),
        )

        written_chunks = len(chunks_payload)
        if chunks_payload:
            if payload.save_client_request:
                summary_for_client = dict(summary_for_client_base)
                if not clients_request_synced:
                    log.info(
                        "parse-site: выполняем upsert clients_requests (POSTGRES/parsing_data) для ИНН %s",
                        inn,
                    )
                    try:
                        await push_clients_request_pg(
                            summary_for_client,
                            domain=current_client_domain_1,
                            domain_secondary=current_client_domain_2,
                        )
                    except Exception as e:  # noqa: BLE001
                        raise HTTPException(
                            status_code=500,
                            detail=f"PG upsert clients_requests failed: {e}",
                        ) from e

                    client_domain_lookup = f"www.{current_client_domain_1}" if current_client_domain_1 else None
                    company_id_pg = await get_clients_request_id_pg(inn, client_domain_lookup)
                    if company_id_pg is None:
                        company_id_pg = await get_clients_request_id_pg(inn)

                    try:
                        await push_clients_request_pd(
                            summary_for_client,
                            domain=current_client_domain_1,
                            domain_secondary=current_client_domain_2,
                        )
                    except Exception as e:  # noqa: BLE001
                        if not mirror_failed_logged:
                            log.warning("mirror parsing_data failed for INN=%s: %s", inn, e)
                            mirror_failed_logged = True
                        company_id_pd = await get_clients_request_id_pd(inn, client_domain_lookup)
                    else:
                        company_id_pd = await get_clients_request_id_pd(inn, client_domain_lookup)
                        if company_id_pd is None:
                            company_id_pd = await get_clients_request_id_pd(inn)
                        log.info(
                            "parse-site: upsert clients_requests в parsing_data завершен, company_id=%s",
                            company_id_pd,
                        )

                    clients_request_synced = True
                    synced_client_domain_2 = current_client_domain_2
                    log.info(
                        "parse-site: синхронизация clients_requests выполнена (domain_1=%s, domain_2=%s)",
                        f"www.{current_client_domain_1}" if current_client_domain_1 else None,
                        f"www.{current_client_domain_2}" if current_client_domain_2 else None,
                    )
                elif current_client_domain_2 and current_client_domain_2 != synced_client_domain_2:
                    log.info(
                        "parse-site: обновляем domain_2 для clients_requests → %s",
                        f"www.{current_client_domain_2}",
                    )
                    try:
                        await push_clients_request_pg(
                            summary_for_client,
                            domain=None,
                            domain_secondary=current_client_domain_2,
                        )
                    except Exception as e:  # noqa: BLE001
                        log.warning(
                            "parse-site: обновление POSTGRES.clients_requests domain_2 не удалось для ИНН %s: %s",
                            inn,
                            e,
                        )
                    else:
                        synced_client_domain_2 = current_client_domain_2
                        log.info(
                            "parse-site: domain_2 обновлён в POSTGRES.clients_requests → %s",
                            f"www.{current_client_domain_2}",
                        )

                    if company_id_pd:
                        try:
                            await push_clients_request_pd(
                                summary_for_client,
                                domain=None,
                                domain_secondary=current_client_domain_2,
                            )
                        except Exception as e:  # noqa: BLE001
                            if not mirror_failed_logged:
                                log.warning(
                                    "mirror parsing_data failed для обновления domain_2 (ИНН %s): %s",
                                    inn,
                                    e,
                                )
                                mirror_failed_logged = True
                        else:
                            log.info(
                                "parse-site: domain_2 обновлён в parsing_data.clients_requests → %s",
                                f"www.{current_client_domain_2}",
                            )

            if company_id_pg is None:
                raise HTTPException(
                    status_code=500,
                    detail="PG: не удалось определить company_id для pars_site",
                )

            log.info(
                "parse-site: обновляем pars_site в POSTGRES (company_id=%s, домен=%s, url=%s)",
                company_id_pg,
                domain_for_pars,
                url_for_pars,
            )
            await pars_site_insert_chunks_pg(
                company_id=company_id_pg,
                domain_1=domain_for_pars,
                url=url_for_pars,
                chunks=chunks_payload,
            )
            log.info(
                "parse-site: сохранено чанков в POSTGRES.pars_site для %s → %s",
                domain_for_pars,
                written_chunks,
            )
            total_inserted += written_chunks

            if company_id_pd:
                try:
                    log.info(
                        "parse-site: обновляем parsing_data.pars_site (company_id=%s, домен=%s, url=%s)",
                        company_id_pd,
                        domain_for_pars,
                        url_for_pars,
                    )
                    await pars_site_insert_chunks_pd(
                        company_id=company_id_pd,
                        domain_1=domain_for_pars,
                        url=url_for_pars,
                        chunks=chunks_payload,
                    )
                except Exception as e:  # noqa: BLE001
                    if not mirror_failed_logged:
                        log.warning("mirror parsing_data failed for INN=%s: %s", inn, e)
                        mirror_failed_logged = True
                else:
                    log.info(
                        "parse-site: чанки записаны в parsing_data.pars_site для %s → %s",
                        domain_for_pars,
                        written_chunks,
                    )

        if (description or vector_literal) and company_id_pg:
            await pars_site_update_metadata_pg(
                company_id=company_id_pg,
                domain_1=domain_for_pars,
                description=description,
                vector_literal=vector_literal,
            )
        if vector_literal and company_id_pd:
            await pars_site_update_vector(
                company_id=company_id_pd,
                domain_1=domain_for_pars,
                vector_literal=vector_literal,
            )

        result = ParsedSiteResult(
            requested_domain=domain_candidate,
            used_domain=domain_for_pars,
            url=url_for_pars,
            chunks_inserted=written_chunks,
            success=True,
            error=None,
            description=description,
            has_description_vector=bool(description_vector),
            okved_score=okved_score,
        )
        results.append(result)
        successes.append(result)
        if domain_for_pars and domain_for_pars not in successful_used_domains:
            successful_used_domains.append(domain_for_pars)
        log.info(
            "parse-site: домен %s обработан успешно (чанков=%s)",
            domain_candidate,
            written_chunks,
        )

    if not successes:
        errors = "; ".join(filter(None, (r.error for r in results))) or "Парсинг не удался"
        log.warning("parse-site: все домены завершились ошибкой для ИНН %s → %s", inn, errors)
        raise HTTPException(status_code=502, detail=errors)

    final_domain_1 = client_domains_success[0] if client_domains_success else None
    final_domain_2 = client_domains_success[1] if len(client_domains_success) > 1 else None

    log.info(
        "parse-site: финальные домены для clients_requests → domain_1=%s, domain_2=%s",
        f"www.{final_domain_1}" if final_domain_1 else None,
        f"www.{final_domain_2}" if final_domain_2 else None,
    )

    log.info("parse-site: успешные домены для парсинга → %s", successful_used_domains)
    log.info(
        "parse-site: успешно обработано доменов %s/%s, всего вставлено чанков %s",
        len(successes),
        len(results),
        total_inserted,
    )

    if company_id_pg is None:
        raise HTTPException(status_code=500, detail="PG company_id is undefined after parsing")

    primary = successes[0]
    return ParseSiteResponse(
        company_id=company_id_pg,
        domain_1=f"www.{final_domain_1}" if final_domain_1 else None,
        domain_2=f"www.{final_domain_2}" if final_domain_2 else None,
        url=primary.url,
        planned_domains=planned_domains,
        successful_domains=successful_used_domains,
        chunks_inserted=total_inserted,
        results=results,
        okved_text=okved_text,
        okved_alerts=okved_alerts,
    )


async def _run_parse_site_background(
    *,
    inn: str,
    parse_domain: str | None,
    company_name: str | None,
    save_client_request: bool,
    reason: str,
) -> None:
    """Выполнить parse-site в отдельной задаче."""

    payload_data: dict[str, object] = {
        "inn": inn,
        "save_client_request": save_client_request,
    }
    if parse_domain:
        payload_data["parse_domain"] = parse_domain
    if company_name:
        payload_data["company_name"] = company_name

    try:
        async with bitrix_session() as session:
            payload = ParseSiteRequest(**payload_data)
            await run_parse_site(payload, session)
    except HTTPException as e:
        log.info(
            "parse-site background skipped (%s): %s (inn=%s, domain=%s)",
            reason,
            e.detail,
            inn,
            parse_domain,
        )
    except Exception:  # noqa: BLE001
        log.exception(
            "parse-site background failed (%s) for inn=%s, domain=%s",
            reason,
            inn,
            parse_domain,
        )


def schedule_parse_site_background(
    *,
    inn: str,
    parse_domain: str | None,
    company_name: str | None = None,
    save_client_request: bool = False,
    reason: str,
) -> None:
    """Запускает parse-site в фоне, не блокируя ответ API."""

    inn_clean = (inn or "").strip()
    if not inn_clean:
        log.debug("parse-site background not scheduled: empty inn (%s).", reason)
        return

    domain_clean = (parse_domain or "").strip() or None
    name_clean = (company_name or "").strip() or None

    async def runner() -> None:
        await _run_parse_site_background(
            inn=inn_clean,
            parse_domain=domain_clean,
            company_name=name_clean,
            save_client_request=save_client_request,
            reason=reason,
        )

    try:
        asyncio.create_task(runner(), name=f"parse-site:{inn_clean}")
    except Exception:  # noqa: BLE001
        log.exception(
            "parse-site background scheduling failed (%s) for inn=%s, domain=%s",
            reason,
            inn_clean,
            domain_clean,
        )


__all__ = [
    "ParseSiteRequest",
    "ParseSiteResponse",
    "ParsedSiteResult",
    "run_parse_site",
    "schedule_parse_site_background",
]
