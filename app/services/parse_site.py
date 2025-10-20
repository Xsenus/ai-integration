from __future__ import annotations

import asyncio
import itertools
import json
import logging
import re
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Iterable, Mapping, Optional, Sequence, Literal

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
from app.services.analyze_client import (
    AnalyzeServiceUnavailable,
    fetch_embedding,
    fetch_site_description,
)
from app.services.scrape import FetchError, fetch_and_chunk, to_home_url
from app.services.vector_similarity import cosine_similarity

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


class OkvedScoreItem(BaseModel):
    code: str
    score: float


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
    okved_score_avg: float | None = None
    okved_scores: list[OkvedScoreItem] = Field(default_factory=list)
    description_status: str | None = None
    notes: list[str] = Field(default_factory=list)
    processing_ms: int | None = None


class OkvedAlert(BaseModel):
    domain: str
    score: float


class ParseSiteResponse(BaseModel):
    status: Literal["success", "partial_success"]
    message: str
    started_at: datetime
    finished_at: datetime
    duration_seconds: float
    duration_ms: int
    company_id: int
    domain_1: str | None
    domain_2: str | None
    url: str | None
    planned_domains: list[str]
    successful_domains: list[str]
    chunks_inserted: int
    domains_attempted: int
    domains_succeeded: int
    failed_domains: list[str] = Field(default_factory=list)
    results: list[ParsedSiteResult]
    okved_text: str | None = None
    okved_alerts: list[OkvedAlert] = Field(default_factory=list)


@dataclass(slots=True)
class _DomainFetchResult:
    requested_domain: str
    home_url: str | None = None
    chunks: list[str] | None = None
    normalized_domain: str | None = None
    error: str | None = None
    elapsed_ms: int | None = None


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


def _okved_item_code(obj: Any) -> str:
    if isinstance(obj, dict):
        return str(
            obj.get("code")
            or obj.get("value")
            or obj.get("okved")
            or obj.get("main")
            or ""
        ).strip()
    if isinstance(obj, str):
        return obj.strip().split(" ", 1)[0]
    return ""


def _okved_item_name(obj: Any) -> Optional[str]:
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


def _format_okved_text(code: str, name: Optional[str]) -> str:
    code = code.strip()
    clean_name = (name or "").strip()
    if not code:
        return clean_name
    if clean_name and code not in clean_name:
        return f"{code} — {clean_name}"
    return clean_name or code


def _collect_okved_entries(
    main_okved: Optional[str], okveds: Any
) -> list[tuple[str, str]]:
    entries: dict[str, str] = {}
    order: list[str] = []

    def _register(code: str, name: Optional[str]) -> None:
        norm_code = code.strip()
        if not norm_code:
            return
        formatted = _format_okved_text(norm_code, name)
        if norm_code not in entries:
            order.append(norm_code)
            entries[norm_code] = formatted
        else:
            if entries[norm_code] == norm_code and formatted != norm_code:
                entries[norm_code] = formatted

    def _walk(obj: Any) -> None:
        if isinstance(obj, dict):
            if isinstance(obj.get("items"), list):
                code = _okved_item_code(obj)
                name = _okved_item_name(obj)
                if code:
                    _register(code, name)
                for nested in obj.get("items") or []:
                    _walk(nested)
                return
            code = _okved_item_code(obj)
            name = _okved_item_name(obj)
            if code:
                _register(code, name)
            return
        if isinstance(obj, (list, tuple, set)):
            for item in obj:
                _walk(item)
            return
        if isinstance(obj, str):
            code = _okved_item_code(obj)
            name = _okved_item_name(obj)
            if code:
                _register(code, name)

    if isinstance(okveds, dict):
        if isinstance(okveds.get("items"), list):
            _walk(okveds.get("items"))
        else:
            for key, value in okveds.items():
                if key == "main":
                    continue
                _walk({"code": key, "name": value})
    elif okveds is not None:
        _walk(okveds)

    code = (main_okved or "").strip()
    if code:
        formatted = entries.get(code) or _format_okved_text(code, None)
        entries[code] = formatted
        if code in order:
            order.remove(code)
        order.insert(0, code)

    return [(c, entries[c]) for c in order]
def _sort_domains_by_score(
    scores: Mapping[str, Optional[float]],
    order_index: Mapping[str, int],
) -> list[str]:
    def _key(item: tuple[str, Optional[float]]) -> tuple[int, float, int]:
        domain, score = item
        if score is None:
            return (1, 0.0, order_index.get(domain, 0))
        return (0, -float(score), order_index.get(domain, 0))

    return [domain for domain, _ in sorted(scores.items(), key=_key)]


def _format_score(value: float) -> float:
    try:
        return float(f"{value:.4f}")
    except (TypeError, ValueError):
        return value


def _summarize_okved_scores(
    scores_by_code: Mapping[str, float],
    okved_entries: Sequence[tuple[str, str]],
) -> tuple[Optional[float], Optional[float], list[OkvedScoreItem], list[str]]:
    """Возвращает скор по основному ОКВЭДу, средний скор и расшифровку."""

    notes: list[str] = []
    if not scores_by_code:
        return None, None, [], notes

    main_code: Optional[str] = okved_entries[0][0] if okved_entries else None
    main_score: Optional[float] = None

    if main_code and main_code in scores_by_code:
        main_score = _format_score(scores_by_code[main_code])
    elif main_code:
        notes.append("Не удалось вычислить скор по основному ОКВЭД")

    avg_score = _format_score(
        sum(scores_by_code.values()) / len(scores_by_code)
    ) if scores_by_code else None

    details: list[OkvedScoreItem] = []
    used_codes: set[str] = set()
    for code, _ in okved_entries:
        if code in scores_by_code:
            details.append(OkvedScoreItem(code=code, score=_format_score(scores_by_code[code])))
            used_codes.add(code)

    for code, value in scores_by_code.items():
        if code in used_codes:
            continue
        details.append(OkvedScoreItem(code=code, score=_format_score(value)))

    return main_score, avg_score, details, notes


async def _generate_site_description(
    text: str,
    *,
    domain: str,
    inn: str,
) -> tuple[Optional[str], Optional[list[float]]]:
    if not text.strip():
        return None, None

    label = f"site:{inn}:{domain}"
    try:
        description, vector = await fetch_site_description(
            text,
            embed_model=settings.embed_model,
            label=label,
        )
    except AnalyzeServiceUnavailable:
        raise
    except Exception as exc:  # noqa: BLE001
        log.warning("parse-site: не удалось получить описание (%s): %s", label, exc)
        return None, None
    if description and not vector:
        try:
            vector = await _embed_text(description, label=f"site-desc:{inn}:{domain}")
        except AnalyzeServiceUnavailable:
            raise
    return description, vector


async def _embed_text(text: str, *, label: str) -> Optional[list[float]]:
    if not text.strip():
        return None

    try:
        vector = await fetch_embedding(text, label=label)
    except AnalyzeServiceUnavailable:
        raise
    except Exception as exc:  # noqa: BLE001
        log.warning("parse-site: не удалось получить embedding (%s): %s", label, exc)
        return None
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
    parsed = _urlparse(home_url)
    host = parsed.netloc.lower()
    if host.startswith("www."):
        host = host[4:]
    normalized_domain = host
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

    started_at = datetime.now(timezone.utc)
    started_monotonic = time.perf_counter()

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

    okved_entries = _collect_okved_entries(okved_main_code, okveds_source)
    okved_text = okved_entries[0][1] if okved_entries else None
    if okved_text:
        log.info("parse-site: основной ОКВЭД для сравнения → %s", okved_text)
        if len(okved_entries) > 1:
            log.info(
                "parse-site: всего ОКВЭДов для сравнения → %s",
                len(okved_entries),
            )
    else:
        log.info("parse-site: основной ОКВЭД не определён")

    okved_vectors_cache: dict[str, list[float]] = {}


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
    synced_client_domain_1: Optional[str] = None
    synced_client_domain_2: Optional[str] = None
    client_domain_lookup: Optional[str] = None
    client_domain_scores: dict[str, Optional[float]] = {}
    domain_order_index: dict[str, int] = {}

    planned_domains = list(domains_to_process)

    total_domains_to_fetch = len(domains_to_process)

    async def _fetch_domain_parallel(
        domain_candidate: str,
        position: int,
    ) -> _DomainFetchResult:
        log.info(
            "parse-site: начинаем обработку домена %s (%s/%s)",
            domain_candidate,
            position,
            total_domains_to_fetch,
        )
        fetch_started = time.perf_counter()
        try:
            home_url, chunks, normalized_domain = await fetch_and_chunk(domain_candidate)
            log.info(
                "parse-site: получен контент для %s → %s чанков (url=%s, нормализованный домен=%s)",
                domain_candidate,
                len(chunks),
                home_url,
                normalized_domain,
            )
            elapsed_ms = int((time.perf_counter() - fetch_started) * 1000)
            return _DomainFetchResult(
                requested_domain=domain_candidate,
                home_url=home_url,
                chunks=chunks,
                normalized_domain=normalized_domain,
                elapsed_ms=elapsed_ms,
            )
        except FetchError as e:
            log.warning(
                "parse-site: ошибка парсинга домена %s → %s",
                domain_candidate,
                e,
            )
            elapsed_ms = int((time.perf_counter() - fetch_started) * 1000)
            return _DomainFetchResult(
                requested_domain=domain_candidate,
                error=str(e),
                elapsed_ms=elapsed_ms,
            )
        except Exception as e:  # noqa: BLE001
            log.exception(
                "parse-site: непредвиденная ошибка при обработке домена %s",
                domain_candidate,
            )
            message = str(e) or e.__class__.__name__
            elapsed_ms = int((time.perf_counter() - fetch_started) * 1000)
            return _DomainFetchResult(
                requested_domain=domain_candidate,
                error=message,
                elapsed_ms=elapsed_ms,
            )

    fetch_tasks = [
        asyncio.create_task(_fetch_domain_parallel(domain_candidate, idx))
        for idx, domain_candidate in enumerate(domains_to_process, start=1)
    ]
    fetch_results_pool: dict[str, list[_DomainFetchResult]] = {}
    if fetch_tasks:
        fetched_results = await asyncio.gather(*fetch_tasks)
        for fetch_result in fetched_results:
            fetch_results_pool.setdefault(fetch_result.requested_domain, []).append(fetch_result)

    results: list[ParsedSiteResult] = []
    successes: list[ParsedSiteResult] = []
    total_inserted = 0
    successful_used_domains: list[str] = []
    okved_alerts: list[OkvedAlert] = []

    for idx, domain_candidate in enumerate(domains_to_process, start=1):
        domain_results = fetch_results_pool.get(domain_candidate, [])
        fetch_result = domain_results.pop(0) if domain_results else None
        if domain_results:
            fetch_results_pool[domain_candidate] = domain_results
        elif domain_candidate in fetch_results_pool and not domain_results:
            fetch_results_pool.pop(domain_candidate, None)

        post_fetch_started = time.perf_counter()
        accumulated_ms = int(fetch_result.elapsed_ms or 0) if fetch_result else 0
        result_notes: list[str] = []
        description_status: Optional[str] = None
        okved_score_avg: Optional[float] = None
        okved_details: list[OkvedScoreItem] = []

        if fetch_result is None:
            log.warning(
                "parse-site: результат загрузки отсутствует для домена %s",
                domain_candidate,
            )
            elapsed_ms = accumulated_ms + int((time.perf_counter() - post_fetch_started) * 1000)
            result_notes.append("Контент не получен")
            results.append(
                ParsedSiteResult(
                    requested_domain=domain_candidate,
                    used_domain=None,
                    url=None,
                    chunks_inserted=0,
                    success=False,
                    error="Результат загрузки отсутствует",
                    description_status="Контент не получен",
                    notes=result_notes,
                    processing_ms=elapsed_ms,
                )
            )
            continue

        if fetch_result.error:
            elapsed_ms = accumulated_ms + int((time.perf_counter() - post_fetch_started) * 1000)
            result_notes.append("Ошибка загрузки сайта")
            results.append(
                ParsedSiteResult(
                    requested_domain=domain_candidate,
                    used_domain=None,
                    url=None,
                    chunks_inserted=0,
                    success=False,
                    error=fetch_result.error,
                    description_status="Контент не получен",
                    notes=result_notes,
                    processing_ms=elapsed_ms,
                )
            )
            continue

        home_url = fetch_result.home_url
        normalized_domain = fetch_result.normalized_domain
        chunks = list(fetch_result.chunks or [])

        if not home_url or not normalized_domain:
            elapsed_ms = accumulated_ms + int((time.perf_counter() - post_fetch_started) * 1000)
            result_notes.append("Не удалось получить главную страницу")
            results.append(
                ParsedSiteResult(
                    requested_domain=domain_candidate,
                    used_domain=None,
                    url=None,
                    chunks_inserted=0,
                    success=False,
                    error=fetch_result.error or "Не удалось получить главную страницу",
                    description_status="Главная страница недоступна",
                    notes=result_notes,
                    processing_ms=elapsed_ms,
                )
            )
            continue

        domain_for_pars = (
            override_domain_for_pars if override_domain_for_pars and len(domains_to_process) == 1 else normalized_domain
        )
        url_for_pars = override_url if override_url and len(domains_to_process) == 1 else home_url

        full_text = "\n\n".join(chunks or [])
        description: Optional[str] = None
        description_vector: Optional[list[float]] = None
        if not full_text:
            description_status = "Не удалось выделить текст сайта"
            result_notes.append("Не удалось выделить текст сайта")
        else:
            try:
                description, description_vector = await _generate_site_description(
                    full_text,
                    domain=domain_for_pars or normalized_domain,
                    inn=inn,
                )
            except AnalyzeServiceUnavailable as exc:
                log.warning(
                    "parse-site: analyze service unavailable for %s (%s): %s",
                    domain_for_pars or normalized_domain,
                    inn,
                    exc,
                )
                description = None
                description_vector = None
                description_status = "Нет доступа к сервису"
                result_notes.append("Нет доступа к сервису")
            else:
                if description and description_vector:
                    description_status = "Описание и вектор получены"
                elif description:
                    description_status = "Описание получено без вектора"
                    result_notes.append("Вектор описания не получен")
                elif description_vector:
                    description_status = "Вектор получен без описания"
                    result_notes.append("Описание не получено, но embedding сформирован")
                else:
                    description_status = "Описание не получено"
                    result_notes.append("Описание не получено")

        if description_status is None:
            description_status = "Описание не формировалось"

        vector_literal = _vector_to_literal(description_vector)
        if vector_literal:
            result_notes.append("Вектор подготовлен к сохранению")

        okved_score: Optional[float] = None
        okved_scores_map: dict[str, float] = {}
        if description_vector and okved_entries:
            if not okved_vectors_cache and okved_entries:
                for code, text_value in okved_entries:
                    vector = await _embed_text(
                        text_value,
                        label=f"okved:{inn}:{code}",
                    )
                    if vector:
                        okved_vectors_cache[code] = vector
            if okved_vectors_cache:
                for code, vector in okved_vectors_cache.items():
                    raw_score = cosine_similarity(description_vector, vector)
                    if raw_score is None:
                        continue
                    okved_scores_map[code] = raw_score
                    log.info(
                        "parse-site: okved score для %s (%s) → %s",
                        domain_for_pars,
                        code,
                        _format_score(raw_score),
                    )
            else:
                log.info(
                    "parse-site: okved embeddings не получены — сравнение пропущено (%s)",
                    domain_for_pars,
                )

        okved_score, okved_score_avg, okved_details, notes_from_scores = _summarize_okved_scores(
            okved_scores_map,
            okved_entries,
        )
        result_notes.extend(notes_from_scores)

        if okved_score is not None:
            log.info(
                "parse-site: основной okved score для %s → %s",
                domain_for_pars,
                okved_score,
            )
        elif okved_score_avg is not None:
            log.info(
                "parse-site: основной okved score отсутствует, средний → %s",
                okved_score_avg,
            )

        effective_score_for_alert = okved_score if okved_score is not None else okved_score_avg
        if (
            effective_score_for_alert is not None
            and effective_score_for_alert < _OKVED_ALERT_THRESHOLD
        ):
            alert_domain = domain_for_pars or normalized_domain
            okved_alerts.append(
                OkvedAlert(domain=alert_domain, score=effective_score_for_alert)
            )

        if domain_candidate not in domain_order_index:
            domain_order_index[domain_candidate] = len(domain_order_index)
        effective_score_for_sort = (
            okved_score if okved_score is not None else okved_score_avg
        )
        client_domain_scores[domain_candidate] = effective_score_for_sort
        ordered_domains = _sort_domains_by_score(
            client_domain_scores,
            domain_order_index,
        )
        current_client_domain_1 = ordered_domains[0] if ordered_domains else None
        current_client_domain_2 = ordered_domains[1] if len(ordered_domains) > 1 else None
        log.info(
            "parse-site: текущий порядок доменов по score → %s",
            ordered_domains,
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

        summary_for_client: Optional[dict[str, Any]] = None
        if payload.save_client_request:
            summary_for_client = dict(summary_for_client_base)

            if not clients_request_synced and chunks_payload:
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

                synced_client_domain_1 = current_client_domain_1
                synced_client_domain_2 = current_client_domain_2
                client_domain_lookup = (
                    f"www.{synced_client_domain_1}" if synced_client_domain_1 else None
                )
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
                log.info(
                    "parse-site: синхронизация clients_requests выполнена (domain_1=%s, domain_2=%s)",
                    f"www.{synced_client_domain_1}" if synced_client_domain_1 else None,
                    f"www.{synced_client_domain_2}" if synced_client_domain_2 else None,
                )
            elif clients_request_synced:
                domain_1_changed = current_client_domain_1 != synced_client_domain_1
                domain_2_changed = current_client_domain_2 != synced_client_domain_2

                if domain_1_changed or domain_2_changed:
                    new_domain_1_for_log = (
                        f"www.{current_client_domain_1}" if current_client_domain_1 else None
                    )
                    new_domain_2_for_log = (
                        f"www.{current_client_domain_2}" if current_client_domain_2 else None
                    )
                    log.info(
                        "parse-site: обновляем clients_requests домены → domain_1=%s, domain_2=%s",
                        new_domain_1_for_log,
                        new_domain_2_for_log,
                    )

                    pg_domain = current_client_domain_1 if domain_1_changed else None
                    pg_domain_secondary = (
                        current_client_domain_2 if domain_2_changed else synced_client_domain_2
                    )

                    try:
                        await push_clients_request_pg(
                            summary_for_client,
                            domain=pg_domain,
                            domain_secondary=pg_domain_secondary,
                        )
                    except Exception as e:  # noqa: BLE001
                        log.warning(
                            "parse-site: обновление POSTGRES.clients_requests доменов не удалось для ИНН %s: %s",
                            inn,
                            e,
                        )
                    else:
                        if domain_1_changed:
                            synced_client_domain_1 = current_client_domain_1
                            client_domain_lookup = (
                                f"www.{synced_client_domain_1}"
                                if synced_client_domain_1
                                else None
                            )
                        if domain_2_changed:
                            synced_client_domain_2 = current_client_domain_2
                        log.info(
                            "parse-site: clients_requests обновлены в POSTGRES → domain_1=%s, domain_2=%s",
                            f"www.{synced_client_domain_1}" if synced_client_domain_1 else None,
                            f"www.{synced_client_domain_2}" if synced_client_domain_2 else None,
                        )

                    if company_id_pd:
                        pd_domain = current_client_domain_1 if domain_1_changed else None
                        pd_domain_secondary = (
                            current_client_domain_2
                            if domain_2_changed
                            else synced_client_domain_2
                        )
                        try:
                            await push_clients_request_pd(
                                summary_for_client,
                                domain=pd_domain,
                                domain_secondary=pd_domain_secondary,
                            )
                        except Exception as e:  # noqa: BLE001
                            if not mirror_failed_logged:
                                log.warning(
                                    "mirror parsing_data failed для обновления доменов (ИНН %s): %s",
                                    inn,
                                    e,
                                )
                                mirror_failed_logged = True
                        else:
                            log.info(
                                "parse-site: clients_requests обновлены в parsing_data → domain_1=%s, domain_2=%s",
                                f"www.{synced_client_domain_1}" if synced_client_domain_1 else None,
                                f"www.{synced_client_domain_2}" if synced_client_domain_2 else None,
                            )

        if chunks_payload:
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

        total_elapsed_ms = accumulated_ms + int((time.perf_counter() - post_fetch_started) * 1000)
        result_notes.append("Парсинг завершён успешно")

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
            okved_score_avg=okved_score_avg,
            okved_scores=okved_details,
            description_status=description_status,
            notes=result_notes,
            processing_ms=total_elapsed_ms,
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

    final_ordered_domains = _sort_domains_by_score(
        client_domain_scores,
        domain_order_index,
    )
    final_domain_1 = final_ordered_domains[0] if final_ordered_domains else None
    final_domain_2 = final_ordered_domains[1] if len(final_ordered_domains) > 1 else None

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

    finished_at = datetime.now(timezone.utc)
    total_duration_seconds = time.perf_counter() - started_monotonic
    duration_seconds = round(total_duration_seconds, 3)
    duration_ms = max(0, int(total_duration_seconds * 1000))
    domains_attempted = len(results)
    domains_succeeded = len(successes)
    failed_domains = [r.requested_domain for r in results if not r.success]
    status: Literal["success", "partial_success"] = (
        "success" if domains_attempted == domains_succeeded else "partial_success"
    )
    if status == "success":
        message = f"Обработаны все домены ({domains_succeeded} из {domains_attempted})"
    else:
        message = (
            f"Обработаны не все домены: {domains_succeeded} из {domains_attempted} успешно"
        )

    if company_id_pg is None:
        raise HTTPException(status_code=500, detail="PG company_id is undefined after parsing")

    primary = successes[0]
    return ParseSiteResponse(
        status=status,
        message=message,
        started_at=started_at,
        finished_at=finished_at,
        duration_seconds=duration_seconds,
        duration_ms=duration_ms,
        company_id=company_id_pg,
        domain_1=f"www.{final_domain_1}" if final_domain_1 else None,
        domain_2=f"www.{final_domain_2}" if final_domain_2 else None,
        url=primary.url,
        planned_domains=planned_domains,
        successful_domains=successful_used_domains,
        chunks_inserted=total_inserted,
        domains_attempted=domains_attempted,
        domains_succeeded=domains_succeeded,
        failed_domains=failed_domains,
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
    "OkvedAlert",
    "OkvedScoreItem",
    "run_parse_site",
    "schedule_parse_site_background",
]
