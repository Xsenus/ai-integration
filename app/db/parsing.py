# app/db/parsing.py
from __future__ import annotations

import logging
from typing import Any, Optional, Iterable, Mapping
from urllib.parse import urlparse

from sqlalchemy import bindparam, text
from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.types import String

from app.config import settings

log = logging.getLogger("db.parsing")

_engine_parsing: Optional[AsyncEngine] = None


# ---------- Engine ----------

def get_parsing_engine() -> Optional[AsyncEngine]:
    """
    Ленивая инициализация движка для БД parsing_data.
    Если DSN не задан — возвращает None (мягкое отключение).
    """
    global _engine_parsing
    url = settings.parsing_url
    if not url:
        log.warning("PARSING_DATABASE_URL не задан — соединение с parsing_data отключено")
        return None
    if _engine_parsing is None:
        _engine_parsing = create_async_engine(
            url, pool_pre_ping=True, future=True, echo=settings.ECHO_SQL
        )
    return _engine_parsing


# ---------- Helpers ----------

async def table_exists(table_qualified: str) -> bool:
    """Проверяет наличие таблицы (например, 'public.clients_requests') в parsing_data."""
    eng = get_parsing_engine()
    if eng is None:
        return False
    try:
        async with eng.connect() as conn:
            res = await conn.execute(text("SELECT to_regclass(:t)"), {"t": table_qualified})
            return res.scalar_one_or_none() is not None
    except Exception as e:  # noqa: BLE001
        log.info("Ошибка соединения с parsing_data (%s) — пропускаю проверку %s.", e, table_qualified)
        return False


async def clients_requests_exists() -> bool:
    """Есть ли таблица public.clients_requests в parsing_data."""
    return await table_exists("public.clients_requests")


def _okved_text(item: Any) -> str | None:
    """Формат 'код — наименование' из dict/str."""
    if item is None:
        return None
    if isinstance(item, dict):
        code = item.get("code") or item.get("value") or item.get("okved")
        name = item.get("name") or item.get("short") or item.get("title")
        if code and name:
            return f"{code} — {name}"
        if code:
            return str(code)
        return name or None
    s = str(item).strip()
    return s or None


def _normalize_domain(domain: Optional[str]) -> Optional[str]:
    """
    Нормализуем домен к виду ДЛЯ pars_site (БЕЗ 'www.'):
      - обрезаем протокол/путь
      - host в lower
      - убираем ведущий 'www.'
    """
    if not domain:
        return None
    s = domain.strip()
    if not s:
        return None
    parsed = urlparse(s if "://" in s else "http://" + s)
    host = (parsed.netloc or parsed.path).strip().lower().rstrip("/")
    if host.startswith("www."):
        host = host[4:]
    return host or None


def _ensure_www(domain: Optional[str]) -> Optional[str]:
    """
    Возвращает домен в формате ДЛЯ clients_requests (С 'www.').
    Принимает сырой домен/URL, сам нормализует и добавит 'www.' при необходимости.
    """
    base = _normalize_domain(domain)  # уже без www
    if not base:
        return None
    return f"www.{base}"


def _prepare_row_from_summary(
    summary: dict,
    domain: Optional[str] = None,
    domain_secondary: Optional[str] = None,
) -> dict:
    """
    Готовит поля под INSERT/UPDATE в public.clients_requests:
      - company_name, inn
      - domain_1 (ВНИМАНИЕ: С 'www.'), domain_2 (e-mail'ы через запятую)
      - okved_main
      - okved_vtor_1..7 (до 7 вторичных ОКВЭД, без дубликата главного)
    """
    main_okved = summary.get("main_okved")
    okveds = summary.get("okveds") or []

    def _get_code(x):
        if isinstance(x, dict):
            return x.get("code") or x.get("value") or x.get("okved")
        return str(x) if x is not None else None

    # Собираем вторичные коды в list[str] без None внутри и без дубликата главного
    secondaries: list[str] = []
    for it in okveds:
        code = _get_code(it)
        if main_okved and code and str(code) == str(main_okved):
            continue
        txt = _okved_text(it)  # использует 'код — наименование', если dict
        if txt:
            secondaries.append(txt)
    secondaries = secondaries[:7]  # максимум 7

    def pick(i: int) -> Optional[str]:
        return secondaries[i] if i < len(secondaries) else None

    return {
        "company_name": summary.get("short_name"),
        "inn": summary.get("inn"),
        # ВАЖНО: для clients_requests.domain_1 храним С 'www.'
        "domain_1": _ensure_www(domain),
        "domain_2": _ensure_www(domain_secondary),
        "okved_main": main_okved,
        "okved_vtor_1": pick(0),
        "okved_vtor_2": pick(1),
        "okved_vtor_3": pick(2),
        "okved_vtor_4": pick(3),
        "okved_vtor_5": pick(4),
        "okved_vtor_6": pick(5),
        "okved_vtor_7": pick(6),
    }


# ---------- CRUD for clients_requests ----------

async def get_clients_request_id(inn: str, domain_1: Optional[str] = None) -> Optional[int]:
    """
    Возвращает id последней записи по ИНН (+ опц. domain_1) из public.clients_requests.
    ВНИМАНИЕ: поиск по domain_1 выполняем в том же формате, как мы храним — С 'www.'.
    """
    eng = get_parsing_engine()
    if eng is None:
        return None

    if domain_1:
        sql = text(
            "SELECT id FROM public.clients_requests "
            "WHERE inn = :inn AND domain_1 = :d ORDER BY id DESC LIMIT 1"
        )
        params = {"inn": inn, "d": _ensure_www(domain_1)}
    else:
        sql = text(
            "SELECT id FROM public.clients_requests "
            "WHERE inn = :inn ORDER BY id DESC LIMIT 1"
        )
        params = {"inn": inn}

    async with eng.begin() as conn:
        row = (await conn.execute(sql, params)).first()
        return int(row[0]) if row else None


async def get_last_domain_by_inn(inn: str) -> Optional[str]:
    """Возвращает последний domain_1 для ИНН из parsing_data.clients_requests."""

    eng = get_parsing_engine()
    if eng is None:
        return None

    sql = text(
        """
        SELECT domain_1
        FROM public.clients_requests
        WHERE inn = :inn
          AND domain_1 IS NOT NULL
          AND TRIM(domain_1) <> ''
        ORDER BY id DESC
        LIMIT 1
        """
    )

    async with eng.begin() as conn:
        row = (await conn.execute(sql, {"inn": inn})).first()
        if not row:
            log.info("parsing_data: domain не найден для inn=%s", inn)
            return None

        dom = row[0]
        try:
            ensured = _ensure_www(dom)
            return ensured
        except Exception:
            return str(dom) if dom is not None else None


async def get_domains_by_inn(inn: str) -> list[str]:
    """Возвращает все домены (domain_1/domain_2) для ИНН из parsing_data.clients_requests."""

    eng = get_parsing_engine()
    if eng is None:
        return []

    sql = text(
        """
        SELECT domain_1, domain_2
        FROM public.clients_requests
        WHERE inn = :inn
        ORDER BY id DESC
        """
    )

    async with eng.begin() as conn:
        rows = await conn.execute(sql, {"inn": inn})
        values: list[str] = []
        for row in rows:
            if not row:
                continue
            for value in row:
                if value is None:
                    continue
                value_text = str(value).strip()
                if value_text:
                    values.append(value_text)
        return values


async def push_clients_request(
    summary: dict,
    domain: Optional[str] = None,
    domain_secondary: Optional[str] = None,
) -> bool:
    """
    Upsert по inn: UPDATE … WHERE inn; если 0 строк — INSERT.
    Если нет соединения/таблицы — тихо выходим (best-effort).

    Соглашение подтверждено:
      - clients_requests.domain_1 храним С 'www.'
    """
    if not await clients_requests_exists():
        return False

    row = _prepare_row_from_summary(summary, domain=domain, domain_secondary=domain_secondary)
    if not row.get("inn"):
        log.info("Не указан ИНН — запись в clients_requests пропущена.")
        return False

    sql_update = text("""
        UPDATE public.clients_requests
        SET company_name=:company_name,
            domain_1=COALESCE(:domain_1, domain_1),
            domain_2=:domain_2,
            okved_main=:okved_main,
            okved_vtor_1=:okved_vtor_1,
            okved_vtor_2=:okved_vtor_2,
            okved_vtor_3=:okved_vtor_3,
            okved_vtor_4=:okved_vtor_4,
            okved_vtor_5=:okved_vtor_5,
            okved_vtor_6=:okved_vtor_6,
            okved_vtor_7=:okved_vtor_7
        WHERE inn=:inn
    """)

    sql_insert = text("""
        INSERT INTO public.clients_requests
        (company_name, inn, domain_1, domain_2, okved_main,
         okved_vtor_1, okved_vtor_2, okved_vtor_3, okved_vtor_4, okved_vtor_5, okved_vtor_6, okved_vtor_7)
        VALUES
        (:company_name, :inn, :domain_1, :domain_2, :okved_main,
         :okved_vtor_1, :okved_vtor_2, :okved_vtor_3, :okved_vtor_4, :okved_vtor_5, :okved_vtor_6, :okved_vtor_7)
    """)

    eng = get_parsing_engine()
    if eng is None:
        return False

    try:
        async with eng.begin() as conn:
            res = await conn.execute(sql_update, row)
            if getattr(res, "rowcount", 0) == 0:
                await conn.execute(sql_insert, row)
        log.info("clients_requests upsert: inn=%s, domain_1=%s", row["inn"], row["domain_1"])
        return True
    except Exception as e:  # noqa: BLE001
        log.warning("clients_requests upsert не удался (inn=%s): %s", row.get("inn"), e)
        return False


# ---------- INSERT into pars_site ----------

async def pars_site_insert_chunks(
    *,
    company_id: int,
    domain_1: str,
    url: str,
    chunks: Iterable[Mapping[str, Any] | tuple[int, int, str]],
    batch_size: int = 500,
) -> int:
    """Обновляет или добавляет чанки в parsing_data.public.pars_site."""

    eng = get_parsing_engine()
    if eng is None:
        return 0

    dom = _normalize_domain(domain_1) or domain_1

    def _coerce_chunk(
        ch: Mapping[str, Any] | tuple[int, int, str]
    ) -> Optional[dict[str, Any]]:
        if isinstance(ch, tuple) and len(ch) == 3:
            start, end, txt = ch
            txt_par = None
        elif isinstance(ch, Mapping):
            start = int(ch.get("start", 0) or 0)
            end = int(ch.get("end", 0) or 0)
            txt = ch.get("text")
            txt_par = ch.get("text_par")
        else:
            return None

        base_text = txt if txt is not None else txt_par
        if base_text is None:
            return None

        s = int(start)
        e = int(end)
        text_value = str(txt) if txt is not None else None
        text_par_value = str(txt_par) if txt_par is not None else str(base_text)

        return {
            "start": s,
            "end": e,
            "text": text_value,
            "text_par": text_par_value,
        }

    rows: list[dict[str, Any]] = []
    for ch in chunks:
        row = _coerce_chunk(ch)
        if not row:
            continue
        rows.append(row)

    if not rows:
        return 0

    params_base = {"company_id": company_id, "domain": dom}

    sql_existing = text(
        """
        SELECT id
        FROM public.pars_site
        WHERE company_id = :company_id
          AND LOWER(domain_1) = LOWER(:domain)
        ORDER BY start NULLS FIRST, id
        """
    )

    sql_update = text(
        """
        UPDATE public.pars_site
        SET url = :url,
            start = :start,
            "end" = :end,
            text = :text,
            text_par = :text_par,
            created_at = now()
        WHERE id = :id
        """
    )

    sql_insert = text(
        """
        INSERT INTO public.pars_site (company_id, domain_1, url, start, "end", text, text_par)
        VALUES (:company_id, :domain, :url, :start, :end, :text, :text_par)
        """
    )

    sql_clear_extra = text(
        """
        UPDATE public.pars_site
        SET url = :url,
            start = NULL,
            "end" = NULL,
            text = NULL,
            text_par = NULL,
            created_at = now()
        WHERE id = :id
        """
    )

    async with eng.begin() as conn:
        existing_ids = [row[0] for row in (await conn.execute(sql_existing, params_base)).all()]

        for idx, row in enumerate(rows):
            payload = {
                "company_id": company_id,
                "domain": dom,
                "url": url,
                "start": row["start"],
                "end": row["end"],
                "text": row["text"],
                "text_par": row["text_par"],
            }

            if idx < len(existing_ids):
                payload["id"] = existing_ids[idx]
                await conn.execute(sql_update, payload)
            else:
                await conn.execute(sql_insert, payload)

        for leftover_id in existing_ids[len(rows):]:
            await conn.execute(sql_clear_extra, {"id": leftover_id, "url": url})

    return len(rows)



async def pars_site_clear_domain(*, company_id: int, domain_1: str) -> None:
    """Удаляет существующие записи pars_site для указанного домена компании."""

    eng = get_parsing_engine()
    if eng is None:
        return

    dom = _normalize_domain(domain_1) or domain_1
    sql = text(
        """
        DELETE FROM public.pars_site
        WHERE company_id = :company_id
          AND LOWER(domain_1) = LOWER(:domain)
        """
    )

    try:
        async with eng.begin() as conn:
            await conn.execute(sql, {"company_id": company_id, "domain": dom})
    except SQLAlchemyError as exc:  # noqa: BLE001
        log.warning(
            "parsing_data: не удалось очистить pars_site (company_id=%s, domain=%s): %s",
            company_id,
            dom,
            exc,
        )


async def _flush_pars_site_batch(conn, rows: list[dict]) -> int:
    """
    Вставка батча через INSERT ... SELECT WHERE NOT EXISTS,
    чтобы не зависеть от наличия UNIQUE индекса.
    """
    if not rows:
        return 0

    sql = text(
        """
        INSERT INTO public.pars_site (company_id, domain_1, url, start, "end", text, text_par)
        SELECT :company_id, :domain_1, :url, :start, :end, :text, :text_par
        WHERE NOT EXISTS (
            SELECT 1 FROM public.pars_site ps
            WHERE ps.company_id = :company_id
              AND ps.domain_1 = :domain_1
              AND ps.url = :url
              AND ps.start = :start
              AND ps."end" = :end
        )
        """
    )

    total = 0
    for row in rows:
        res = await conn.execute(sql, row)
        try:
            total += int(getattr(res, "rowcount", 0) or 0)
        except Exception:
            pass
    return total


async def pars_site_update_vector(
    *,
    company_id: int,
    domain_1: str,
    vector_literal: Optional[str],
) -> None:
    """Обновляет text_vector в последнем наборе pars_site в зеркальной базе."""

    if vector_literal is None:
        return

    eng = get_parsing_engine()
    if eng is None:
        return

    sql = text(
        """
        WITH latest AS (
            SELECT MAX(created_at) AS created_at
            FROM public.pars_site
            WHERE company_id = :company_id
              AND domain_1 = :domain
        )
        UPDATE public.pars_site AS ps
        SET text_vector = CASE WHEN :vec IS NULL THEN NULL ELSE CAST(:vec AS vector) END
        FROM latest
        WHERE ps.company_id = :company_id
          AND ps.domain_1 = :domain
          AND (latest.created_at IS NULL OR ps.created_at = latest.created_at)
        """
    )

    sql = sql.bindparams(bindparam("vec", type_=String))

    params = {
        "company_id": company_id,
        "domain": _normalize_domain(domain_1) or domain_1,
        "vec": vector_literal,
    }

    try:
        async with eng.begin() as conn:
            await conn.execute(sql, params)
    except Exception as exc:  # noqa: BLE001
        log.warning(
            "parsing_data: не удалось обновить text_vector (company_id=%s, domain=%s): %s",
            company_id,
            domain_1,
            exc,
        )


# ---------- Schema init (CREATE IF NOT EXISTS) ----------

async def _has_extension(conn, name: str) -> bool:
    q = text("SELECT EXISTS (SELECT 1 FROM pg_extension WHERE extname = :n)")
    return bool((await conn.execute(q, {"n": name})).scalar())

async def ensure_parsing_schema() -> None:
    """
    Идемпотентная инициализация parsing_data:
    - при наличии прав включает pgvector (иначе логируем и используем BYTEA)
    - создаёт таблицы/индексы
    - не тянет внешние ключи на внешние БД (оставляет INT-колонки)
    """
    eng = get_parsing_engine()
    if eng is None:
        log.warning("init: parsing_data отключена (нет DSN) — пропускаю создание схемы")
        return

    try:
        async with eng.begin() as conn:
            await conn.execute(text("SET search_path = public"))

            # vector: пытаемся включить, но не падаем, если нельзя
            has_vector = await _has_extension(conn, "vector")
            if not has_vector:
                try:
                    await conn.execute(text("CREATE EXTENSION IF NOT EXISTS vector"))
                    has_vector = True
                except Exception as e:  # noqa: BLE001
                    log.warning("pgvector недоступен (%s) — text_vector будет BYTEA", e)

            VECTOR_TYPE = "VECTOR(3072)" if has_vector else "BYTEA"

            # 1) clients_requests — БЕЗ внешних FK на другие БД
            await conn.execute(text("""
            CREATE TABLE IF NOT EXISTS public.clients_requests (
                id BIGINT GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
                company_name TEXT,
                inn VARCHAR(20),
                domain_1 TEXT,
                domain_2 TEXT,
                okved_main TEXT,
                okved_vtor_1 TEXT,
                okved_vtor_2 TEXT,
                okved_vtor_3 TEXT,
                okved_vtor_4 TEXT,
                okved_vtor_5 TEXT,
                okved_vtor_6 TEXT,
                okved_vtor_7 TEXT,
                okved_top_1 TEXT,
                okved_top_2 TEXT,
                okved_top_3 TEXT,
                industry_top INT,
                prodclass_top INT,
                manager_id INT,
                manager_name VARCHAR(300),
                site_1_description TEXT,
                site_2_description TEXT,
                utp TEXT,
                pismo TEXT,
                started_at TIMESTAMPTZ,
                ended_at TIMESTAMPTZ,
                sec_duration INT,
                step_1 BOOLEAN DEFAULT FALSE,
                step_2 BOOLEAN DEFAULT FALSE,
                step_3 BOOLEAN DEFAULT FALSE,
                step_4 BOOLEAN DEFAULT FALSE,
                step_5 BOOLEAN DEFAULT FALSE,
                step_6 BOOLEAN DEFAULT FALSE,
                step_7 BOOLEAN DEFAULT FALSE,
                step_8 BOOLEAN DEFAULT FALSE,
                step_9 BOOLEAN DEFAULT FALSE,
                step_10 BOOLEAN DEFAULT FALSE,
                step_11 BOOLEAN DEFAULT FALSE,
                step_12 BOOLEAN DEFAULT FALSE,
                created_at TIMESTAMPTZ NOT NULL DEFAULT now()
            );
            """))

            # 2) pars_site — совместим со старым и новым потоком
            await conn.execute(text(f"""
            CREATE TABLE IF NOT EXISTS public.pars_site (
                id BIGINT GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
                company_id BIGINT NOT NULL REFERENCES public.clients_requests(id) ON UPDATE CASCADE ON DELETE CASCADE,
                domain_1 TEXT,
                -- "старый" поток
                start INT,
                "end" INT,
                text TEXT,
                -- "новый" поток
                text_par TEXT,
                url TEXT,
                created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                text_vector {VECTOR_TYPE}
            );
            """))

            # Индексы для pars_site
            await conn.execute(text('CREATE INDEX IF NOT EXISTS ix_pars_site_company_id ON public.pars_site(company_id);'))
            await conn.execute(text('CREATE INDEX IF NOT EXISTS ix_pars_site_domain_1   ON public.pars_site(domain_1);'))
            await conn.execute(text('CREATE INDEX IF NOT EXISTS ix_pars_site_url        ON public.pars_site(url);'))

            # 4) ai_site_prodclass — без внешних FK на ib_prodclass
            await conn.execute(text("""
            CREATE TABLE IF NOT EXISTS public.ai_site_prodclass (
                id BIGINT GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
                text_pars_id BIGINT NOT NULL REFERENCES public.pars_site(id) ON UPDATE CASCADE ON DELETE CASCADE,
                prodclass INT NOT NULL,
                prodclass_score NUMERIC(4,2),
                created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                CONSTRAINT chk_ai_site_prodclass_score CHECK (
                    prodclass_score IS NULL OR (prodclass_score >= 0.00 AND prodclass_score <= 1.00)
                )
            );
            """))

            # 6) ai_site_goods_types
            await conn.execute(text(f"""
            CREATE TABLE IF NOT EXISTS public.ai_site_goods_types (
                id BIGINT GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
                text_par_id BIGINT NOT NULL REFERENCES public.pars_site(id) ON UPDATE CASCADE ON DELETE CASCADE,
                goods_type TEXT,
                goods_type_ID INT,
                goods_types_score NUMERIC(4,2),
                text_vector {VECTOR_TYPE},
                created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                CONSTRAINT chk_ai_site_goods_score CHECK (
                    goods_types_score IS NULL OR (goods_types_score >= 0.00 AND goods_types_score <= 1.00)
                )
            );
            """))

            # 8) ai_site_equipment
            await conn.execute(text(f"""
            CREATE TABLE IF NOT EXISTS public.ai_site_equipment (
                id BIGINT GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
                text_pars_id BIGINT NOT NULL REFERENCES public.pars_site(id) ON UPDATE CASCADE ON DELETE CASCADE,
                equipment TEXT,
                equipment_score NUMERIC(4,2),
                equipment_ID INT,
                text_vector {VECTOR_TYPE},
                created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                CONSTRAINT chk_ai_site_equipment_score CHECK (
                    equipment_score IS NULL OR (equipment_score >= 0.00 AND equipment_score <= 1.00)
                )
            );
            """))

        log.info("init: схема parsing_data проверена/создана (pgvector=%s)", has_vector)
    except SQLAlchemyError as e:
        log.error("init: ошибка инициализации схемы parsing_data: %s", e)
    except Exception as e:  # noqa: BLE001
        log.error("init: непредвиденная ошибка инициализации parsing_data: %s", e)
