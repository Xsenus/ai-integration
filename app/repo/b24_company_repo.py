from __future__ import annotations
from typing import Any, Dict, Optional, Tuple

from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.bitrix_company import B24CompanyRaw

def _extract_inn(payload: Dict[str, Any]) -> Optional[str]:
    for k, v in payload.items():
        if isinstance(k, str) and "INN" in k.upper():
            if isinstance(v, str) and v.strip():
                return v.strip()
    return None

def _extract_kpp(payload: Dict[str, Any]) -> Optional[str]:
    for k, v in payload.items():
        if isinstance(k, str) and "KPP" in k.upper():
            if isinstance(v, str) and v.strip():
                return v.strip()
    return None

def _extract_updated_at(payload: Dict[str, Any]) -> Optional[str]:
    return payload.get("DATE_MODIFY") or payload.get("DATE_UPDATE")

async def upsert_company(session: AsyncSession, company: Dict[str, Any]) -> Tuple[str, int]:
    """
    Вставить/обновить компанию.
    Возвращает ("inserted" | "updated" | "skipped", affected_rows).
    Обновление происходит ТОЛЬКО если изменился payload_sha256 (анти-дубликат).
    """
    b24_id = int(company["ID"])
    title = (company.get("TITLE") or "").strip() or None
    inn = _extract_inn(company)
    kpp = _extract_kpp(company)
    updated_at = _extract_updated_at(company)
    sha = B24CompanyRaw.compute_sha256(company)

    ins = pg_insert(B24CompanyRaw).values(
        b24_id=b24_id,
        title=title,
        inn=inn,
        kpp=kpp,
        updated_at=updated_at,
        payload=company,
        payload_sha256=sha,
        is_deleted=False,
    )

    # Обновлять только если хэш изменился
    stmt = ins.on_conflict_do_update(
        index_elements=[B24CompanyRaw.b24_id],
        set_={
            "title": title,
            "inn": inn,
            "kpp": kpp,
            "updated_at": updated_at,
            "payload": company,
            "payload_sha256": sha,
            "is_deleted": False,
        },
        where=(B24CompanyRaw.payload_sha256 != ins.excluded.payload_sha256),
    ).returning(B24CompanyRaw.id, B24CompanyRaw.payload_sha256)

    res = await session.execute(stmt)
    row = res.first()

    # Логика статуса:
    # - если вставка — row есть всегда
    # - если on_conflict_do_update с where… НЕ сработал (дубликат), PostgreSQL не меняет строк — row будет None
    if row is None:
        return "skipped", 0
    # Мы не различаем тут insert/update по return, но можно дергать system column xmax при желании.
    # Для простоты считаем, что была модификация (insert/update).
    return "updated", 1
