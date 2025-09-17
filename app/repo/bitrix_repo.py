from __future__ import annotations

from sqlalchemy import select, delete
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.bitrix import DaDataResult, DaDataResultFullJSON


async def replace_dadata_raw(session: AsyncSession, inn: str, payload: dict) -> None:
    """
    В таблице dadata_result_full_json держим ровно одну строку на ИНН:
    удаляем все старые строки по inn и вставляем свежую.
    """
    await session.execute(delete(DaDataResultFullJSON).where(DaDataResultFullJSON.inn == inn))
    await session.execute(
        insert(DaDataResultFullJSON).values(inn=inn, payload=payload)
    )


async def upsert_company_summary(session: AsyncSession, data: dict) -> None:
    """
    Upsert в dadata_result по PK(inn).
    """
    stmt = insert(DaDataResult).values(**data)
    stmt = stmt.on_conflict_do_update(
        index_elements=[DaDataResult.inn],
        set_={k: stmt.excluded[k] for k in data.keys() if k != "inn"},
    )
    await session.execute(stmt)


async def get_last_raw(session: AsyncSession, inn: str) -> dict | None:
    """
    Возвращает последний сохранённый raw JSON по ИНН.
    (с учётом replace_* всегда будет одна запись)
    """
    q = (
        select(DaDataResultFullJSON.payload)
        .where(DaDataResultFullJSON.inn == inn)
        .order_by(DaDataResultFullJSON.id.desc())
        .limit(1)
    )
    res = await session.execute(q)
    return res.scalar_one_or_none()
