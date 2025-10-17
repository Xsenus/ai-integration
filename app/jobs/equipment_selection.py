"""CLI-утилита для пересоздания таблиц оборудования.

Скрипт использует :func:`app.services.equipment_selection.compute_equipment_selection`
и повторяет шаги, описанные в аналитическом ноутбуке.  Его можно запустить
локально, передав ``clients_requests.id`` через аргументы командной строки.

Пример::

    python -m app.jobs.equipment_selection --client-id 1

Перед запуском необходимо сконфигурировать ``POSTGRES_DATABASE_URL`` в
``.env`` или переменных окружения.
"""

from __future__ import annotations

import argparse
import asyncio
import json
from typing import Any

from sqlalchemy.ext.asyncio import AsyncConnection

from app.db.tx import get_primary_engine, run_on_engine
from app.services.equipment_selection import compute_equipment_selection

try:
    from tabulate import tabulate
except Exception:  # pragma: no cover - опциональная зависимость
    tabulate = None


def _json_default(obj: Any) -> Any:
    if hasattr(obj, "_asdict"):
        return obj._asdict()
    if hasattr(obj, "__dict__"):
        return obj.__dict__
    return str(obj)


async def _run(client_id: int) -> Any:
    engine = get_primary_engine()
    if engine is None:
        raise RuntimeError("POSTGRES_DATABASE_URL не задан — подключение к postgres недоступно")

    async def action(conn: AsyncConnection) -> Any:
        return await compute_equipment_selection(conn, client_id)

    return await run_on_engine(engine, action)


def main() -> None:
    parser = argparse.ArgumentParser(description="Пересоздание таблиц оборудования")
    parser.add_argument(
        "--client-id",
        type=int,
        required=True,
        help="Значение clients_requests.id",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="Вывести подробный отчёт в формате JSON",
    )

    args = parser.parse_args()

    result = asyncio.run(_run(args.client_id))
    payload = result.model_dump() if hasattr(result, "model_dump") else result

    if args.json:
        print(json.dumps(payload, default=_json_default, ensure_ascii=False, indent=2))
        return

    tables = payload.get("sample_tables", [])
    for table in tables:
        lines = table.get("lines", [])
        if not lines:
            title = table.get("title", "Таблица")
            print(f"\n=== {title} ===\nПусто.")
            continue
        print()
        for line in lines:
            print(line)

    print("\n=== Журнал выполнения ===")
    for entry in payload.get("log", []):
        print(f"- {entry}")


if __name__ == "__main__":
    main()

