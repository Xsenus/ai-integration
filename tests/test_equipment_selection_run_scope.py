"""Tests for run-scope filtering in equipment selection."""

from __future__ import annotations

import asyncio
import pathlib
import sys

ROOT_DIR = pathlib.Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from datetime import datetime

from app.services import equipment_selection as mod


def test_load_prodclass_rows_filters_by_current_started_at() -> None:
    captured: dict[str, object] = {}

    class _Result:
        def mappings(self):
            return self

        def __iter__(self):
            return iter([])

    class _Conn:
        async def execute(self, stmt, params=None):
            captured["sql"] = str(getattr(stmt, "text", stmt))
            captured["params"] = dict(params or {})
            return _Result()

    run_scope = mod.RunScope(started_at=datetime(2025, 1, 1, 12, 0, 0), latest_pars_id=None)
    asyncio.run(mod._load_prodclass_rows(_Conn(), 115, run_scope))

    assert "ap.created_at >= :run_started_at" in str(captured["sql"])
    assert captured["params"]["run_started_at"] == datetime(2025, 1, 1, 12, 0, 0)


def test_build_run_scope_sql_uses_latest_pars_when_started_at_missing() -> None:
    run_scope = mod.RunScope(started_at=None, latest_pars_id=999)
    sql, params = mod._build_run_scope_sql(
        run_scope,
        created_at_column="ap.created_at",
        pars_id_column="pst.id",
    )

    assert "pst.id = :latest_pars_id" in sql
    assert params == {"latest_pars_id": 999}
