"""Tests for virtual-client sync behavior in equipment selection."""

from __future__ import annotations

import asyncio
import pathlib
import sys

ROOT_DIR = pathlib.Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from app.services import equipment_selection as mod


class _FakeConn:
    pass


def test_build_equipment_tables_skips_sync_for_virtual_client(monkeypatch) -> None:
    async def _load_client(*_args, **_kwargs):
        return [
            {
                "id": 0,
                "company_name": None,
                "inn": "1841109992",
                "domain_1": None,
                "started_at": None,
                "ended_at": None,
            }
        ]

    async def _resolve_company_id(*_args, **_kwargs):
        return 0

    async def _empty(*_args, **_kwargs):
        return []

    async def _oneway(*_args, **_kwargs):
        return [], [], [], []

    async def _twoway(*_args, **_kwargs):
        return [], [], []

    async def _threeway(*_args, **_kwargs):
        return [], []

    def _merge(*_args, **_kwargs):
        return [], []

    sync_calls: list[str] = []

    async def _sync(_conn, table_name, *_args, **_kwargs):
        sync_calls.append(table_name)

    monkeypatch.setattr(mod, "_load_client", _load_client)
    async def _resolve_run_scope(*_args, **_kwargs):
        return mod.RunScope(started_at=None, latest_pars_id=None)

    monkeypatch.setattr(mod, "_resolve_company_id", _resolve_company_id)
    monkeypatch.setattr(mod, "_resolve_run_scope", _resolve_run_scope)
    monkeypatch.setattr(mod, "_load_goods_types", _empty)
    monkeypatch.setattr(mod, "_load_site_equipment", _empty)
    monkeypatch.setattr(mod, "_load_prodclass_rows", _empty)
    monkeypatch.setattr(mod, "_compute_equipment_1way", _oneway)
    monkeypatch.setattr(mod, "_compute_equipment_2way", _twoway)
    monkeypatch.setattr(mod, "_compute_equipment_3way", _threeway)
    monkeypatch.setattr(mod, "_merge_equipment_tables", _merge)
    monkeypatch.setattr(mod, "_sync_equipment_table", _sync)

    report = asyncio.run(
        mod.build_equipment_tables(
            _FakeConn(),
            0,
            allow_virtual_client=True,
            fallback_inn="1841109992",
        )
    )

    assert sync_calls == []
    assert any("Пропускаем синхронизацию EQUIPMENT_*" in message for message in report.log)
