"""Tests for virtual client fallback in equipment selection."""

from __future__ import annotations

import asyncio
import pathlib
import sys

import pytest

ROOT_DIR = pathlib.Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from app.services.equipment_selection import EquipmentSelectionNotFound, _load_client


class _FakeResult:
    def __init__(self, rows):
        self._rows = rows

    def mappings(self):
        return self._rows


class _FakeConnection:
    async def execute(self, _stmt, _params):
        return _FakeResult([])


def test_load_client_returns_virtual_row_when_enabled() -> None:
    rows = asyncio.run(
        _load_client(
            _FakeConnection(),
            0,
            allow_virtual_client=True,
            fallback_inn="1841109992",
        )
    )

    assert rows == [
        {
            "id": 0,
            "company_name": None,
            "inn": "1841109992",
            "domain_1": None,
            "started_at": None,
            "ended_at": None,
        }
    ]


def test_load_client_raises_when_virtual_client_disabled() -> None:
    with pytest.raises(EquipmentSelectionNotFound):
        asyncio.run(_load_client(_FakeConnection(), 0, allow_virtual_client=False))
