"""Tests for virtual client fallback in equipment selection."""

from __future__ import annotations

import asyncio
import pathlib
import sys

import pytest

ROOT_DIR = pathlib.Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from app.services.equipment_selection import (
    EquipmentSelectionNotFound,
    _load_client,
    resolve_client_request_id,
)


class _FakeResult:
    def __init__(self, rows):
        self._rows = rows

    def mappings(self):
        return self._rows


class _FakeConnection:
    async def execute(self, _stmt, _params):
        return _FakeResult([])


class _ResolveFakeConnection:
    def __init__(self, *, public_row=None, parsing_data_row=None):
        self.public_row = public_row
        self.parsing_data_row = parsing_data_row

    async def execute(self, stmt, _params):
        sql = str(getattr(stmt, "text", stmt))
        if "FROM public.clients_requests" in sql:
            return _ResolveResult(self.public_row)
        if "FROM parsing_data.clients_requests" in sql:
            return _ResolveResult(self.parsing_data_row)
        raise AssertionError(f"Unexpected SQL: {sql}")


class _ResolveResult:
    def __init__(self, row):
        self._row = row

    def first(self):
        return self._row


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


def test_resolve_client_request_id_falls_back_to_parsing_data() -> None:
    resolved = asyncio.run(
        resolve_client_request_id(
            _ResolveFakeConnection(public_row=None, parsing_data_row=(777,)),
            "1841109992",
        )
    )

    assert resolved == 777


def test_resolve_client_request_id_returns_none_without_rows() -> None:
    resolved = asyncio.run(
        resolve_client_request_id(
            _ResolveFakeConnection(public_row=None, parsing_data_row=None),
            "1841109992",
        )
    )

    assert resolved is None
