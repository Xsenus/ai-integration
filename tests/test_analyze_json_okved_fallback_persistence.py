"""Tests for persisting OKVED fallback snapshot with varying DB schemas."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass

from app.api import analyze_json as analyze_json_mod


@dataclass
class _FakeResult:
    data: dict[str, int] | None = None

    def mappings(self) -> "_FakeResult":
        return self

    def first(self) -> dict[str, int] | None:
        return self.data


class _FakeConnection:
    def __init__(self) -> None:
        self.last_sql: str | None = None
        self.last_prodclass_params: dict[str, object] | None = None

    async def execute(self, stmt, params=None):
        sql_text = str(getattr(stmt, "text", stmt))
        if "INSERT INTO public.pars_site" in sql_text:
            self.last_sql = sql_text
            return _FakeResult({"id": 77})
        if "INSERT INTO public.ai_site_prodclass" in sql_text:
            self.last_prodclass_params = dict(params)
            return _FakeResult(None)
        if "FROM public.clients_requests" in sql_text:
            return _FakeResult({"id": 321})
        return _FakeResult(None)


class _FakeBegin:
    def __init__(self, conn: _FakeConnection) -> None:
        self._conn = conn

    async def __aenter__(self) -> _FakeConnection:
        return self._conn

    async def __aexit__(self, exc_type, exc, tb) -> None:
        return None


class _FakeEngine:
    def __init__(self, conn: _FakeConnection) -> None:
        self._conn = conn

    def begin(self) -> _FakeBegin:
        return _FakeBegin(self._conn)


def test_persist_okved_fallback_uses_existing_pars_site_columns_only(monkeypatch) -> None:
    fake_conn = _FakeConnection()
    fake_engine = _FakeEngine(fake_conn)

    async def _get_table_columns(_conn, table_name: str, _schema: str = "public"):
        if table_name == "pars_site":
            return {"id", "company_id", "domain_1", "url", "created_at"}
        if table_name == "ai_site_prodclass":
            return {"id", "text_pars_id", "prodclass", "prodclass_score", "okved_score", "prodclass_by_okved"}
        return set()

    monkeypatch.setattr(analyze_json_mod, "_get_table_columns", _get_table_columns)

    company_id, pars_id = asyncio.run(
        analyze_json_mod._persist_okved_fallback_snapshot(
            fake_engine,
            inn="1841109992",
            company_id=115,
            prodclass_by_okved=102,
        )
    )

    assert company_id == 115
    assert pars_id == 77
    assert fake_conn.last_sql is not None
    assert "domain_1" in fake_conn.last_sql
    assert "url" in fake_conn.last_sql
    assert "start" not in fake_conn.last_sql
    assert '"end"' not in fake_conn.last_sql
    assert "text_par" not in fake_conn.last_sql



def test_persist_okved_fallback_persists_null_scores(monkeypatch) -> None:
    fake_conn = _FakeConnection()
    fake_engine = _FakeEngine(fake_conn)

    async def _get_table_columns(_conn, table_name: str, _schema: str = "public"):
        if table_name == "pars_site":
            return {"id", "company_id", "domain_1", "url", "created_at"}
        if table_name == "ai_site_prodclass":
            return {
                "id",
                "text_pars_id",
                "prodclass",
                "prodclass_score",
                "description_score",
                "description_okved_score",
                "okved_score",
                "prodclass_by_okved",
                "score_source",
            }
        return set()

    monkeypatch.setattr(analyze_json_mod, "_get_table_columns", _get_table_columns)

    company_id, pars_id = asyncio.run(
        analyze_json_mod._persist_okved_fallback_snapshot(
            fake_engine,
            inn="1841109992",
            company_id=115,
            prodclass_by_okved=102,
        )
    )

    assert company_id == 115
    assert pars_id == 77
    assert fake_conn.last_prodclass_params is not None
    assert fake_conn.last_prodclass_params["prodclass_score"] is None
    assert fake_conn.last_prodclass_params["description_score"] is None
    assert fake_conn.last_prodclass_params["description_okved_score"] is None
    assert fake_conn.last_prodclass_params["okved_score"] is None
    assert fake_conn.last_prodclass_params["score_source"] == "okved_fallback"
