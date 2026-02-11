"""Tests for analyze-json response when site is unavailable and OKVED fallback is used."""

from __future__ import annotations

import asyncio
import pathlib
import sys
from datetime import datetime, timezone

ROOT_DIR = pathlib.Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from app.api import analyze_json as analyze_json_mod  # noqa: E402
from app.schemas.analyze_json import AnalyzeFromInnRequest  # noqa: E402
from app.services.parse_site import ParseSiteResponse, SiteUnavailableFallback  # noqa: E402


def test_run_analyze_marks_okved_fallback_as_success(monkeypatch) -> None:
    async def _ensure_service_available(_base_url: str, *, label: str) -> None:
        assert label.startswith("analyze-json:")

    async def _collect_latest_pars_site(_engine, _inn: str):
        return []

    async def _trigger_parse_site(_inn: str) -> ParseSiteResponse:
        now = datetime.now(timezone.utc)
        return ParseSiteResponse(
            status="fallback",
            message="Сайт недоступен",
            started_at=now,
            finished_at=now,
            duration_seconds=0.1,
            duration_ms=100,
            company_id=321,
            domain_1=None,
            domain_2=None,
            url=None,
            planned_domains=[],
            successful_domains=[],
            chunks_inserted=0,
            domains_attempted=0,
            domains_succeeded=0,
            failed_domains=[],
            results=[],
            site_unavailable=SiteUnavailableFallback(
                okved="25.62",
                prodclass_by_okved=17,
            ),
        )

    async def _persist_okved_fallback_snapshot(_engine, *, inn: str, company_id: int | None, prodclass_by_okved: int | None):
        assert inn == "7701234567"
        assert company_id == 321
        assert prodclass_by_okved == 17
        return company_id, 999

    monkeypatch.setattr(analyze_json_mod, "get_postgres_engine", lambda: object())
    monkeypatch.setattr(analyze_json_mod.settings, "AI_ANALYZE_BASE", "http://analyze.local")
    monkeypatch.setattr(analyze_json_mod, "ensure_service_available", _ensure_service_available)
    monkeypatch.setattr(analyze_json_mod, "_collect_latest_pars_site", _collect_latest_pars_site)
    monkeypatch.setattr(analyze_json_mod, "_trigger_parse_site", _trigger_parse_site)
    monkeypatch.setattr(analyze_json_mod, "_persist_okved_fallback_snapshot", _persist_okved_fallback_snapshot)

    result = asyncio.run(
        analyze_json_mod._run_analyze(
            AnalyzeFromInnRequest(inn="7701234567", refresh_site=False),
            source="GET",
        )
    )

    assert result.status == "ok"
    assert result.okved_fallback_used is True
    assert result.prodclass_id == 17
    assert result.pars_id == 999
    assert result.runs
    assert result.runs[0].pars_id == 999
    assert result.runs[0].okved_fallback_used is True


def test_run_analyze_prefers_parse_site_fallback_over_stale_snapshots(monkeypatch) -> None:
    async def _ensure_service_available(_base_url: str, *, label: str) -> None:
        assert label.startswith("analyze-json:")

    async def _collect_latest_pars_site(_engine, _inn: str):
        now = datetime.now(timezone.utc)
        return [
            analyze_json_mod.ParsSiteSnapshot(
                pars_id=111,
                company_id=222,
                text="stale text",
                created_at=now,
                chunk_ids=[1],
                domain="stale.example.com",
                url="https://stale.example.com",
                domain_source="domain_1",
            )
        ]

    async def _trigger_parse_site(_inn: str) -> ParseSiteResponse:
        now = datetime.now(timezone.utc)
        return ParseSiteResponse(
            status="fallback",
            message="Не удалось определить домены для парсинга",
            started_at=now,
            finished_at=now,
            duration_seconds=0.1,
            duration_ms=100,
            company_id=555,
            domain_1=None,
            domain_2=None,
            url=None,
            planned_domains=[],
            successful_domains=[],
            chunks_inserted=0,
            domains_attempted=0,
            domains_succeeded=0,
            failed_domains=[],
            results=[],
            site_unavailable=SiteUnavailableFallback(
                okved="26.30.15",
                prodclass_by_okved=102,
            ),
        )

    async def _persist_okved_fallback_snapshot(_engine, *, inn: str, company_id: int | None, prodclass_by_okved: int | None):
        assert inn == "1841109992"
        assert company_id == 555
        assert prodclass_by_okved == 102
        return company_id, 1001

    async def _unexpected_http_client():
        raise AssertionError("External analyze service must not be called for site_unavailable fallback")

    monkeypatch.setattr(analyze_json_mod, "get_postgres_engine", lambda: object())
    monkeypatch.setattr(analyze_json_mod.settings, "AI_ANALYZE_BASE", "http://analyze.local")
    monkeypatch.setattr(analyze_json_mod, "ensure_service_available", _ensure_service_available)
    monkeypatch.setattr(analyze_json_mod, "_collect_latest_pars_site", _collect_latest_pars_site)
    monkeypatch.setattr(analyze_json_mod, "_trigger_parse_site", _trigger_parse_site)
    monkeypatch.setattr(analyze_json_mod, "_persist_okved_fallback_snapshot", _persist_okved_fallback_snapshot)
    monkeypatch.setattr(analyze_json_mod, "_get_http_client", _unexpected_http_client)

    result = asyncio.run(
        analyze_json_mod._run_analyze(
            AnalyzeFromInnRequest(inn="1841109992", refresh_site=True),
            source="GET",
        )
    )

    assert result.status == "ok"
    assert result.okved_fallback_used is True
    assert result.prodclass_id == 102
    assert result.pars_id == 1001
    assert result.runs
    assert result.runs[0].okved_fallback_used is True
