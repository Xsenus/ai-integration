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

    monkeypatch.setattr(analyze_json_mod, "get_postgres_engine", lambda: object())
    monkeypatch.setattr(analyze_json_mod.settings, "AI_ANALYZE_BASE", "http://analyze.local")
    monkeypatch.setattr(analyze_json_mod, "ensure_service_available", _ensure_service_available)
    monkeypatch.setattr(analyze_json_mod, "_collect_latest_pars_site", _collect_latest_pars_site)
    monkeypatch.setattr(analyze_json_mod, "_trigger_parse_site", _trigger_parse_site)

    result = asyncio.run(
        analyze_json_mod._run_analyze(
            AnalyzeFromInnRequest(inn="7701234567", refresh_site=False),
            source="GET",
        )
    )

    assert result.status == "ok"
    assert result.okved_fallback_used is True
    assert result.prodclass_id == 17
    assert result.runs
    assert result.runs[0].okved_fallback_used is True
