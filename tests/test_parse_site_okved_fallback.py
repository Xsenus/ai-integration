"""Unit tests for OKVED fallback trigger when parse-site has no saved site data."""

from __future__ import annotations

import pathlib
import sys

ROOT_DIR = pathlib.Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from app.services.parse_site import ParsedSiteResult, _should_use_okved_fallback


def _result(chunks: int) -> ParsedSiteResult:
    return ParsedSiteResult(
        requested_domain="example.com",
        used_domain="example.com",
        url="https://example.com",
        chunks_inserted=chunks,
        success=True,
    )


def test_should_use_okved_fallback_when_no_results() -> None:
    assert _should_use_okved_fallback([], total_inserted=0) is True


def test_should_use_okved_fallback_when_success_without_chunks() -> None:
    assert _should_use_okved_fallback([_result(0)], total_inserted=0) is True


def test_should_not_use_okved_fallback_when_chunks_saved() -> None:
    assert _should_use_okved_fallback([_result(1)], total_inserted=1) is False
