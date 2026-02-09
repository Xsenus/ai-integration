"""Tests for parsing prodclass fallback from analyze-service responses."""

from __future__ import annotations

import asyncio
import pathlib
import sys

ROOT_DIR = pathlib.Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from app.services import analyze_client


class _FakeResponse:
    def __init__(self, payload: dict[str, object], status_code: int = 200) -> None:
        self._payload = payload
        self.status_code = status_code

    def json(self) -> dict[str, object]:
        return self._payload


def test_fetch_prodclass_by_okved_parses_plain_digit_raw_response(monkeypatch) -> None:
    async def _ensure_service_available(_base_url: str, *, label: str) -> None:
        assert label.startswith("health:")

    async def _post_with_retries(_base_url: str, _path: str, _payload: dict[str, object], *, label: str):
        assert label == "site-unavailable:test"
        return _FakeResponse({"raw_response": "102"})

    monkeypatch.setattr(analyze_client, "get_analyze_base_url", lambda: "http://analyze.local")
    monkeypatch.setattr(analyze_client, "ensure_service_available", _ensure_service_available)
    monkeypatch.setattr(analyze_client, "_post_with_retries", _post_with_retries)

    prompt, raw_response, prodclass_by_okved = asyncio.run(
        analyze_client.fetch_prodclass_by_okved("26.30.15", label="site-unavailable:test")
    )

    assert prompt is None
    assert raw_response == "102"
    assert prodclass_by_okved == 102


def test_fetch_prodclass_by_okved_extracts_digit_from_verbose_raw_response(monkeypatch) -> None:
    async def _ensure_service_available(_base_url: str, *, label: str) -> None:
        assert label.startswith("health:")

    async def _post_with_retries(_base_url: str, _path: str, _payload: dict[str, object], *, label: str):
        assert label == "site-unavailable:test"
        return _FakeResponse({"raw_response": "IB_PRODCLASS = 101"})

    monkeypatch.setattr(analyze_client, "get_analyze_base_url", lambda: "http://analyze.local")
    monkeypatch.setattr(analyze_client, "ensure_service_available", _ensure_service_available)
    monkeypatch.setattr(analyze_client, "_post_with_retries", _post_with_retries)

    _, raw_response, prodclass_by_okved = asyncio.run(
        analyze_client.fetch_prodclass_by_okved("26.30.15", label="site-unavailable:test")
    )

    assert raw_response == "IB_PRODCLASS = 101"
    assert prodclass_by_okved == 101
