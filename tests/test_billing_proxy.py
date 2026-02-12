from __future__ import annotations

import asyncio
import pathlib
import sys

import httpx
import pytest
from fastapi import HTTPException

ROOT_DIR = pathlib.Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from app.api import billing  # noqa: E402
from app.config import settings  # noqa: E402


class _DummyResponse:
    def __init__(self, payload: dict[str, object], status_code: int = 200) -> None:
        self._payload = payload
        self.status_code = status_code

    def json(self) -> dict[str, object]:
        return self._payload


class _DummyClient:
    def __init__(self) -> None:
        self.calls = 0

    async def get(self, _target: str) -> _DummyResponse:
        self.calls += 1
        return _DummyResponse({"remaining": 1.23, "limit": 10, "spend": 8.77})


def test_billing_remaining_uses_cache(monkeypatch: pytest.MonkeyPatch) -> None:
    billing._billing_remaining_cache["payload"] = None
    billing._billing_remaining_cache["timestamp"] = 0.0
    monkeypatch.setattr(settings, "AI_ANALYZE_BASE", "http://analyzer")

    client = _DummyClient()

    async def _fake_get_client() -> _DummyClient:
        return client

    monkeypatch.setattr(billing, "_get_http_client", _fake_get_client)

    first = asyncio.run(billing.billing_remaining())
    second = asyncio.run(billing.billing_remaining())

    assert first == second
    assert client.calls == 1


def test_billing_remaining_returns_502_when_upstream_unavailable(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    billing._billing_remaining_cache["payload"] = None
    billing._billing_remaining_cache["timestamp"] = 0.0
    monkeypatch.setattr(settings, "AI_ANALYZE_BASE", "http://analyzer")

    class _FailingClient:
        async def get(self, _target: str) -> _DummyResponse:
            raise httpx.RequestError("boom")

    async def _fake_get_client() -> _FailingClient:
        return _FailingClient()

    monkeypatch.setattr(billing, "_get_http_client", _fake_get_client)

    with pytest.raises(HTTPException) as exc_info:
        asyncio.run(billing.billing_remaining())

    assert exc_info.value.status_code == 502
    assert "ai-site-analyzer unavailable" in str(exc_info.value.detail)
