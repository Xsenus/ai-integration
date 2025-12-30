"""Unit tests for cosine similarity helper shared across services."""

from __future__ import annotations

import asyncio
import math
import pytest

import app.services.ib_match as ib_match_mod

from app.services.ib_match import (
    CatalogEntry,
    SourceRow,
    _match_rows,
    _normalize_prodclass_updates,
)
from app.services.vector_similarity import cosine_similarity


def test_cosine_similarity_returns_one_for_identical_vectors() -> None:
    vector = [0.5, -0.25, 0.75]
    result = cosine_similarity(vector, vector)

    assert result is not None
    assert math.isclose(result, 1.0, rel_tol=1e-9)


def test_cosine_similarity_returns_none_for_mismatched_dimensions() -> None:
    left = [0.6, 0.2, -0.1]
    right = left + [0.3]

    assert cosine_similarity(left, right) is None


def test_cosine_similarity_handles_invalid_coordinates() -> None:
    vector = [0.2, 0.4, 0.6]
    malformed = ["bad", 0.1, 0.2]

    assert cosine_similarity(vector, malformed) is None


def test_cosine_similarity_rejects_zero_vectors() -> None:
    vector = [0.0, 0.0, 0.0]

    assert cosine_similarity(vector, vector) is None


def test_cosine_similarity_supports_negative_scores() -> None:
    result = cosine_similarity([1.0, 0.0], [-1.0, 0.0])

    assert result is not None
    assert math.isclose(result, -1.0, rel_tol=1e-9)


def test_match_rows_clamps_negative_scores_before_persisting() -> None:
    rows = [SourceRow(ai_id=1, text="foo", vector=[1.0, 0.0])]
    entry = CatalogEntry(ib_id=2, name="bar")
    entry.add_vector([-1.0, 0.0], source=ib_match_mod._VECTOR_SOURCE_CATALOG)
    catalog = [entry]

    matches, updates = _match_rows(rows, catalog)

    assert matches[0].score == 0.0
    assert updates[0]["score"] == 0.0


def test_match_rows_uses_name_fallback_when_catalog_vector_is_misleading() -> None:
    rows = [SourceRow(ai_id=5, text="site", vector=[1.0, 0.0])]
    entry = CatalogEntry(ib_id=10, name="pharma")
    entry.add_vector([-1.0, 0.0], source=ib_match_mod._VECTOR_SOURCE_CATALOG)
    entry.add_vector([1.0, 0.0], source=ib_match_mod._VECTOR_SOURCE_NAME)

    matches, updates = _match_rows(rows, [entry])

    assert matches[0].match_ib_id == 10
    assert matches[0].note == "vector_source=name_embedding"
    assert updates[0]["match_id"] == 10
    assert math.isclose(matches[0].score or 0.0, 1.0, rel_tol=1e-9)


def test_normalize_prodclass_updates_preserves_score_for_sql_payload() -> None:
    rows = [SourceRow(ai_id=7, text="pars", vector=[1.0, 0.0])]
    entry = CatalogEntry(ib_id=11, name="label")
    entry.add_vector([1.0, 0.0], source=ib_match_mod._VECTOR_SOURCE_CATALOG)

    _, updates = _match_rows(rows, [entry])
    normalized = _normalize_prodclass_updates(updates)

    assert normalized
    payload = normalized[0]
    assert payload["score"] == payload["prodclass_score"]
    assert payload["prodclass"] == 11
    assert payload["text_pars_id"] == 7


def test_assign_by_inn_handles_missing_parsing_schema(monkeypatch) -> None:
    class FakeResult:
        def __init__(self, rows: list[dict[str, int]]):
            self._rows = rows

        def mappings(self):
            return iter(self._rows)

    class FakeConn:
        def __init__(self) -> None:
            self.primary_rows: list[dict[str, int]] = []

        async def __aenter__(self):
            return self

        async def __aexit__(self, *excinfo):
            return False

        async def execute(self, query, params=None):  # type: ignore[override]
            sql = str(query)
            if "public.clients_requests" in sql:
                return FakeResult(self.primary_rows)
            if "parsing_data.clients_requests" in sql:
                raise ib_match_mod.SQLAlchemyError("schema missing")
            raise AssertionError("Unexpected query")

    class FakeEngine:
        def __init__(self, conn: FakeConn):
            self._conn = conn

        def connect(self):
            return self._conn

    fake_conn = FakeConn()
    fake_engine = FakeEngine(fake_conn)
    monkeypatch.setattr(ib_match_mod, "get_postgres_engine", lambda: fake_engine)

    async def fake_assign_ib_matches(*, client_id: int, reembed_if_exists: bool):
        raise AssertionError("assign_ib_matches should not be called")

    monkeypatch.setattr(ib_match_mod, "assign_ib_matches", fake_assign_ib_matches)

    async def invoke():
        return await ib_match_mod.assign_ib_matches_by_inn(
            inn="1234567890", reembed_if_exists=False
        )

    with pytest.raises(ib_match_mod.IbMatchServiceError) as exc:
        asyncio.run(invoke())

    assert exc.value.status_code == 404
