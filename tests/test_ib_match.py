"""Unit tests for cosine similarity helper shared across services."""

from __future__ import annotations

import math

import app.services.ib_match as ib_match_mod

from app.services.ib_match import CatalogEntry, SourceRow, _match_rows
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
