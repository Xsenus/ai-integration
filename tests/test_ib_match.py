"""Unit tests for cosine similarity helper shared across services."""

from __future__ import annotations

import math

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
