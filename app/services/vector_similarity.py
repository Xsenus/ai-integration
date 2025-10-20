"""Utility functions for vector similarity metrics used across services."""

from __future__ import annotations

import logging
import math
from typing import Iterable, Optional, Sequence

log = logging.getLogger("services.vector_similarity")


def _coerce_vector(name: str, raw: Sequence[float] | Iterable[float] | None) -> list[float] | None:
    """Convert an arbitrary vector representation into a list of floats.

    Returns ``None`` when the input is not iterable, empty or contains values that
    cannot be converted to ``float``. A debug log entry is written for each early
    exit so callers can trace why a similarity score could not be produced.
    """

    if raw is None:
        log.debug("cosine_similarity: %s vector is None", name)
        return None

    try:
        iterator = iter(raw)
    except TypeError:
        log.debug("cosine_similarity: %s vector is not iterable", name)
        return None

    values: list[float] = []
    for index, value in enumerate(iterator):
        try:
            values.append(float(value))
        except (TypeError, ValueError):
            log.debug(
                "cosine_similarity: %s vector has non-numeric coordinate at index %s", name, index
            )
            return None

    if not values:
        log.debug("cosine_similarity: %s vector is empty", name)
        return None

    return values


def cosine_similarity(
    vec_a: Sequence[float] | Iterable[float] | None,
    vec_b: Sequence[float] | Iterable[float] | None,
    *,
    clamp: bool = True,
) -> Optional[float]:
    """Calculate cosine similarity between two numeric vectors.

    The function accepts any iterable of numeric values, gracefully handles
    mismatched dimensions, empty vectors and malformed coordinates, and returns
    ``None`` when a score cannot be computed. When ``clamp`` is ``True`` (the
    default) the result is constrained to the ``[-1.0, 1.0]`` interval to mitigate
    floating point drift.
    """

    left = _coerce_vector("left", vec_a)
    right = _coerce_vector("right", vec_b)

    if left is None or right is None:
        return None

    if len(left) != len(right):
        log.debug(
            "cosine_similarity: dimension mismatch (left=%s, right=%s)",
            len(left),
            len(right),
        )
        return None

    dot = 0.0
    norm_left = 0.0
    norm_right = 0.0
    for a, b in zip(left, right):
        dot += a * b
        norm_left += a * a
        norm_right += b * b

    if norm_left <= 0.0 or norm_right <= 0.0:
        log.debug(
            "cosine_similarity: zero magnitude detected (left=%s, right=%s)",
            norm_left,
            norm_right,
        )
        return None

    denom = math.sqrt(norm_left) * math.sqrt(norm_right)
    if denom == 0.0:
        log.debug("cosine_similarity: zero denominator detected")
        return None

    value = dot / denom

    if clamp:
        if value > 1.0:
            return 1.0
        if value < -1.0:
            return -1.0

    return value

