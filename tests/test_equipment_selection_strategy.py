"""Tests for equipment selection source strategy."""

from __future__ import annotations

import pathlib
import sys

ROOT_DIR = pathlib.Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from app.services.equipment_selection import _resolve_selection_strategy


def test_strategy_prefers_okved_when_prodclass_by_okved_exists() -> None:
    strategy, score, prodclass_by_okved, reason = _resolve_selection_strategy(
        [
            {
                "description_okved_score": 0.95,
                "okved_score": 0.9,
                "prodclass_by_okved": 21,
            }
        ]
    )

    assert strategy == "okved"
    assert score == 0.95
    assert prodclass_by_okved == 21
    assert "ОКВЭД" in reason


def test_strategy_uses_site_when_prodclass_by_okved_missing() -> None:
    strategy, score, prodclass_by_okved, reason = _resolve_selection_strategy(
        [
            {
                "description_okved_score": 0.9,
                "okved_score": 0.8,
                "prodclass_by_okved": None,
            }
        ]
    )

    assert strategy == "site"
    assert score == 0.9
    assert prodclass_by_okved is None
    assert "сайт" in reason
