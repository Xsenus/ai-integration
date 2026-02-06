"""Unit tests for equipment source selection in AI analyzer."""

from __future__ import annotations

import pathlib
import sys

ROOT_DIR = pathlib.Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from app.services.ai_analyzer import _compose_equipment, _select_equipment_rows  # noqa: E402


def test_select_equipment_rows_prefers_equipment_all_rows() -> None:
    selection_rows = [{"equipment_name": "Асептический наполнитель", "equipment_score": 0.65}]
    site_rows = [{"equipment": "Литьевая машина", "equipment_score": 0.99}]

    result = _select_equipment_rows(selection_rows, site_rows)

    assert result == selection_rows


def test_select_equipment_rows_fallbacks_to_site_rows_when_selection_empty() -> None:
    selection_rows = []
    site_rows = [{"equipment": "Литьевая машина", "equipment_score": 0.99}]

    result = _select_equipment_rows(selection_rows, site_rows)

    assert result == site_rows


def test_compose_equipment_filters_out_rows_below_minimum_score() -> None:
    rows = [
        {"equipment": "Ножницы", "equipment_score": 0.2, "url": "https://example.com"},
        {"equipment": "Стан", "equipment_score": 0.74, "url": "https://example.com"},
    ]

    result = _compose_equipment(rows, lookup=None)

    assert result == []
