"""Unit tests for prodclass formatting inside the AI analyzer service."""

from __future__ import annotations

import pathlib
import sys

ROOT_DIR = pathlib.Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from app.services.ai_analyzer import (  # noqa: E402
    _resolve_industry_from_prodclass,
    _select_primary_prodclass,
)


def test_select_primary_prodclass_uses_lookup_text_label():
    prod_rows = [
        {
            "prodclass": 17,
            "prodclass_score": 0.87,
        }
    ]
    lookup = {
        17: {
            "text": "Добыча меди",
        }
    }

    result = _select_primary_prodclass(prod_rows, lookup)

    assert result == {
        "id": 17,
        "name": "Добыча меди",
        "label": "[17] Добыча меди",
        "score": 0.87,
    }


def test_resolve_industry_from_prodclass_prefers_industry_label():
    prod_rows = [
        {
            "prodclass": 25,
            "prodclass_score": 0.9,
        }
    ]
    lookup = {
        25: {
            "industry_label": "Металлургия",
            "industry_id": 42,
        }
    }

    label = _resolve_industry_from_prodclass(prod_rows, lookup)

    assert label == "[42] Металлургия"


def test_resolve_industry_from_prodclass_fallback_uses_prodclass_name():
    prod_rows = [
        {
            "prodclass": 33,
            "prodclass_score": 0.55,
        }
    ]
    lookup = {
        33: {
            "text": "Обогащение руды",
        }
    }

    label = _resolve_industry_from_prodclass(prod_rows, lookup)

    assert label == "[33] Обогащение руды"
