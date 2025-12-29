"""Unit tests for prodclass formatting inside the AI analyzer service."""

from __future__ import annotations

import pathlib
import sys

ROOT_DIR = pathlib.Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from app.services.ai_analyzer import (  # noqa: E402
    _apply_prodclass_name_fallback,
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


def test_select_primary_prodclass_handles_string_identifier():
    prod_rows = [
        {
            "prodclass": "17",
            "prodclass_score": 0.73,
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
        "score": 0.73,
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


def test_resolve_industry_from_prodclass_accepts_string_identifier():
    prod_rows = [
        {
            "prodclass": "33",
            "prodclass_score": 0.41,
        }
    ]
    lookup = {
        33: {
            "text": "Обогащение руды",
        }
    }

    label = _resolve_industry_from_prodclass(prod_rows, lookup)

    assert label == "[33] Обогащение руды"


def test_apply_prodclass_name_fallback_updates_placeholder_label():
    primary = {
        "id": 17,
        "name": None,
        "label": "[17]",
        "score": 0.95,
    }

    updated = _apply_prodclass_name_fallback(primary, "Добыча меди")

    assert updated["name"] == "Добыча меди"
    assert updated["label"] == "[17] Добыча меди"
    # исходный словарь не должен мутироваться
    assert primary["name"] is None
    assert primary["label"] == "[17]"


def test_apply_prodclass_name_fallback_preserves_custom_label():
    primary = {
        "id": 17,
        "name": None,
        "label": "[17] Кастом",
        "score": 0.95,
    }

    updated = _apply_prodclass_name_fallback(primary, "Добыча меди")

    assert updated["name"] == "Добыча меди"
    assert updated["label"] == "[17] Кастом"


def test_select_primary_prodclass_carries_new_scores_and_fallback():
    prod_rows = [
        {
            "prodclass": 31,
            "prodclass_score": 0.85,
            "description_okved_score": 0.9,
            "description_score": 0.95,
            "okved_score": 0.9,
            "prodclass_by_okved": 99,
        }
    ]

    lookup = {
        31: {
            "text": "Литейные предприятия чёрных металлов",
        },
        99: {
            "text": "Фолбэк по ОКВЭД",
        },
    }

    result = _select_primary_prodclass(prod_rows, lookup)

    assert result == {
        "id": 31,
        "name": "Литейные предприятия чёрных металлов",
        "label": "[31] Литейные предприятия чёрных металлов",
        "score": 0.85,
        "description_okved_score": 0.9,
        "description_score": 0.95,
        "okved_score": 0.9,
        "prodclass_by_okved": 99,
    }
