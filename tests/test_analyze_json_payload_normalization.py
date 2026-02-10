from __future__ import annotations

import pathlib
import sys

ROOT_DIR = pathlib.Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from app.api import analyze_json as analyze_json_mod  # noqa: E402


def test_coerce_text_extracts_nested_mapping_name() -> None:
    value = {"name": {"title": "Лазерная резка"}}

    assert analyze_json_mod._coerce_text(value) == "Лазерная резка"


def test_coerce_text_flattens_iterables() -> None:
    value = ["Станок", {"text": "пресс"}, None]

    assert analyze_json_mod._coerce_text(value) == "Станок, пресс"


def test_sanitize_db_payload_goods_uses_human_readable_name() -> None:
    payload = {
        "goods": [
            {
                "name": {"name": "Криооборудование"},
                "match_id": 17,
                "score": 0.88,
            }
        ]
    }

    sanitized = analyze_json_mod._sanitize_db_payload(payload)

    assert sanitized["goods"][0]["name"] == "Криооборудование"
    assert sanitized["goods"][0]["id"] == 17
    assert sanitized["goods"][0]["score"] == 0.88


def test_normalize_score_keeps_four_decimals() -> None:
    assert analyze_json_mod._normalize_score(0.81549) == 0.8155
    assert analyze_json_mod._normalize_score(81.549) == 0.8155
