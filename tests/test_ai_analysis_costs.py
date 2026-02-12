from __future__ import annotations

import pathlib
import sys

ROOT_DIR = pathlib.Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from app.api.ai_analysis import _aggregate_cost_rows  # noqa: E402


def test_aggregate_cost_rows_sums_tokens_and_cost() -> None:
    rows = [
        {
            "model": "gpt-4o-mini",
            "input_tokens": 100,
            "cached_input_tokens": 20,
            "output_tokens": 40,
            "cost_usd": "0.015",
        },
        {
            "model": "gpt-4o-mini",
            "input_tokens": 50,
            "cached_input_tokens": 0,
            "output_tokens": 10,
            "cost_usd": "0.005",
        },
        {
            "model": "gpt-4.1",
            "input_tokens": 10,
            "cached_input_tokens": 5,
            "output_tokens": 3,
            "cost_usd": "0.1",
        },
    ]

    result = _aggregate_cost_rows(rows)

    assert result["tokens_input"] == 160
    assert result["tokens_cached_input"] == 25
    assert result["tokens_output"] == 53
    assert result["tokens_total"] == 238
    assert result["cost_total_usd"] == 0.12
    assert result["breakdown"] == {"gpt-4.1": 0.1, "gpt-4o-mini": 0.02}
