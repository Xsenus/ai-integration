from __future__ import annotations

import pathlib
import sys

ROOT_DIR = pathlib.Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from app.api.analyze_json import _extract_billing_summary, _extract_request_cost  # noqa: E402


def test_extract_request_cost_accepts_usage_shape() -> None:
    payload = {
        "request_cost": {
            "model": "gpt-4o-mini",
            "usage": {
                "prompt_tokens": 110,
                "cached_prompt_tokens": 30,
                "completion_tokens": 25,
                "total_tokens": 165,
            },
            "cost_usd": 0.123,
        }
    }

    result = _extract_request_cost(payload)

    assert result["model"] == "gpt-4o-mini"
    assert result["input_tokens"] == 110
    assert result["cached_input_tokens"] == 30
    assert result["output_tokens"] == 25
    assert result["tokens_total"] == 165
    assert result["cost_usd"] == 0.123
    assert result["raw"]["usage"]["prompt_tokens"] == 110


def test_extract_billing_summary_keeps_raw_payload() -> None:
    payload = {
        "billing_summary": {
            "remaining_usd": 91.5,
            "spend_month_to_date_usd": 8.5,
            "limit_usd": 100,
            "month_to_date": {"spend": 8.5},
        }
    }

    result = _extract_billing_summary(payload)

    assert result["remaining_usd"] == 91.5
    assert result["spend_month_to_date_usd"] == 8.5
    assert result["limit_usd"] == 100
    assert result["raw"]["month_to_date"] == {"spend": 8.5}
