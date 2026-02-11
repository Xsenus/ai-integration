from __future__ import annotations

import pathlib
import sys

ROOT_DIR = pathlib.Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from app.main import _health_ok  # noqa: E402


def test_health_ok_ignores_disabled_connections() -> None:
    connections = {
        "bitrix_data": "ok",
        "parsing_data": "disabled",
        "pp719": "disabled",
        "postgres": "ok",
    }

    assert _health_ok(connections) is True


def test_health_ok_false_when_enabled_connection_is_down() -> None:
    connections = {
        "bitrix_data": "ok",
        "parsing_data": "down",
        "pp719": "disabled",
        "postgres": "ok",
    }

    assert _health_ok(connections) is False


def test_health_ok_true_when_everything_disabled() -> None:
    connections = {
        "bitrix_data": "disabled",
        "parsing_data": "disabled",
        "pp719": "disabled",
        "postgres": "disabled",
    }

    assert _health_ok(connections) is True
