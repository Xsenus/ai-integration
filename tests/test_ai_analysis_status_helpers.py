from __future__ import annotations

from datetime import datetime, timedelta, timezone
import pathlib
import sys

ROOT_DIR = pathlib.Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from app.api.ai_analysis import _resolve_duration_ms, _resolve_progress, _resolve_status  # noqa: E402


def _base_row(**kwargs):
    row = {
        "created_at": None,
        "started_at": None,
        "ended_at": None,
        "sec_duration": None,
    }
    for idx in range(1, 13):
        row[f"step_{idx}"] = False
    row.update(kwargs)
    return row


def test_status_running_when_started_not_finished() -> None:
    now = datetime.now(timezone.utc)
    row = _base_row(created_at=now - timedelta(seconds=5), started_at=now - timedelta(seconds=4))
    assert _resolve_status(row) == "running"


def test_status_completed_when_ended_and_step12() -> None:
    now = datetime.now(timezone.utc)
    row = _base_row(started_at=now - timedelta(seconds=7), ended_at=now, step_12=True)
    assert _resolve_status(row) == "completed"


def test_status_failed_when_ended_without_step12() -> None:
    now = datetime.now(timezone.utc)
    row = _base_row(started_at=now - timedelta(seconds=7), ended_at=now, step_3=True)
    assert _resolve_status(row) == "failed"


def test_progress_completed_is_one() -> None:
    row = _base_row(step_1=True, step_2=True, step_12=True)
    assert _resolve_progress(row, "completed") == 1.0


def test_duration_active_uses_max_of_sec_and_timeline() -> None:
    now = datetime.now(timezone.utc)
    row = _base_row(started_at=now - timedelta(seconds=20), sec_duration=5)
    ms = _resolve_duration_ms(row, "running", now)
    assert ms >= 20_000


def test_duration_terminal_uses_timeline_fallback() -> None:
    start = datetime(2024, 1, 1, tzinfo=timezone.utc)
    end = start + timedelta(seconds=42)
    row = _base_row(started_at=start, ended_at=end, sec_duration=None)
    assert _resolve_duration_ms(row, "completed", end) == 42_000


def test_duration_queued_does_not_grow_before_start() -> None:
    now = datetime.now(timezone.utc)
    row = _base_row(created_at=now - timedelta(minutes=3), started_at=None, sec_duration=None)
    assert _resolve_duration_ms(row, "queued", now) == 0
