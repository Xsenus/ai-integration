from __future__ import annotations

import pathlib
import sys

ROOT_DIR = pathlib.Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from app.models.bitrix_company import B24CompanyRaw  # noqa: E402


def test_compute_sha256_stable_for_same_content_with_different_key_order() -> None:
    payload_a = {
        "id": 42,
        "title": "ООО Ромашка",
        "meta": {"kpp": "123", "inn": "456"},
    }
    payload_b = {
        "meta": {"inn": "456", "kpp": "123"},
        "title": "ООО Ромашка",
        "id": 42,
    }

    assert B24CompanyRaw.compute_sha256(payload_a) == B24CompanyRaw.compute_sha256(payload_b)
