import pytest

from app.services.parse_site import _summarize_okved_scores


def test_summarize_okved_scores_prefers_main_okved():
    scores = {"02": 0.61234, "01": 0.21345}
    entries = [("02", ""), ("01", "")]

    main_score, avg_score, details, notes = _summarize_okved_scores(scores, entries)

    assert main_score == pytest.approx(0.6123, rel=0, abs=1e-4)
    assert avg_score == pytest.approx((0.61234 + 0.21345) / 2, rel=0, abs=1e-4)
    assert [d.code for d in details] == ["02", "01"]
    detail_scores = [d.score for d in details]
    assert detail_scores[0] == pytest.approx(0.6123, abs=1e-4)
    assert detail_scores[1] == pytest.approx(0.2135, abs=1e-4)
    assert notes == []


def test_summarize_okved_scores_reports_missing_main():
    scores = {"02": 0.51}
    entries = [("01", ""), ("02", "")]

    main_score, avg_score, details, notes = _summarize_okved_scores(scores, entries)

    assert main_score is None
    assert avg_score == pytest.approx(0.51, rel=0, abs=1e-4)
    assert [d.code for d in details] == ["02"]
    assert notes == ["Не удалось вычислить скор по основному ОКВЭД"]


def test_summarize_okved_scores_empty():
    main_score, avg_score, details, notes = _summarize_okved_scores({}, [])

    assert main_score is None
    assert avg_score is None
    assert details == []
    assert notes == []
