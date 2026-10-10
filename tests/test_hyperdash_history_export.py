import importlib.util
from pathlib import Path

import pytest


spec = importlib.util.spec_from_file_location(
    "hyperdash_history_export", Path(__file__).parents[1] / "scripts/fetch_hyperdash_history.py")
export = importlib.util.module_from_spec(spec)
spec.loader.exec_module(export)


def test_short_fill_page_does_not_hide_later_history(monkeypatch):
    # The live facade returned 1,998 rows while much newer executions existed.
    # Model that failure with a short page followed by additional executions.
    responses = iter([[{"time": 100, "tid": 1}], [{"time": 200, "tid": 2}],
                      [{"time": 200, "tid": 2}], []])
    cursors = []

    def request(url, payload):
        cursors.append(payload["startTime"])
        return next(responses)

    monkeypatch.setattr(export, "request", request)
    monkeypatch.setattr(export.time, "sleep", lambda _: None)
    result = export.raw_fills("0x" + "1" * 40, 50, 250, 10)
    assert [r["tid"] for r in result["fills"]] == [1, 2]
    assert cursors == [50, 100, 200, 201]
    assert result["paginationStatus"] == "empty_final_page"
    assert result["chainCoverageVerified"] is False


def test_fill_timestamp_saturation_stays_unverified(monkeypatch):
    monkeypatch.setattr(export, "request", lambda *args: [
        {"time": 100, "tid": i} for i in range(2000)])
    result = export.raw_fills("0x" + "1" * 40, 100, 250, 10)
    assert result["paginationStatus"] == "timestamp_saturation"
    assert result["chainCoverageVerified"] is False


def test_resume_keeps_earlier_fills_and_deduplicates_boundary(monkeypatch):
    previous = {"fills": [{"time": 100, "tid": 1}], "pages": [{"startTime": 50, "count": 1}]}
    responses = iter([[{"time": 100, "tid": 1}, {"time": 200, "tid": 2}], []])
    monkeypatch.setattr(export, "request", lambda *args: next(responses))
    monkeypatch.setattr(export.time, "sleep", lambda _: None)
    result = export.raw_fills("0x" + "1" * 40, 50, 250, 10, previous)
    assert [r["tid"] for r in result["fills"]] == [1, 2]
    assert result["pages"][1]["startTime"] == 100
    assert result["paginationStatus"] == "empty_final_page"


def test_completed_missing_pages_are_not_marked_complete(monkeypatch):
    row = {"endTime": "2026-10-01T00:00:00Z", "coin": "ETH"}
    monkeypatch.setattr(export, "gql", lambda *args: {"tradeCount": 5, "trades": [row]})
    monkeypatch.setattr(export.time, "sleep", lambda _: None)
    result = export.completed_trades("0x" + "1" * 40, 0, 10)
    assert result["paginationStatus"] == "repeated_page"
    assert len(result["trades"]) == 1


def test_provider_closed_pf_is_not_rank_eligible():
    row = {"endTime": "2026-10-01T00:00:00Z", "coin": "ETH",
           "grossPnl": 100, "fundingPnl": -2, "totalFees": 3, "netPnl": 95}
    summary = export.closed_summary({"trades": [row]}, 0, 2000000000000)
    assert summary["closedNetPnlUsd"] == 95
    assert summary["closedOnlyProfitFactor"] is None
    assert summary["rankingEligible"] is False
    assert summary["netPnlEquationExceptions"] == []


def test_nonfinite_financial_data_fails_validation():
    row = {"endTime": "2026-10-01T00:00:00Z", "coin": "ETH",
           "grossPnl": 100, "fundingPnl": 0, "totalFees": 0, "netPnl": "NaN"}
    with pytest.raises(ValueError, match="Nonfinite"):
        export.closed_summary({"trades": [row]}, 0, 2000000000000)
