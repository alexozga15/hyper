"""Public quality, data and risk verdicts; not a trading-admission policy."""
from __future__ import annotations

from typing import Any
from available_info_rank import METRIC, evidence_aware_rank


def quality_assessment(wallet: dict[str, Any], dimensions: dict[str, Any]) -> dict[str, Any]:
    rank = wallet.get("recentWinRateRank") or {}
    if not isinstance(rank, dict):
        rank = {}
    underlying = rank.get("underlying180dRank") or rank
    verification = underlying.get("equityCurveVerification") or {}
    availability = underlying.get("scoreComponentsAvailable") or {}
    pf_verified = underlying.get("pfHistoryComplete") is True
    curve_verified = underlying.get("equityCurveVerified") is True
    missing = [key for key in ("calmar", "sortino", "adjustedProfitFactor")
               if not availability.get(key) or (key == "adjustedProfitFactor" and not pf_verified)]
    sample_passed = underlying.get("sampleGatePassed") is True
    verified = bool(underlying.get("rankable") and underlying.get("score") is not None
                    and curve_verified and pf_verified and sample_passed
                    and underlying.get("largestLoserComplete")
                    and underlying.get("largestLoserPct") is not None)
    available_evidence = None
    if rank.get("metric") == METRIC:
        available_evidence = evidence_aware_rank(
            profit_factor=rank.get("adjustedObservedPF"), trend_points=rank.get("trendScore"),
            episodes=int(rank.get("episodesObserved") or 0), losses=int(rank.get("lossesObserved") or 0),
            observed_drawdown_pct=rank.get("observedDrawdownPct"),
            current_open_loss_pct=rank.get("currentOpenLossPctOfTotalPortfolio"),
            profit_factor_verified=rank.get("pfHistoryComplete") is True,
            window_coverage_verified=rank.get("historicalModeVerified") is True,
        )
    risk_reasons = list(rank.get("shadowReasons") or [])
    risk_dimensions = dimensions.get("risk") or {}
    state_ok = (wallet.get("dataQuality") or {}).get("stateOk") is not False
    if risk_reasons or rank.get("label") == "Shadow":
        risk_status = "blocked"
    elif not state_ok or risk_dimensions.get("status") == "unknown":
        risk_status = "unknown"
    else:
        risk_status = "pass" if risk_dimensions.get("status") == "pass" else "blocked"
    quality_status = ("Shadow" if risk_status == "blocked" else
                      underlying.get("label", "Verified") if verified else "InsufficientData")
    return {
        "version": "quality-evidence-v1",
        "quality": {
            "status": quality_status,
            "score": underlying.get("score") if verified else None,
            "periodDays": 180,
            "method": underlying.get("metric"),
            # A stress/available-info score is not a verified performance rank.
            "diagnosticScore": rank.get("score"),
            "diagnosticMethod": rank.get("metric"),
            "availableInfoEvidence": available_evidence,
        },
        "data": {
            "status": "Verified" if verified else "Preliminary",
            "asOf": (wallet.get("dataQuality") or {}).get("qualityRefreshedAt"),
            "missingComponents": missing,
            "equityVerified": curve_verified,
            "profitFactorVerified": pf_verified,
            "sampleGatePassed": sample_passed,
            "dailyReturns": underlying.get("dailyReturns180d"),
            "cashflowsComplete": wallet.get("cashflowHistoryComplete"),
            "reasons": list(verification.get("reasons") or []),
            "verifiedWindows": [name for name, metrics in (wallet.get("equityMetricsByWindow") or {}).items()
                                if (metrics.get("verification") or {}).get("verified")],
        },
        "risk": {"status": risk_status, "reasons": risk_reasons,
                 "asOf": wallet.get("fetchedAt")},
        "copyability": {"status": (dimensions.get("copyability") or {}).get("status", "pending")},
        "legacyCompositeRank": dimensions.get("rank"),
        # Observe the existing decision, never bypass or replace its gates.
        "tradeEligible": bool(dimensions.get("tradeEligible")),
    }


def format_quality_assessment(assessment: dict[str, Any]) -> str:
    quality = assessment.get("quality") or {}
    score = quality.get("score")
    value = "n/a" if score is None else f"{score:.1f}/100"
    return (
        f"Quality {quality.get('periodDays', 180)}D: {quality.get('status', 'InsufficientData')} ({value})"
        f" | Data: {(assessment.get('data') or {}).get('status', 'Preliminary')}"
        f" | Risk: {(assessment.get('risk') or {}).get('status', 'unknown')}"
        f" | Copyability: {(assessment.get('copyability') or {}).get('status', 'pending')}"
    )
