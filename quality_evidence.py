"""Public quality, data and risk verdicts; not a trading-admission policy."""
from __future__ import annotations

import math
from typing import Any
from available_info_rank import METRIC, evidence_aware_rank
from quality_period import QUALITY_PERIOD_DAYS, rank_period_days, period_key


def preliminary_quality_score(wallet: dict[str, Any], rank: dict[str, Any]) -> dict[str, Any] | None:
    """Normalize only observed components, without changing admission evidence.

    An incomplete episode subset can bias PF in either direction. Its score is
    explicitly provisional, never a verified replacement for the full window.
    """
    days = rank_period_days(rank)
    if (not str(rank.get("metric", "")).startswith("calmar_sortino_adjusted_pf_trend_")
            or int(wallet.get("qualityPeriodDays") or days) != days):
        return None

    def number(value):
        try:
            result = float(value)
            return result if math.isfinite(result) else None
        except (TypeError, ValueError, OverflowError):
            return None

    components = {}
    weights = {"calmar": 35, "sortino": 25, "adjustedProfitFactor": 20, "trend": 20}
    curve_usable = (rank.get("equityCurveComplete") and rank.get("maxDrawdownPct") is not None
                    and not rank.get("drawdownInvalidDays"))
    if curve_usable:
        for name, scale in (("calmar", 3), ("sortino", 2)):
            value = number(rank.get(period_key(name + "180d", days)))
            if value is not None:
                components[name] = max(0, min(100, value / scale * 100))

    trend = number(rank.get("trendScore"))
    checks = rank.get("trendChecks") or {}
    if (trend is not None and 0 <= trend <= 20
            and (number(rank.get(period_key("dailyReturns180d", days))) or 0) > 0
            and all(k in checks for k in ("pnl7dPositive", "pnl30dAbove7d",
                                           period_key("pnl180dAbove30d", days)))):
        components["trend"] = trend * 5

    pf = None
    if rank.get("currentRiskKnown") is True and (wallet.get("dataQuality") or {}).get("stateOk") is not False:
        profit = number(rank.get(period_key("closedGrossProfit180d", days)))
        loss = number(rank.get(period_key("closedGrossLoss180d", days)))
        costs = number(rank.get(period_key("openCostsLoss180d", days)))
        positions = wallet.get("positions")
        pnls = [number(p.get("unrealizedPnl")) for p in positions if isinstance(p, dict)] if isinstance(positions, list) else None
        episodes = number(rank.get(period_key("episodes180d", days))) or 0
        if (episodes > 0 and all(v is not None and v >= 0 for v in (profit, loss, costs))
                and pnls is not None and len(pnls) == len(positions) and all(v is not None for v in pnls)):
            denominator = loss + costs + sum(max(0, -v) for v in pnls)
            if denominator > 0:
                pf = profit / denominator
            elif profit > 0:
                pf = float("inf")
            if pf is not None:
                components["adjustedProfitFactor"] = max(0, min(100, (pf - 1) / 1.5 * 100))

    coverage = sum(weights[k] for k in components)
    if not coverage:
        return None
    return {
        "score": round(sum(weights[k] * value for k, value in components.items()) / coverage, 1),
        "method": f"observed_components_{days}d_v1",
        "confidence": "Low", "originalWeightCoveragePct": coverage,
        "componentScores": {k: round(v, 2) for k, v in components.items()},
        "effectiveWeights": {k: weights[k] / coverage for k in components},
        "excludedComponents": [k for k in weights if k not in components],
        "observedAdjustedProfitFactor": "inf" if pf == float("inf") else pf,
        "profitFactorVerified": rank.get("pfHistoryComplete") is True,
        "sampleGatePassed": rank.get("sampleGatePassed") is True,
        "tradeEligible": False,
    }


def quality_assessment(wallet: dict[str, Any], dimensions: dict[str, Any]) -> dict[str, Any]:
    rank = wallet.get("recentWinRateRank") or {}
    if not isinstance(rank, dict):
        rank = {}
    underlying = rank.get("underlying180dRank") or rank
    days = rank_period_days(underlying)
    verification = underlying.get("equityCurveVerification") or {}
    availability = underlying.get("scoreComponentsAvailable") or {}
    pf_verified = underlying.get("pfHistoryComplete") is True
    curve_verified = underlying.get("equityCurveVerified") is True
    missing = [key for key in ("calmar", "sortino", "adjustedProfitFactor")
               if not availability.get(key) or (key == "adjustedProfitFactor" and not pf_verified)]
    sample_passed = underlying.get("sampleGatePassed") is True
    period_matches = int(wallet.get("qualityPeriodDays") or days) == days
    verified = bool(period_matches and underlying.get("rankable") and underlying.get("score") is not None
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
    preliminary = None if verified else preliminary_quality_score(wallet, underlying)
    quality_status = ("Shadow" if risk_status == "blocked" else
                      underlying.get("label", "Verified") if verified else
                      "Provisional" if preliminary else "InsufficientData")
    score = underlying.get("score") if verified else preliminary["score"] if preliminary else None
    return {
        "version": "quality-evidence-v2",
        "quality": {
            "status": quality_status,
            "score": score,
            "verifiedScore": underlying.get("score") if verified else None,
            "scoreType": "Verified" if verified else "Provisional" if preliminary else "Unavailable",
            "preliminary": preliminary,
            "periodDays": days,
            "windowStartMs": underlying.get("qualityWindowStartMs"),
            "windowEndMs": underlying.get("qualityWindowEndMs"),
            "method": preliminary["method"] if preliminary else underlying.get("metric"),
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
            "dailyReturns": underlying.get(period_key("dailyReturns180d", days)),
            "cashflowsComplete": wallet.get("cashflowHistoryComplete"),
            "reasons": list(verification.get("reasons") or []) + ([] if period_matches else ["quality_period_mismatch"]),
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
    preliminary = quality.get("preliminary")
    basis = (f" | Provisional / Low; component weight {preliminary['originalWeightCoveragePct']}%"
             if preliminary else "")
    return (
        f"Quality {quality.get('periodDays', QUALITY_PERIOD_DAYS)}D: {quality.get('status', 'InsufficientData')} ({value})"
        + basis
        + f" | Data: {(assessment.get('data') or {}).get('status', 'Preliminary')}"
        f" | Risk: {(assessment.get('risk') or {}).get('status', 'unknown')}"
        f" | Copyability: {(assessment.get('copyability') or {}).get('status', 'pending')}"
    )
