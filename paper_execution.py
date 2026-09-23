"""Deterministic, fail-closed paper fills from prospective Hyperliquid books.

This module prices a hypothetical taker order. A book quote is not an actual
exchange fill: queue changes, latency and rejected orders remain unobservable.
Every returned estimate therefore retains its source and timing evidence.
"""

from __future__ import annotations

import math
from decimal import Decimal, InvalidOperation, ROUND_DOWN
from typing import Any


def _number(value: Any) -> float:
    try:
        parsed = float(value)
    except (TypeError, ValueError, OverflowError):
        return float("nan")
    return parsed if math.isfinite(parsed) else float("nan")


def quote_book(
    book: dict[str, Any],
    *,
    action: str,
    received_at_ms: int,
    detected_at_ms: int,
    notional_usd: float | None = None,
    base_size: float | None = None,
    size_decimals: int | None = None,
    taker_fee_rate: float = 0.00045,
    adverse_slippage_bps: float = 2.0,
    max_book_age_ms: int = 10_000,
    depth_reserve_fraction: float = 0.10,
) -> dict[str, Any]:
    """Walk the opposite L2 side; return a priced quote or a skip reason.

    `notional_usd` is sized from the best executable level, not a mark.
    At exit `base_size` closes exactly the originally quoted quantity.
    Only 90% of displayed size is considered available by default.
    """
    if action not in {"buy", "sell"}:
        return {"ok": False, "reason": "invalid_action"}
    if (notional_usd is None) == (base_size is None):
        return {"ok": False, "reason": "invalid_quantity"}
    if isinstance(size_decimals, bool) or not isinstance(size_decimals, int) or not 0 <= size_decimals <= 8:
        return {"ok": False, "reason": "missing_size_decimals"}
    if received_at_ms < detected_at_ms:
        return {"ok": False, "reason": "quote_precedes_detection"}
    raw_book_time = _number(book.get("time")) if isinstance(book, dict) else float("nan")
    book_time = int(raw_book_time) if math.isfinite(raw_book_time) else 0
    if book_time <= 0 or book_time > received_at_ms + 1_000:
        return {"ok": False, "reason": "invalid_book_time"}
    if received_at_ms - book_time > max_book_age_ms:
        return {"ok": False, "reason": "stale_book"}
    if not 0 <= depth_reserve_fraction < 1:
        return {"ok": False, "reason": "invalid_depth_reserve"}
    if not math.isfinite(taker_fee_rate) or taker_fee_rate < 0:
        return {"ok": False, "reason": "invalid_fee"}
    if not math.isfinite(adverse_slippage_bps) or adverse_slippage_bps < 0:
        return {"ok": False, "reason": "invalid_slippage"}
    levels = book.get("levels") if isinstance(book, dict) else None
    if not isinstance(levels, list) or len(levels) != 2:
        return {"ok": False, "reason": "missing_book_side"}
    if not all(isinstance(side_levels, list) and side_levels for side_levels in levels):
        return {"ok": False, "reason": "missing_book_side"}
    best_bid = _number(levels[0][0].get("px")) if isinstance(levels[0][0], dict) else float("nan")
    best_ask = _number(levels[1][0].get("px")) if isinstance(levels[1][0], dict) else float("nan")
    if not math.isfinite(best_bid) or not math.isfinite(best_ask) or best_bid >= best_ask:
        return {"ok": False, "reason": "crossed_book"}
    opposite = levels[1] if action == "buy" else levels[0]
    if not isinstance(opposite, list) or not opposite:
        return {"ok": False, "reason": "missing_book_side"}
    parsed: list[tuple[float, float]] = []
    for row in opposite:
        if not isinstance(row, dict):
            return {"ok": False, "reason": "invalid_book_level"}
        price, size = _number(row.get("px")), _number(row.get("sz"))
        if not math.isfinite(price) or not math.isfinite(size) or price <= 0 or size <= 0:
            return {"ok": False, "reason": "invalid_book_level"}
        parsed.append((price, size * (1 - depth_reserve_fraction)))
    # Refuse a malformed or crossed level sequence; the API's first side is
    # bid descending and second side is ask ascending.
    if any(
        (parsed[index][0] < parsed[index + 1][0] if action == "sell"
         else parsed[index][0] > parsed[index + 1][0])
        for index in range(len(parsed) - 1)
    ):
        return {"ok": False, "reason": "unordered_book"}
    requested = _number(base_size if base_size is not None else notional_usd)
    if not math.isfinite(requested) or requested <= 0:
        return {"ok": False, "reason": "invalid_quantity"}
    increment = Decimal(1).scaleb(-size_decimals)
    try:
        if base_size is None:
            max_lots = int((Decimal(str(requested)) / Decimal(str(parsed[0][0])) / increment).to_integral_value(
                rounding=ROUND_DOWN
            ))
        else:
            target_decimal = Decimal(str(requested))
            if target_decimal.quantize(increment) != target_decimal:
                return {"ok": False, "reason": "invalid_lot_size"}
    except (InvalidOperation, ZeroDivisionError):
        return {"ok": False, "reason": "invalid_quantity"}
    slip = adverse_slippage_bps / 10_000.0

    def walk(size: float) -> float | None:
        remaining = size
        quote_value = 0.0
        for price, available in parsed:
            take = min(remaining, available)
            quote_value += take * price
            remaining -= take
            if remaining <= max(size * 1e-10, 1e-12):
                break
        if remaining > max(size * 1e-10, 1e-12):
            return None
        return quote_value / size

    if base_size is None:
        if max_lots <= 0:
            return {"ok": False, "reason": "below_minimum_size"}
        low, high = 1, max_lots
        best_size = best_vwap = 0.0
        while low <= high:
            middle = (low + high) // 2
            candidate_size = float(increment * middle)
            candidate_vwap = walk(candidate_size)
            if candidate_vwap is None:
                high = middle - 1
                continue
            candidate_price = candidate_vwap * (1 + slip if action == "buy" else 1 - slip)
            total_entry_cost = candidate_price * candidate_size * (1 + taker_fee_rate)
            if total_entry_cost <= requested + 1e-9:
                best_size, best_vwap = candidate_size, candidate_vwap
                low = middle + 1
            else:
                high = middle - 1
        if best_size <= 0:
            return {"ok": False, "reason": "insufficient_depth"}
        target_size, book_vwap = best_size, best_vwap
        if target_size * book_vwap < requested * 0.95:
            return {"ok": False, "reason": "insufficient_depth"}
    else:
        target_size = float(target_decimal)
        if target_size <= 0:
            return {"ok": False, "reason": "below_minimum_size"}
        walked = walk(target_size)
        if walked is None:
            return {"ok": False, "reason": "insufficient_depth"}
        book_vwap = walked
    fill_price = book_vwap * (1 + slip if action == "buy" else 1 - slip)
    fill_value = fill_price * target_size
    return {
        "ok": True,
        "action": action,
        "coin": str(book.get("coin") or ""),
        "bookTimeMs": book_time,
        "receivedAtMs": received_at_ms,
        "detectedAtMs": detected_at_ms,
        "latencyMs": received_at_ms - detected_at_ms,
        "baseSize": target_size,
        "bookVwap": book_vwap,
        "fillPrice": fill_price,
        "fillValueUsd": fill_value,
        "feeUsd": fill_value * taker_fee_rate,
        "takerFeeRate": taker_fee_rate,
        "adverseSlippageBps": adverse_slippage_bps,
        "depthReserveFraction": depth_reserve_fraction,
        "sizeDecimals": size_decimals,
    }


def paper_trade_result(
    entry: dict[str, Any],
    exit_quote: dict[str, Any],
    *,
    side: str,
    funding_cashflow_usd: float | None,
    initial_notional_usd: float,
) -> dict[str, Any]:
    """Price one fully closed hypothetical trade; funding must be known."""
    if side not in {"long", "short"}:
        return {"complete": False, "reason": "invalid_side"}
    if not entry.get("ok") or not exit_quote.get("ok"):
        return {"complete": False, "reason": "missing_executable_quote"}
    expected_entry = "buy" if side == "long" else "sell"
    expected_exit = "sell" if side == "long" else "buy"
    if entry.get("action") != expected_entry or exit_quote.get("action") != expected_exit:
        return {"complete": False, "reason": "wrong_trade_direction"}
    quantity = _number(entry.get("baseSize"))
    exit_quantity = _number(exit_quote.get("baseSize"))
    if not math.isfinite(quantity) or not math.isclose(quantity, exit_quantity, rel_tol=1e-9):
        return {"complete": False, "reason": "quantity_mismatch"}
    if funding_cashflow_usd is None or not math.isfinite(_number(funding_cashflow_usd)):
        return {"complete": False, "reason": "funding_unverified"}
    initial_notional = _number(initial_notional_usd)
    if not math.isfinite(initial_notional) or initial_notional <= 0:
        return {"complete": False, "reason": "invalid_notional"}
    entry_price, exit_price = _number(entry.get("fillPrice")), _number(exit_quote.get("fillPrice"))
    entry_fee, exit_fee = _number(entry.get("feeUsd")), _number(exit_quote.get("feeUsd"))
    if any(not math.isfinite(value) or value < 0 for value in (entry_price, exit_price, entry_fee, exit_fee)):
        return {"complete": False, "reason": "invalid_quote_value"}
    direction = 1 if side == "long" else -1
    gross_usd = direction * quantity * (exit_price - entry_price)
    fees_usd = entry_fee + exit_fee
    net_usd = gross_usd - fees_usd + funding_cashflow_usd
    return {
        "complete": True,
        "grossUsd": gross_usd,
        "feesUsd": fees_usd,
        "fundingUsd": funding_cashflow_usd,
        "netUsd": net_usd,
        "netReturnPct": net_usd / initial_notional * 100,
    }


def modeled_funding_cashflow(
    rates: list[dict[str, Any]],
    oracle_samples: list[dict[str, Any]],
    *,
    side: str,
    base_size: float,
    entry_at_ms: int,
    exit_at_ms: int,
    history_complete: bool,
    max_oracle_distance_ms: int = 15 * 60 * 1000,
    adverse_oracle_buffer_pct: float = 5.0,
) -> dict[str, Any]:
    """Model settled funding, using deliberately adverse nearby oracle prices.

    Hyperliquid charges signed size × oracle price × hourly rate. The
    historical rate endpoint supplies the rate but not oracle, so this is a
    conservative *model*, not an exact account ledger. Missing any hourly
    event or nearby oracle snapshot prevents a completed net estimate.
    """
    if not history_complete:
        return {"complete": False, "reason": "funding_history_incomplete"}
    if side not in {"long", "short"} or not math.isfinite(base_size) or base_size <= 0:
        return {"complete": False, "reason": "invalid_position"}
    if entry_at_ms <= 0 or exit_at_ms <= entry_at_ms:
        return {"complete": False, "reason": "invalid_holding_interval"}
    if not 0 <= adverse_oracle_buffer_pct < 100:
        return {"complete": False, "reason": "invalid_oracle_buffer"}
    hour_ms = 60 * 60 * 1000
    margin_ms = 60_000
    # The rate timestamp does not establish whether an entry/exit executed
    # before or after the settlement at the same hour. This also catches an
    # exit exactly on the boundary, which the exclusive range below omits.
    for endpoint in (entry_at_ms, exit_at_ms):
        distance = endpoint % hour_ms
        if min(distance, hour_ms - distance) <= margin_ms:
            return {"complete": False, "reason": "funding_boundary_ambiguous"}
    boundaries = range((entry_at_ms // hour_ms + 1) * hour_ms, exit_at_ms, hour_ms)
    direction = 1 if side == "long" else -1
    cashflows: list[dict[str, Any]] = []
    for boundary in boundaries:
        if boundary - entry_at_ms <= margin_ms or exit_at_ms - boundary <= margin_ms:
            return {"complete": False, "reason": "funding_boundary_ambiguous"}
        matches = [
            row for row in rates
            if isinstance(row, dict)
            and math.isfinite(_number(row.get("time")))
            and abs(_number(row.get("time")) - boundary) <= margin_ms
        ]
        if len(matches) != 1:
            return {"complete": False, "reason": "funding_event_missing_or_duplicate"}
        row = matches[0]
        rate = _number(row.get("fundingRate"))
        if not math.isfinite(rate):
            return {"complete": False, "reason": "invalid_funding_rate"}
        valid_oracles = [
            sample for sample in oracle_samples
            if isinstance(sample, dict)
            and math.isfinite(_number(sample.get("observedAtMs")))
            and math.isfinite(_number(sample.get("oraclePrice")))
            and _number(sample.get("oraclePrice")) > 0
            and abs(_number(sample.get("observedAtMs")) - boundary) <= max_oracle_distance_ms
        ]
        if not valid_oracles:
            return {"complete": False, "reason": "oracle_sample_missing"}
        nearest = min(
            valid_oracles,
            key=lambda sample: abs(_number(sample.get("observedAtMs")) - boundary),
        )
        oracle = _number(nearest["oraclePrice"])
        raw_payment = -direction * base_size * oracle * rate
        # A 5% price cushion makes both costs larger and credits smaller.
        factor = 1 + adverse_oracle_buffer_pct / 100 if raw_payment < 0 else 1 - adverse_oracle_buffer_pct / 100
        cashflows.append({
            "time": int(_number(row["time"])),
            "rate": rate,
            "oracleSampleAtMs": int(_number(nearest["observedAtMs"])),
            "oracleSamplePrice": oracle,
            "modeledCashflowUsd": raw_payment * factor,
        })
    return {
        "complete": True,
        "basis": "conservative_oracle_sample_model",
        "adverseOracleBufferPct": adverse_oracle_buffer_pct,
        "eventCount": len(cashflows),
        "cashflowUsd": sum(row["modeledCashflowUsd"] for row in cashflows),
        "events": cashflows,
    }


def stressed_trade_result(
    entry: dict[str, Any],
    exit_quote: dict[str, Any],
    *,
    side: str,
    funding_cashflow_usd: float,
    initial_notional_usd: float,
    fee_multiplier: float = 2.0,
    slippage_multiplier: float = 2.0,
) -> dict[str, Any]:
    """Reprice the same observed depth with doubled modeled friction."""
    if not all(math.isfinite(value) and value >= 1 for value in (fee_multiplier, slippage_multiplier)):
        return {"complete": False, "reason": "invalid_stress_multiplier"}
    stressed = []
    for source in (entry, exit_quote):
        if not source.get("ok"):
            return {"complete": False, "reason": "missing_executable_quote"}
        book_vwap = _number(source.get("bookVwap"))
        quantity = _number(source.get("baseSize"))
        fee_rate = _number(source.get("takerFeeRate"))
        slip_bps = _number(source.get("adverseSlippageBps"))
        if any(not math.isfinite(value) or value < 0 for value in (book_vwap, quantity, fee_rate, slip_bps)):
            return {"complete": False, "reason": "invalid_quote_value"}
        adverse = slip_bps * slippage_multiplier / 10_000
        price = book_vwap * (1 + adverse if source.get("action") == "buy" else 1 - adverse)
        stressed.append({
            **source,
            "fillPrice": price,
            "feeUsd": price * quantity * fee_rate * fee_multiplier,
        })
    result = paper_trade_result(
        stressed[0], stressed[1], side=side,
        funding_cashflow_usd=funding_cashflow_usd,
        initial_notional_usd=initial_notional_usd,
    )
    if result.get("complete"):
        result["feeMultiplier"] = fee_multiplier
        result["slippageMultiplier"] = slippage_multiplier
    return result
