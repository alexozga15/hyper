from __future__ import annotations

import base64
import hashlib
import json
import os
import re
import socket
import ssl
import struct
import threading
import time
import urllib.parse
import urllib.error
import urllib.request
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from http import HTTPStatus
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

from coinmarketman import CoinMarketManApiError, CoinMarketManClient


ROOT = Path(__file__).resolve().parent
STATIC_DIR = ROOT / "static"
DATA_DIR = Path(os.environ.get("DATA_DIR", str(ROOT / "data"))).resolve()
WALLETS_FILE = DATA_DIR / "tracked_wallets.json"
ALERTS_FILE = DATA_DIR / "alerts.json"
TELEGRAM_STATE_FILE = DATA_DIR / "telegram_bot_state.json"
WALLET_QUALITY_CACHE_FILE = DATA_DIR / "wallet_quality_cache.json"
HYPERLIQUID_INFO_URL = "https://api.hyperliquid.xyz/info"
HYPERLIQUID_WS_URLS = (
    "wss://api-ui.hyperliquid.xyz/ws",
    "wss://api.hyperliquid.xyz/ws",
)
HEX_ADDRESS_RE = re.compile(r"0x[a-fA-F0-9]{40}")
MAX_IMPORT_BATCH = 100
MAX_DISCOVERY_BATCH = 60
DEFAULT_CONSENSUS_THRESHOLD = 4
SIGNAL_CONVICTION_ALERT_MIN_DELTA = 15.0
SIGNAL_RE_ALERT_VWAP_DELTA_PCT = 1.0
SIGNAL_LIFETIME_MS = 2 * 60 * 60 * 1000
SIGNAL_OUTCOME_RETENTION_MS = 30 * 24 * 60 * 60 * 1000
SIGNAL_OUTCOME_HORIZONS_MS = {
    "15m": 15 * 60 * 1000,
    "1h": 60 * 60 * 1000,
    "4h": 4 * 60 * 60 * 1000,
    "24h": 24 * 60 * 60 * 1000,
}
ACTIONABLE_SIGNAL_PROBABILITY_THRESHOLD = 70.0
EXTREME_SIGNAL_PROBABILITY_THRESHOLD = 85.0
ACTIONABLE_SIGNAL_MIN_WALLETS = 4
ACTIONABLE_SIGNAL_MIN_NET_WALLETS = 2
ACTIONABLE_SIGNAL_MIN_QNET = 1.5
ACTIONABLE_SIGNAL_MIN_INDEPENDENT_WALLETS = 4
ACTIONABLE_SIGNAL_MIN_INDEPENDENT_NET_WALLETS = 3
ACTIONABLE_SIGNAL_MIN_VERIFIED_FRESH_WALLETS = 3
ACTIONABLE_SIGNAL_MIN_FRESH_NET_WALLETS = 3
ACTIONABLE_SIGNAL_MAX_OPPOSITE_FRESH_WALLETS = 1
ACTIONABLE_SIGNAL_MIN_TOP_WALLETS = 2
WALLET_SIGNAL_ACTIVITY_WINDOW_MS = 15 * 60 * 1000
WALLET_SIGNAL_MAX_ENTRY_DISTANCE_PCT = 4.0
WALLET_SIGNAL_MAJOR_ASSET_MAX_ENTRY_DISTANCE_PCT = 1.5
WALLET_SIGNAL_HIGH_BETA_MAX_ENTRY_DISTANCE_PCT = 2.5
WALLET_CORRELATION_MIN_SHARED_EVENTS = 3
WALLET_CORRELATION_MIN_JACCARD = 0.8
ASSET_QUALITY_MIN_CLOSED_TRADES = 5
WALLET_QUARANTINE_MIN_7D_RETURN_PCT = -10.0
WALLET_QUARANTINE_MAX_7D_WIN_RATE = 35.0
CMM_SIGNAL_PROBABILITY_THRESHOLD = 70.0
CMM_WATCH_PROBABILITY_THRESHOLD = 60.0
CMM_ALERT_PROBABILITY_THRESHOLD = 80.0
CMM_SIGNAL_MIN_TOTAL_VALUE = 500_000
CMM_ACTIONABLE_MIN_TOTAL_VALUE = 1_000_000
CMM_SIGNAL_DEFAULT_COINS: tuple[str, ...] = ()
CMM_SIGNAL_FALLBACK_COINS = ("BTC", "ETH", "SOL", "HYPE")
CMM_SIGNAL_DEFAULT_SEGMENTS = (8, 7, 9)
CMM_CONTRARIAN_DEFAULT_SEGMENTS = (12, 13, 14, 15, 16)
CMM_TREND_ENRICHMENT_ENABLED = True
CMM_SIGNAL_TREND_LOOKBACK_HOURS = 1
CMM_SIGNAL_MAX_TREND_COINS = 3
CMM_SIGNAL_MAX_TREND_SEGMENTS_PER_COIN = 2
CMM_SIGNAL_MAX_TREND_REQUESTS = 6
CMM_SIGNAL_CACHE_TTL_MINUTES = 60
CMM_RATE_LIMIT_BACKOFF_MINUTES = 360
CMM_SIGNAL_SEGMENT_LABELS = {
    7: "Leviathan",
    8: "Money Printer",
    9: "Smart Money",
    12: "Exit Liquidity",
    13: "Semi-Rekt",
    14: "Full Rekt",
    15: "Giga-Rekt",
    16: "Shrimp",
}
CMM_SIGNAL_SEGMENT_WEIGHTS = {
    7: 0.85,
    8: 1.0,
    9: 0.7,
}
POSITION_GROUP_DISPLAY_MIN_VALUE = 1_000_000
MIN_POSITION_MESSAGE_WALLETS = 3
FRESH_WALLET_FLOW_MIN_VALUE = 500_000
LARGE_POSITION_ALERT_MIN_VALUE = 700_000
MIN_POSITION_MESSAGE_VALUE = POSITION_GROUP_DISPLAY_MIN_VALUE
NEW_POSITION_ALERT_MIN_VALUE = LARGE_POSITION_ALERT_MIN_VALUE
POSITION_INCREASE_ALERT_MIN_DELTA = LARGE_POSITION_ALERT_MIN_VALUE
POSITION_INCREASE_ALERT_MIN_PCT = 0.5
ALERT_DEDUPE_COOLDOWN_MS = 60 * 60 * 1000
CLUSTERED_OPEN_ALERT_MIN_WALLETS = 3
OPEN_POSITION_ALERT_WINDOW_MS = 5 * 60 * 1000
CLUSTERED_OPEN_ALERT_WINDOW_MS = OPEN_POSITION_ALERT_WINDOW_MS
COUNTED_POSITION_MAX_UNREALIZED_LOSS = -1_000_000
RECENT_ADD_POSITION_MIN_PCT = 0.20
POSITION_RECENT_ADD_WINDOW_MS = 7 * 24 * 60 * 60 * 1000
RECENT_FILL_ALERT_LIMIT = 100
CONSENSUS_SIZE_ALERT_MIN_DELTA = 2
CONSENSUS_SIZE_ALERT_MIN_PCT = 0.5
HYPERLIQUID_DASHBOARD_WORKERS = 3
HYPERLIQUID_SNAPSHOT_WORKERS = 3
HYPERLIQUID_API_RETRY_ATTEMPTS = 3
HYPERLIQUID_API_RETRY_DELAY_SECONDS = 0.25
WALLET_QUALITY_REFRESH_BATCH_SIZE = 3
WALLET_LIVE_FILL_LOOKBACK_MS = 2 * 60 * 60 * 1000
WALLET_RECENT_FILL_CACHE_RETENTION_MS = 7 * 24 * 60 * 60 * 1000
WALLET_RECENT_FILL_CACHE_LIMIT = 200
WALLET_CACHED_QUALITY_FIELDS = (
    "role",
    "realizedPnl",
    "recentRealizedPnl",
    "realizedPnl30d",
    "recentWins",
    "recentLosses",
    "recentClosedTrades",
    "closedTrades30d",
    "grossProfit30d",
    "grossLoss30d",
    "qualityClosedEvents30d",
    "qualityNetPnl30d",
    "qualityProfitFactor30d",
    "qualityTopWinConcentrationPct",
    "qualityHoldout6dEvents",
    "qualityHoldout6dNetPnl",
    "fills30d",
    "daysSinceLastFill",
    "holdingOnly30d",
    "recentWinRateRank",
    "assetQuality",
    "performance",
    "hitRate",
    "openOrderCount",
    "openOrders",
)
LORACLE_WALLET_ADDRESS = "0x8def9f50456c6c4e37fa5d3d57f108ed23992dae"
EXCLUDED_COUNTED_POSITIONS = {
    (LORACLE_WALLET_ADDRESS, "HYPE"),
}
RANKING_MIN_7D_CLOSED_TRADES = 5
RANKING_FULL_CONFIDENCE_7D_CLOSED_TRADES = 20
RANKING_MIN_30D_CLOSED_TRADES = 20
RANKING_FULL_CONFIDENCE_30D_CLOSED_TRADES = 30
CONVICTION_WEIGHT_30D_SHARE = 0.70
CONVICTION_WEIGHT_7D_SHARE = 0.30
CONVICTION_WEIGHT_7D_MAX_EFFECT_PCT = 20.0
ELITE_MIN_QUALITY_SCORE = 65.0
ELITE_MIN_PROFIT_FACTOR = 1.5
ELITE_MAX_DRAWDOWN_PCT = 35.0
ELITE_WALLET_OVERRIDES = {"0xc9e839a529d1a3a46e2b48d20c461d4afecb72e4"}
TOP_CONVICTION_WALLET_COUNT = 10
TOP_CONVICTION_WALLET_MULTIPLIER = 1.5
NON_TOP_CONVICTION_WALLET_MULTIPLIER = 0.5
CONVICTION_WALLET_WEIGHT_MIN = 0.5
CONVICTION_WALLET_WEIGHT_MAX = 1.5
MONTHLY_QUALITY_MIN_CLOSED_EVENTS = 5
MONTHLY_QUALITY_MIN_PROFIT_FACTOR = 1.2
MONTHLY_QUALITY_MAX_WIN_CONCENTRATION_PCT = 60.0
MONTHLY_QUALITY_HOLDOUT_MS = 6 * 24 * 60 * 60 * 1000
MONTHLY_QUALITY_EVENT_WINDOW_MS = 5 * 60 * 1000
BACKTEST_ELITE_WALLETS = {
    "0x8bae3527e5a33fa0cf184f37bc112d071463ab6d",
    "0xa20fb0c9e04063eec5be286e9269028d966646fa",
}
BACKTEST_STANDARD_WALLETS = {
    "0xa5fd942d4badbab4fe84a9e10f565dd40d5f15ff",
    "0x7d5c17cddaabc227c7d1a34ac7f6cdfda6985d48",
    "0x2d99fe0f36c1aebd28a1a2c0e82e8ca13c2ea351",
    "0x418aa6bf98a2b2bc93779f810330d88cde488888",
    "0x2fcb6898d5a0623de19c3691904927685014c4d8",
    "0x9c2a2a966ed8e47f0c8b7e2ec2b91424f229f6a8",
    "0xe9ffe7698f46f96f980f2877e18c43f5b4165903",
    "0x1f67d79afc8d0e7609ddba6c9b657cc635f69981",
}
BACKTEST_REVIEW_WALLETS = {
    "0x350e33a777d510616fbdb483d1de3b50d1edfcfb",
    "0x8607a7d180de23645db594d90621d837749408d5",
    "0x54a7240cea67b8c41b7c7f2b485360f37331aef4",
    "0x63d417a577b50c96f4f09148d4e4d70950db0522",
    "0xf5a523b171032c060d49c39fbf2e9bec473e1286",
}
TOXIC_CONVICTION_WALLET_MAX_30D_PNL = -500_000
RANKING_WINDOW_MS = 7 * 24 * 60 * 60 * 1000
HOLDING_ONLY_WINDOW_MS = 30 * 24 * 60 * 60 * 1000
OIL_POSITION_ALIASES = {"flx:OIL", "cash:WTI", "xyz:BRENTOIL", "xyz:CL"}
RAW_OIL_POSITION_NAMES = {"BRENTOIL", "CL", "WTI", "OIL"}
RAW_COMMODITY_POSITION_NAMES = RAW_OIL_POSITION_NAMES | {"GOLD", "SILVER", "COPPER", "NATGAS"}
RAW_STOCK_INDEX_POSITION_NAMES = {"SP500", "US500", "XYZ100", "NAS100", "NDX", "SPX", "EWY"}
STOCK_POSITION_PREFIXES = {"xyz", "vntl", "km"}
NON_STOCK_MARKET_SUFFIXES = RAW_COMMODITY_POSITION_NAMES

WALLET_SIZE_BANDS = [
    ("Apex", 5_000_000),
    ("Whale", 1_000_000),
    ("Large", 100_000),
    ("Medium", 10_000),
    ("Small", 0),
]

PROFITABILITY_BANDS = [
    ("Money Printer", 1_000_000),
    ("Very Profitable", 100_000),
    ("Profitable", 0),
    ("Unprofitable", -100_000),
    ("Very Unprofitable", -1_000_000),
    ("Rekt", float("-inf")),
]


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def iso_hours_ago(hours: int) -> str:
    return (datetime.now(timezone.utc) - timedelta(hours=hours)).isoformat().replace("+00:00", "Z")


def iso_to_ms(value: Any) -> int:
    if not value:
        return 0
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        return 0
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return int(parsed.timestamp() * 1000)


def env_flag(name: str, default: bool) -> bool:
    raw = os.environ.get(name)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "on"}


def env_int(name: str, default: int) -> int:
    raw = os.environ.get(name)
    if raw is None:
        return default
    try:
        return int(raw)
    except ValueError:
        return default


def env_csv(name: str, default: tuple[str, ...]) -> list[str]:
    raw = os.environ.get(name, "")
    if not raw.strip():
        return list(default)
    return [item.strip() for item in raw.split(",") if item.strip()]


def env_int_csv(name: str, default: tuple[int, ...]) -> list[int]:
    values: list[int] = []
    for item in env_csv(name, tuple(str(value) for value in default)):
        try:
            values.append(int(item))
        except ValueError:
            continue
    return values or list(default)


def to_float(value: Any) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def first_present(source: dict[str, Any], *keys: str) -> Any:
    for key in keys:
        value = source.get(key)
        if value not in (None, ""):
            return value
    return None


def normalize_position_coin(coin: Any) -> str:
    label = str(coin or "Unknown")
    if label in OIL_POSITION_ALIASES:
        return "OIL"
    if label in RAW_OIL_POSITION_NAMES:
        return "OIL"
    if label in RAW_COMMODITY_POSITION_NAMES or label in RAW_STOCK_INDEX_POSITION_NAMES:
        return label
    prefix, separator, suffix = label.partition(":")
    if separator and suffix in NON_STOCK_MARKET_SUFFIXES:
        return "OIL" if suffix in {"BRENTOIL", "CL", "WTI", "OIL"} else suffix
    if separator and prefix in STOCK_POSITION_PREFIXES and suffix and suffix not in NON_STOCK_MARKET_SUFFIXES:
        return suffix
    return label


def normalize_cmm_coin(coin: Any) -> str:
    label = normalize_position_coin(coin)
    prefix, separator, suffix = label.partition(":")
    if separator and suffix:
        return normalize_position_coin(suffix)
    return label


def should_count_position(address: Any, coin: Any) -> bool:
    normalized_address = str(address or "").strip().lower()
    normalized_coin = normalize_position_coin(coin)
    return (normalized_address, normalized_coin) not in EXCLUDED_COUNTED_POSITIONS


def should_count_open_position(address: Any, coin: Any, position: Any) -> bool:
    if not should_count_position(address, coin):
        return False
    if isinstance(position, dict) and to_float(position.get("unrealizedPnl")) < COUNTED_POSITION_MAX_UNREALIZED_LOSS:
        return False
    return True


def is_stock_like_position(coin: Any) -> bool:
    label = str(coin or "Unknown")
    if label in OIL_POSITION_ALIASES:
        return False
    if label in RAW_STOCK_INDEX_POSITION_NAMES:
        return True
    if label in RAW_COMMODITY_POSITION_NAMES:
        return False
    prefix, separator, suffix = label.partition(":")
    return bool(separator and prefix in STOCK_POSITION_PREFIXES and suffix and suffix not in NON_STOCK_MARKET_SUFFIXES)


def is_commodity_like_position(coin: Any) -> bool:
    label = str(coin or "Unknown")
    if label in OIL_POSITION_ALIASES:
        return True
    if label in RAW_COMMODITY_POSITION_NAMES:
        return True
    _, separator, suffix = label.partition(":")
    return bool(separator and suffix in NON_STOCK_MARKET_SUFFIXES)


def normalize_address(value: str) -> str:
    match = HEX_ADDRESS_RE.search(value or "")
    return match.group(0) if match else ""


def short_address(value: str) -> str:
    address = normalize_address(value)
    if not address:
        return value
    return f"{address[:6]}...{address[-4:]}"


def wallet_label(alias: str, address: str) -> str:
    clean_alias = str(alias or "").strip()
    if clean_alias and clean_alias.lower() != address.lower():
        return clean_alias
    return short_address(address)


def format_position_size(value: float) -> str:
    numeric = abs(to_float(value))
    sign = "-" if to_float(value) < 0 else ""
    if numeric >= 1_000_000:
        return f"{sign}{numeric / 1_000_000:.1f}M"
    if numeric >= 1_000:
        return f"{sign}{numeric / 1_000:.1f}K"
    if numeric >= 10:
        return f"{sign}{numeric:,.0f}"
    return f"{sign}{numeric:,.4g}"


def format_price(value: float) -> str:
    numeric = to_float(value)
    if abs(numeric) >= 1_000:
        return f"{numeric:,.0f}"
    if abs(numeric) >= 1:
        return f"{numeric:,.2f}"
    return f"{numeric:,.4g}"


def format_money_thousands(value: float) -> str:
    return f"${round(to_float(value) / 1_000):,.0f}K"


def format_money_compact(value: float) -> str:
    numeric = to_float(value)
    sign = "-" if numeric < 0 else ""
    absolute = abs(numeric)
    if absolute >= 1_000_000:
        return f"{sign}${absolute / 1_000_000:.1f}M"
    if absolute >= 1_000:
        return f"{sign}${absolute / 1_000:.0f}K"
    return f"{sign}${absolute:,.0f}"


def classify_wallet_size(account_value: float) -> str:
    for label, floor in WALLET_SIZE_BANDS:
        if account_value >= floor:
            return label
    return "Small"


def classify_profitability(realized_pnl: float) -> str:
    for label, floor in PROFITABILITY_BANDS:
        if realized_pnl >= floor:
            return label
    return "Rekt"


def signal_action_from_side(side: str) -> str:
    normalized = str(side or "").lower()
    if normalized == "long":
        return "buy"
    if normalized == "short":
        return "sell"
    return "watch"


def clamp(value: float, low: float = 0.0, high: float = 100.0) -> float:
    return max(low, min(to_float(value), high))


def return_score(return_pct: float) -> float:
    return clamp(50.0 + (to_float(return_pct) * 2.5))


def profit_factor_score(profit_factor: float) -> float:
    if profit_factor == float("inf"):
        return 100.0
    return clamp(((to_float(profit_factor) - 0.5) / 2.5) * 100)


def capped_recent_quality_blend(base_30d_score: float, recent_7d_score: float, recent_weight: float) -> float:
    base = clamp(base_30d_score)
    recent = clamp(recent_7d_score)
    max_recent_weight = min(CONVICTION_WEIGHT_7D_SHARE, max(0.0, 1.0 - CONVICTION_WEIGHT_30D_SHARE))
    weight = max(0.0, min(to_float(recent_weight), max_recent_weight))
    blended = (base * (1.0 - weight)) + (recent * weight)
    max_effect = CONVICTION_WEIGHT_7D_MAX_EFFECT_PCT
    return clamp(max(base - max_effect, min(base + max_effect, blended)))


def build_wallet_quality_rank(
    hit_rate: float,
    closed_trade_count: int,
    pnl_7d: float,
    account_value: float,
    *,
    hit_rate_30d: float | None = None,
    closed_trade_count_30d: int | None = None,
    pnl_30d: float | None = None,
    gross_profit_30d: float = 0.0,
    gross_loss_30d: float = 0.0,
    max_drawdown_pct: float = 0.0,
    margin_usage_pct: float = 0.0,
    unrealized_pnl: float = 0.0,
) -> dict[str, Any]:
    normalized_hit_rate = max(0.0, min(to_float(hit_rate), 100.0))
    sample_size_7d = max(0, int(to_float(closed_trade_count)))
    sample_size_30d = max(0, int(to_float(closed_trade_count_30d if closed_trade_count_30d is not None else sample_size_7d)))
    normalized_hit_rate_30d = max(0.0, min(to_float(hit_rate_30d if hit_rate_30d is not None else hit_rate), 100.0))
    pnl_30d_value = to_float(pnl_30d if pnl_30d is not None else pnl_7d)
    account = to_float(account_value)
    confidence_7d = min(sample_size_7d / RANKING_FULL_CONFIDENCE_7D_CLOSED_TRADES, 1.0)
    confidence_30d = min(sample_size_30d / RANKING_FULL_CONFIDENCE_30D_CLOSED_TRADES, 1.0)
    pnl_7d_return_pct = (to_float(pnl_7d) / account) * 100 if account > 0 else 0.0
    pnl_30d_return_pct = (pnl_30d_value / account) * 100 if account > 0 else 0.0
    profit = max(0.0, to_float(gross_profit_30d))
    loss = max(0.0, abs(to_float(gross_loss_30d)))
    if loss > 0:
        profit_factor = profit / loss
    elif profit > 0:
        profit_factor = float("inf")
    else:
        profit_factor = 0.0
    expectancy_pct = ((pnl_30d_value / max(sample_size_30d, 1)) / account) * 100 if account > 0 else 0.0
    win_rate_score = normalized_hit_rate_30d * confidence_30d
    recent_win_rate_score = normalized_hit_rate * confidence_7d
    recent_quality_confidence = min(sample_size_7d / RANKING_MIN_7D_CLOSED_TRADES, 1.0)
    recent_quality_win_rate_score = normalized_hit_rate * recent_quality_confidence
    drawdown_control_score = clamp(100.0 - (to_float(max_drawdown_pct) * 5.0))
    unrealized_return_pct = (to_float(unrealized_pnl) / account) * 100 if account > 0 else 0.0
    unrealized_score = return_score(unrealized_return_pct)
    open_health_score = unrealized_score
    score = (
        return_score(pnl_7d_return_pct) * 0.25
        + return_score(pnl_30d_return_pct) * 0.20
        + profit_factor_score(profit_factor) * 0.15
        + return_score(expectancy_pct) * 0.15
        + win_rate_score * 0.10
        + drawdown_control_score * 0.10
        + open_health_score * 0.05
    )
    quality_30d_score = (
        return_score(pnl_30d_return_pct) * 0.30
        + profit_factor_score(profit_factor) * 0.25
        + return_score(expectancy_pct) * 0.20
        + win_rate_score * 0.15
        + drawdown_control_score * 0.10
    )
    quality_7d_score = (
        return_score(pnl_7d_return_pct) * 0.60
        + recent_quality_win_rate_score * 0.40
    )
    effective_7d_weight = CONVICTION_WEIGHT_7D_SHARE * min(sample_size_7d / RANKING_MIN_7D_CLOSED_TRADES, 1.0)
    conviction_weight_score = capped_recent_quality_blend(quality_30d_score, quality_7d_score, effective_7d_weight)

    elite_eligible = (
        sample_size_30d >= RANKING_MIN_30D_CLOSED_TRADES
        and profit_factor >= ELITE_MIN_PROFIT_FACTOR
        and to_float(max_drawdown_pct) <= ELITE_MAX_DRAWDOWN_PCT
    )

    if sample_size_7d < RANKING_MIN_7D_CLOSED_TRADES and sample_size_30d < RANKING_MIN_30D_CLOSED_TRADES:
        label = "Unranked"
    elif score >= ELITE_MIN_QUALITY_SCORE and elite_eligible:
        label = "Elite"
    elif score >= 65:
        label = "Strong"
    elif score >= 55:
        label = "Balanced"
    elif score >= 45:
        label = "Weak"
    else:
        label = "Cold"

    return {
        "label": label,
        "score": round(score, 1),
        "winRate": round(normalized_hit_rate, 1),
        "winRate30d": round(normalized_hit_rate_30d, 1),
        "sampleSize": sample_size_7d,
        "sampleSize30d": sample_size_30d,
        "confidence": round(confidence_7d, 2),
        "confidence30d": round(confidence_30d, 2),
        "pnl": round(to_float(pnl_7d), 2),
        "pnl30d": round(pnl_30d_value, 2),
        "pnlReturnPct": round(pnl_7d_return_pct, 2),
        "pnl30dReturnPct": round(pnl_30d_return_pct, 2),
        "profitFactor": "inf" if profit_factor == float("inf") else round(profit_factor, 2),
        "expectancyPct": round(expectancy_pct, 3),
        "maxDrawdownPct": round(to_float(max_drawdown_pct), 2),
        "marginUsagePct": round(to_float(margin_usage_pct), 2),
        "openHealthScore": round(open_health_score, 1),
        "hitRateScore": round(recent_win_rate_score, 1),
        "quality7dHitRateScore": round(recent_quality_win_rate_score, 1),
        "hitRate30dScore": round(win_rate_score, 1),
        "pnlScore": round(return_score(pnl_7d_return_pct), 1),
        "pnl30dScore": round(return_score(pnl_30d_return_pct), 1),
        "profitFactorScore": round(profit_factor_score(profit_factor), 1),
        "drawdownScore": round(drawdown_control_score, 1),
        "quality7dScore": round(quality_7d_score, 1),
        "quality30dScore": round(quality_30d_score, 1),
        "convictionWeightScore": round(conviction_weight_score, 1),
        "convictionWeight7dShare": round(effective_7d_weight, 2),
        "eliteEligible": elite_eligible,
        "metric": "multi_period_quality",
    }


def side_from_size(size: float) -> str:
    if size > 0:
        return "Long"
    if size < 0:
        return "Short"
    return "Flat"


def format_error(message: str, status: int = 400) -> tuple[int, dict[str, Any]]:
    return status, {"error": message, "timestamp": now_iso()}


def latest_series_value(points: list[Any]) -> float:
    if not points:
        return 0.0
    latest = points[-1]
    if isinstance(latest, (list, tuple)) and len(latest) > 1:
        return to_float(latest[1])
    return 0.0


def max_drawdown_pct(points: list[Any]) -> float:
    peak: float | None = None
    worst = 0.0
    for point in points:
        if not (isinstance(point, (list, tuple)) and len(point) > 1):
            continue
        value = to_float(point[1])
        if value <= 0:
            continue
        peak = value if peak is None else max(peak, value)
        if peak and peak > 0:
            worst = max(worst, ((peak - value) / peak) * 100)
    return round(worst, 2)


def current_time_ms() -> int:
    return int(time.time() * 1000)


def load_json_file(path: Path, default: Any) -> Any:
    if not path.exists():
        return default
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (ValueError, OSError):
        return default


def save_json_file(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")


def parse_import_lines(raw_text: str) -> tuple[list[dict[str, str]], list[str]]:
    entries: list[dict[str, str]] = []
    invalid: list[str] = []

    for raw_line in raw_text.splitlines():
        line = raw_line.strip()
        if not line:
            continue

        address = normalize_address(line)
        alias = ""
        notes = ""

        if "," in line:
            parts = [part.strip() for part in line.split(",")]
            if len(parts) >= 2:
                first_address = normalize_address(parts[0])
                second_address = normalize_address(parts[1])
                if first_address:
                    address = first_address
                    alias = parts[1] if not second_address else ""
                    notes = ",".join(parts[2:]).strip()
                elif second_address:
                    address = second_address
                    alias = parts[0]
                    notes = ",".join(parts[2:]).strip()

        if address and not alias:
            alias = line.replace(address, "").strip(" -|")

        if not address:
            invalid.append(line)
            continue

        entries.append(
            {
                "address": address,
                "alias": alias[:80],
                "notes": notes[:240],
            }
        )

    return entries[:MAX_IMPORT_BATCH], invalid


@dataclass
class TrackedWallet:
    address: str
    alias: str
    notes: str
    created_at: str

    def to_dict(self) -> dict[str, Any]:
        return {
            "address": self.address,
            "alias": self.alias,
            "notes": self.notes,
            "createdAt": self.created_at,
        }


@dataclass
class WebSocketConnection:
    sock: socket.socket
    remainder: bytes = b""


class WalletStore:
    def __init__(self, path: Path) -> None:
        self.path = path
        self.path.parent.mkdir(parents=True, exist_ok=True)
        if not self.path.exists():
            self.path.write_text("[]\n", encoding="utf-8")

    def list_wallets(self) -> list[TrackedWallet]:
        raw = json.loads(self.path.read_text(encoding="utf-8"))
        return [
            TrackedWallet(
                address=item["address"],
                alias=item.get("alias", ""),
                notes=item.get("notes", ""),
                created_at=item.get("createdAt", now_iso()),
            )
            for item in raw
        ]

    def save_wallets(self, wallets: list[TrackedWallet]) -> None:
        payload = [wallet.to_dict() for wallet in wallets]
        self.path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")

    def upsert_wallet(self, address: str, alias: str, notes: str) -> tuple[TrackedWallet, bool]:
        wallets = self.list_wallets()
        for wallet in wallets:
            if wallet.address.lower() == address.lower():
                wallet.alias = alias or wallet.alias
                wallet.notes = notes if notes else wallet.notes
                self.save_wallets(wallets)
                return wallet, False

        wallet = TrackedWallet(address=address, alias=alias, notes=notes, created_at=now_iso())
        wallets.append(wallet)
        wallets.sort(key=lambda item: (item.alias or item.address).lower())
        self.save_wallets(wallets)
        return wallet, True

    def remove_wallet(self, address: str) -> bool:
        wallets = self.list_wallets()
        filtered = [wallet for wallet in wallets if wallet.address.lower() != address.lower()]
        if len(filtered) == len(wallets):
            return False
        self.save_wallets(filtered)
        return True


class HyperliquidClient:
    def post(self, payload: dict[str, Any], url: str = HYPERLIQUID_INFO_URL) -> Any:
        request = urllib.request.Request(
            url,
            data=json.dumps(payload).encode("utf-8"),
            headers={"Content-Type": "application/json"},
        )
        with urllib.request.urlopen(request, timeout=20) as response:
            return json.load(response)

    def safe_post(self, payload: dict[str, Any], fallback: Any) -> Any:
        try:
            return self.post(payload)
        except (urllib.error.URLError, TimeoutError, ValueError):
            return fallback

    def safe_post_result(
        self,
        payload: dict[str, Any],
        fallback: Any,
        *,
        attempts: int = HYPERLIQUID_API_RETRY_ATTEMPTS,
        retry_delay: float = HYPERLIQUID_API_RETRY_DELAY_SECONDS,
    ) -> dict[str, Any]:
        last_error = ""
        for attempt in range(max(1, attempts)):
            try:
                return {"ok": True, "data": self.post(payload), "error": ""}
            except urllib.error.HTTPError as exc:
                last_error = f"HTTP {exc.code}: {exc.reason}"
            except (urllib.error.URLError, TimeoutError, ValueError) as exc:
                last_error = str(exc)
            if attempt < max(1, attempts) - 1 and retry_delay > 0:
                time.sleep(retry_delay * (attempt + 1))
        return {"ok": False, "data": fallback, "error": last_error or "request failed"}

    def _websocket_connect(self, url: str, timeout: float = 8.0) -> WebSocketConnection:
        parsed = urllib.parse.urlparse(url)
        host = parsed.hostname or ""
        if not host:
            raise ValueError(f"Invalid WebSocket URL: {url}")
        port = parsed.port or (443 if parsed.scheme == "wss" else 80)
        resource = parsed.path or "/"
        if parsed.query:
            resource = f"{resource}?{parsed.query}"

        raw_socket = socket.create_connection((host, port), timeout=timeout)
        if parsed.scheme == "wss":
            context = ssl.create_default_context()
            sock = context.wrap_socket(raw_socket, server_hostname=host)
        else:
            sock = raw_socket
        sock.settimeout(timeout)

        key = base64.b64encode(os.urandom(16)).decode("ascii")
        request = (
            f"GET {resource} HTTP/1.1\r\n"
            f"Host: {host}\r\n"
            "Upgrade: websocket\r\n"
            "Connection: Upgrade\r\n"
            f"Sec-WebSocket-Key: {key}\r\n"
            "Sec-WebSocket-Version: 13\r\n"
            "\r\n"
        )
        sock.sendall(request.encode("ascii"))

        response = b""
        while b"\r\n\r\n" not in response:
            chunk = sock.recv(4096)
            if not chunk:
                raise ConnectionError("WebSocket handshake failed")
            response += chunk

        header_bytes, remainder = response.split(b"\r\n\r\n", 1)
        header_text = header_bytes.decode("utf-8", errors="replace")
        status_line = header_text.splitlines()[0] if header_text else ""
        if " 101 " not in status_line:
            raise ConnectionError(f"WebSocket handshake failed: {status_line}")

        expected_accept = base64.b64encode(
            hashlib.sha1((key + "258EAFA5-E914-47DA-95CA-C5AB0DC85B11").encode("ascii")).digest()
        ).decode("ascii")
        headers = {}
        for line in header_text.splitlines()[1:]:
            if ":" not in line:
                continue
            name, value = line.split(":", 1)
            headers[name.strip().lower()] = value.strip()
        if headers.get("sec-websocket-accept") != expected_accept:
            raise ConnectionError("WebSocket handshake validation failed")

        return WebSocketConnection(sock=sock, remainder=remainder)

    def _websocket_recv_exact(self, connection: WebSocketConnection, size: int) -> bytes:
        chunks = []
        remainder = connection.remainder
        if remainder:
            take = remainder[:size]
            chunks.append(take)
            connection.remainder = remainder[size:]
            size -= len(take)
        while size > 0:
            chunk = connection.sock.recv(size)
            if not chunk:
                raise ConnectionError("Unexpected WebSocket EOF")
            chunks.append(chunk)
            size -= len(chunk)
        return b"".join(chunks)

    def _websocket_send_text(self, connection: WebSocketConnection, message: str) -> None:
        payload = message.encode("utf-8")
        header = bytearray([0x81])
        length = len(payload)
        if length < 126:
            header.append(0x80 | length)
        elif length < 65536:
            header.append(0x80 | 126)
            header.extend(struct.pack("!H", length))
        else:
            header.append(0x80 | 127)
            header.extend(struct.pack("!Q", length))
        mask = os.urandom(4)
        masked = bytes(payload[i] ^ mask[i % 4] for i in range(length))
        connection.sock.sendall(bytes(header) + mask + masked)

    def _websocket_read_json_message(self, connection: WebSocketConnection) -> dict[str, Any]:
        while True:
            first, second = self._websocket_recv_exact(connection, 2)
            opcode = first & 0x0F
            masked = bool(second & 0x80)
            length = second & 0x7F
            if length == 126:
                length = struct.unpack("!H", self._websocket_recv_exact(connection, 2))[0]
            elif length == 127:
                length = struct.unpack("!Q", self._websocket_recv_exact(connection, 8))[0]
            mask = self._websocket_recv_exact(connection, 4) if masked else b""
            payload = self._websocket_recv_exact(connection, length)
            if masked:
                payload = bytes(payload[i] ^ mask[i % 4] for i in range(length))
            if opcode == 0x8:
                raise ConnectionError("WebSocket closed by remote host")
            if opcode == 0x9:
                pong_header = bytearray([0x8A])
                pong_len = len(payload)
                if pong_len < 126:
                    pong_header.append(0x80 | pong_len)
                elif pong_len < 65536:
                    pong_header.append(0x80 | 126)
                    pong_header.extend(struct.pack("!H", pong_len))
                else:
                    pong_header.append(0x80 | 127)
                    pong_header.extend(struct.pack("!Q", pong_len))
                pong_mask = os.urandom(4)
                masked_payload = bytes(payload[i] ^ pong_mask[i % 4] for i in range(pong_len))
                connection.sock.sendall(bytes(pong_header) + pong_mask + masked_payload)
                continue
            if opcode != 0x1:
                continue
            try:
                message = json.loads(payload.decode("utf-8"))
            except (UnicodeDecodeError, json.JSONDecodeError):
                continue
            if isinstance(message, dict):
                return message

    def _subscribe_channel(
        self,
        channel: str,
        subscription: dict[str, Any],
        *,
        address: str,
        timeout: float = 8.0,
    ) -> dict[str, Any]:
        request = json.dumps({"method": "subscribe", "subscription": subscription})
        deadline = time.time() + timeout
        last_error: Exception | None = None
        for url in HYPERLIQUID_WS_URLS:
            connection: WebSocketConnection | None = None
            try:
                connection = self._websocket_connect(url, timeout=timeout)
                self._websocket_send_text(connection, request)
                while time.time() < deadline:
                    remaining = max(0.5, deadline - time.time())
                    connection.sock.settimeout(remaining)
                    message = self._websocket_read_json_message(connection)
                    if message.get("channel") != channel:
                        continue
                    data = message.get("data")
                    if isinstance(data, dict) and str(data.get("user", "")).lower() == address.lower():
                        return data
            except (ConnectionError, OSError, TimeoutError, ValueError, ssl.SSLError) as error:
                last_error = error
            finally:
                if connection is not None:
                    try:
                        connection.sock.close()
                    except OSError:
                        pass
        if last_error is not None:
            raise last_error
        raise TimeoutError(f"Timed out waiting for {channel} for {address}")

    def subscribe_clearinghouse_state(self, address: str, timeout: float = 8.0) -> dict[str, Any]:
        return self._subscribe_channel(
            "clearinghouseState",
            {"type": "clearinghouseState", "user": address},
            address=address,
            timeout=timeout,
        )

    def safe_subscribe_clearinghouse_state(self, address: str, fallback: dict[str, Any]) -> dict[str, Any]:
        try:
            return self.subscribe_clearinghouse_state(address)
        except (ConnectionError, OSError, TimeoutError, ValueError, ssl.SSLError):
            return fallback

    def merge_all_dexs_clearinghouse_state(self, address: str, data: dict[str, Any]) -> dict[str, Any]:
        aggregated = {
            "user": address,
            "marginSummary": {
                "accountValue": "0.0",
                "totalNtlPos": "0.0",
                "totalRawUsd": "0.0",
                "totalMarginUsed": "0.0",
            },
            "crossMarginSummary": {
                "accountValue": "0.0",
                "totalNtlPos": "0.0",
                "totalRawUsd": "0.0",
                "totalMarginUsed": "0.0",
            },
            "crossMaintenanceMarginUsed": "0.0",
            "withdrawable": "0.0",
            "assetPositions": [],
            "time": None,
        }

        states = data.get("clearinghouseStates", []) if isinstance(data, dict) else []
        summary_fields = ("accountValue", "totalNtlPos", "totalRawUsd", "totalMarginUsed")
        latest_time: int | None = None
        for state_entry in states:
            if not (isinstance(state_entry, list) and len(state_entry) == 2):
                continue
            dex, state = state_entry
            if not isinstance(state, dict):
                continue

            aggregated["withdrawable"] = str(to_float(aggregated["withdrawable"]) + to_float(state.get("withdrawable")))
            aggregated["crossMaintenanceMarginUsed"] = str(
                to_float(aggregated["crossMaintenanceMarginUsed"]) + to_float(state.get("crossMaintenanceMarginUsed"))
            )

            for summary_name in ("marginSummary", "crossMarginSummary"):
                source_summary = state.get(summary_name, {})
                target_summary = aggregated[summary_name]
                if not isinstance(source_summary, dict):
                    continue
                for field in summary_fields:
                    target_summary[field] = str(to_float(target_summary.get(field)) + to_float(source_summary.get(field)))

            for raw_position in state.get("assetPositions", []):
                if not isinstance(raw_position, dict):
                    continue
                position_with_dex = dict(raw_position)
                position_with_dex.setdefault("dex", dex)
                aggregated["assetPositions"].append(position_with_dex)

            state_time = state.get("time")
            if isinstance(state_time, (int, float)):
                latest_time = max(latest_time or int(state_time), int(state_time))

        aggregated["time"] = latest_time
        return aggregated

    def subscribe_all_dexs_clearinghouse_state(self, address: str, timeout: float = 8.0) -> dict[str, Any]:
        data = self._subscribe_channel(
            "allDexsClearinghouseState",
            {"type": "allDexsClearinghouseState", "user": address},
            address=address,
            timeout=timeout,
        )
        return self.merge_all_dexs_clearinghouse_state(address, data)

    def safe_subscribe_all_dexs_clearinghouse_state(self, address: str, fallback: dict[str, Any]) -> dict[str, Any]:
        try:
            return self.subscribe_all_dexs_clearinghouse_state(address)
        except (ConnectionError, OSError, TimeoutError, ValueError, ssl.SSLError):
            try:
                return self.subscribe_clearinghouse_state(address)
            except (ConnectionError, OSError, TimeoutError, ValueError, ssl.SSLError):
                return fallback

    def list_markets(self) -> list[str]:
        meta = self.safe_post({"type": "meta"}, {"universe": []})
        universe = meta.get("universe", []) if isinstance(meta, dict) else []
        return [
            asset.get("name", "")
            for asset in universe
            if asset.get("name") and not asset.get("isDelisted")
        ]


class WalletTrackerService:
    def __init__(self, store: WalletStore, client: HyperliquidClient) -> None:
        self.store = store
        self.client = client
        self.alerts_path = ALERTS_FILE
        self.wallet_quality_cache_path = WALLET_QUALITY_CACHE_FILE
        self.cmm_client = CoinMarketManClient()

    def fetch_wallet_role(self, address: str) -> str:
        result = self.client.safe_post({"type": "userRole", "user": address}, {})
        if isinstance(result, dict):
            return str(result.get("role") or result.get("type") or "unknown")
        if isinstance(result, str):
            return result
        return "unknown"

    def fetch_portfolio(self, address: str) -> dict[str, Any]:
        portfolio = self.client.safe_post({"type": "portfolio", "user": address}, [])
        normalized: dict[str, Any] = {}
        if not isinstance(portfolio, list):
            return normalized
        for item in portfolio:
            if isinstance(item, list) and len(item) == 2 and isinstance(item[0], str) and isinstance(item[1], dict):
                normalized[item[0]] = item[1]
        return normalized

    def fetch_portfolio_result(self, address: str) -> dict[str, Any]:
        result = self.client.safe_post_result({"type": "portfolio", "user": address}, [])
        portfolio = result.get("data") if result.get("ok") else []
        normalized: dict[str, Any] = {}
        ok = bool(result.get("ok")) and isinstance(portfolio, list)
        if isinstance(portfolio, list):
            for item in portfolio:
                if isinstance(item, list) and len(item) == 2 and isinstance(item[0], str) and isinstance(item[1], dict):
                    normalized[item[0]] = item[1]
        return {"ok": ok, "data": normalized, "error": result.get("error", "")}

    def fetch_open_orders_result(self, address: str) -> dict[str, Any]:
        result = self.client.safe_post_result({"type": "openOrders", "user": address}, [])
        orders = result.get("data") if result.get("ok") else []
        ok = bool(result.get("ok")) and isinstance(orders, list)
        return {"ok": ok, "data": orders if isinstance(orders, list) else [], "error": result.get("error", "")}

    def fetch_fills_result(self, address: str, start_time: int) -> dict[str, Any]:
        result = self.client.safe_post_result(
            {
                "type": "userFillsByTime",
                "user": address,
                "startTime": start_time,
                "aggregateByTime": True,
            },
            [],
        )
        fills = result.get("data") if result.get("ok") else []
        ok = bool(result.get("ok")) and isinstance(fills, list)
        return {"ok": ok, "data": fills if isinstance(fills, list) else [], "error": result.get("error", "")}

    def build_performance(self, portfolio: dict[str, Any]) -> dict[str, Any]:
        periods = {}
        for period_name in ("day", "week", "month", "allTime"):
            block = portfolio.get(period_name, {})
            pnl_value = latest_series_value(block.get("pnlHistory", []))
            account_value = latest_series_value(block.get("accountValueHistory", []))
            periods[period_name] = {
                "pnl": pnl_value,
                "accountValue": account_value,
                "volume": to_float(block.get("vlm")),
                "maxDrawdownPct": max_drawdown_pct(block.get("accountValueHistory", [])),
            }
        return periods

    def fetch_wallet_snapshot(
        self,
        wallet: TrackedWallet,
        *,
        full_quality_refresh: bool = True,
        cached_snapshot: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        now_ms = current_time_ms()
        cutoff_7d_ms = now_ms - RANKING_WINDOW_MS
        cutoff_30d_ms = now_ms - HOLDING_ONLY_WINDOW_MS
        cutoff_holdout_ms = now_ms - MONTHLY_QUALITY_HOLDOUT_MS
        fills_start_ms = cutoff_30d_ms if full_quality_refresh else now_ms - WALLET_LIVE_FILL_LOOKBACK_MS
        with ThreadPoolExecutor(max_workers=HYPERLIQUID_SNAPSHOT_WORKERS) as executor:
            futures = {
                "state": executor.submit(
                    self.client.safe_subscribe_all_dexs_clearinghouse_state,
                    wallet.address,
                    {
                        "user": wallet.address,
                        "marginSummary": {},
                        "crossMarginSummary": {},
                        "withdrawable": "0",
                        "assetPositions": [],
                        "time": None,
                    },
                ),
                "fills": executor.submit(self.fetch_fills_result, wallet.address, fills_start_ms),
            }
            if full_quality_refresh:
                futures["orders"] = executor.submit(self.fetch_open_orders_result, wallet.address)
                futures["role"] = executor.submit(self.fetch_wallet_role, wallet.address)
                futures["portfolio"] = executor.submit(self.fetch_portfolio_result, wallet.address)

        state = futures["state"].result()
        fills_result = futures["fills"].result()
        cached = cached_snapshot if isinstance(cached_snapshot, dict) else {}
        orders_result = (
            futures["orders"].result()
            if "orders" in futures
            else {"ok": True, "data": cached.get("openOrders", []), "error": ""}
        )
        role = futures["role"].result() if "role" in futures else str(cached.get("role") or "unknown")
        portfolio_result = (
            futures["portfolio"].result()
            if "portfolio" in futures
            else {"ok": True, "data": cached.get("portfolio", {}), "error": ""}
        )
        open_orders = orders_result.get("data", []) if isinstance(orders_result, dict) else []
        fills = fills_result.get("data", []) if isinstance(fills_result, dict) else []
        portfolio = portfolio_result.get("data", {}) if isinstance(portfolio_result, dict) else {}
        fills_ok = bool(isinstance(fills_result, dict) and fills_result.get("ok"))
        portfolio_ok = bool(isinstance(portfolio_result, dict) and portfolio_result.get("ok"))
        orders_ok = bool(isinstance(orders_result, dict) and orders_result.get("ok"))

        margin_summary = state.get("marginSummary", {})
        positions = []
        long_exposure = 0.0
        short_exposure = 0.0
        unrealized_pnl = 0.0

        for raw_position in state.get("assetPositions", []):
            position = raw_position.get("position", {})
            size = to_float(position.get("szi"))
            position_value = to_float(position.get("positionValue"))
            position_unrealized_pnl = to_float(position.get("unrealizedPnl"))
            unrealized_pnl += position_unrealized_pnl
            if size > 0:
                long_exposure += position_value
            elif size < 0:
                short_exposure += position_value

            positions.append(
                {
                    "coin": position.get("coin", "Unknown"),
                    "side": side_from_size(size),
                    "size": size,
                    "entryPx": to_float(position.get("entryPx")),
                    "positionValue": position_value,
                    "unrealizedPnl": position_unrealized_pnl,
                    "returnOnEquity": to_float(position.get("returnOnEquity")),
                    "marginUsed": to_float(position.get("marginUsed")),
                    "liquidationPx": to_float(position.get("liquidationPx")),
                    "leverage": position.get("leverage", {}),
                }
            )

        positions.sort(key=lambda item: abs(item["positionValue"]), reverse=True)

        recent_fills = []
        recent_realized_pnl = 0.0
        realized_pnl_30d = 0.0
        gross_profit_30d = 0.0
        gross_loss_30d = 0.0
        win_count = 0
        loss_count = 0
        win_count_30d = 0
        loss_count_30d = 0
        fills_30d_count = 0
        last_fill_time = 0
        asset_trade_stats: dict[str, dict[str, float]] = {}
        quality_events: dict[tuple[str, str, int], float] = {}
        for fill in fills:
            closed_pnl = to_float(fill.get("closedPnl"))
            fee = abs(to_float(fill.get("fee")))
            fill_time = int(to_float(fill.get("time")))
            last_fill_time = max(last_fill_time, fill_time)
            if fill_time >= cutoff_30d_ms:
                fills_30d_count += 1
                if closed_pnl != 0:
                    event_key = (
                        normalize_position_coin(fill.get("coin")),
                        str(fill.get("dir") or "").lower(),
                        fill_time // MONTHLY_QUALITY_EVENT_WINDOW_MS,
                    )
                    quality_events[event_key] = quality_events.get(event_key, 0.0) + closed_pnl - fee
                    realized_pnl_30d += closed_pnl
                    if closed_pnl > 0:
                        gross_profit_30d += closed_pnl
                        win_count_30d += 1
                    elif closed_pnl < 0:
                        gross_loss_30d += abs(closed_pnl)
                        loss_count_30d += 1
                    asset_coin = normalize_position_coin(fill.get("coin"))
                    asset_bucket = asset_trade_stats.setdefault(
                        asset_coin,
                        {"closedTrades": 0.0, "wins": 0.0, "losses": 0.0, "pnl": 0.0},
                    )
                    asset_bucket["closedTrades"] += 1
                    asset_bucket["pnl"] += closed_pnl
                    if closed_pnl > 0:
                        asset_bucket["wins"] += 1
                    else:
                        asset_bucket["losses"] += 1
            is_7d_closed_fill = fill_time >= cutoff_7d_ms and closed_pnl != 0
            if is_7d_closed_fill:
                recent_realized_pnl += closed_pnl
                if closed_pnl > 0:
                    win_count += 1
                elif closed_pnl < 0:
                    loss_count += 1

            if len(recent_fills) < RECENT_FILL_ALERT_LIMIT:
                recent_fills.append(
                    {
                        "coin": fill.get("coin", "Unknown"),
                        "direction": fill.get("dir", "Unknown"),
                        "price": to_float(fill.get("px")),
                        "size": to_float(fill.get("sz")),
                        "closedPnl": closed_pnl,
                        "fee": fee,
                        "time": fill.get("time"),
                    }
                )

        normalized_orders = []
        for order in open_orders[:25]:
            normalized_orders.append(
                {
                    "coin": order.get("coin", "Unknown"),
                    "side": "Buy" if order.get("side") == "B" else "Sell",
                    "limitPx": to_float(order.get("limitPx")),
                    "size": to_float(order.get("sz")),
                    "orderType": order.get("orderType", "limit"),
                    "reduceOnly": bool(order.get("reduceOnly")),
                    "timestamp": order.get("timestamp"),
                }
            )

        performance = self.build_performance(portfolio)
        all_time_realized = performance.get("allTime", {}).get("pnl", 0.0)
        recent_closed_trade_count = win_count + loss_count
        hit_rate = (win_count / max(recent_closed_trade_count, 1)) * 100
        closed_trade_count_30d = win_count_30d + loss_count_30d
        hit_rate_30d = (win_count_30d / max(closed_trade_count_30d, 1)) * 100
        quality_event_pnls = list(quality_events.values())
        quality_wins = [pnl for pnl in quality_event_pnls if pnl > 0]
        quality_losses = [abs(pnl) for pnl in quality_event_pnls if pnl < 0]
        quality_gross_profit = sum(quality_wins)
        quality_gross_loss = sum(quality_losses)
        quality_profit_factor = (
            quality_gross_profit / quality_gross_loss
            if quality_gross_loss > 0
            else (float("inf") if quality_gross_profit > 0 else 0.0)
        )
        quality_top_win_concentration_pct = (
            max(quality_wins) / quality_gross_profit * 100 if quality_gross_profit > 0 else 100.0
        )
        holdout_event_pnls = [
            pnl
            for (_, _, time_bucket), pnl in quality_events.items()
            if time_bucket * MONTHLY_QUALITY_EVENT_WINDOW_MS >= cutoff_holdout_ms
        ]

        account_value = to_float(margin_summary.get("accountValue"))
        total_notional = to_float(margin_summary.get("totalNtlPos"))
        margin_used = to_float(margin_summary.get("totalMarginUsed"))
        withdrawable = to_float(state.get("withdrawable"))
        margin_usage_pct = (margin_used / account_value) * 100 if account_value > 0 else 0.0
        holding_only_30d = bool(positions) and fills_ok and fills_30d_count == 0 and len(open_orders) == 0
        days_since_last_fill = None
        if last_fill_time:
            days_since_last_fill = round(max(0, now_ms - last_fill_time) / (24 * 60 * 60 * 1000), 1)
        discovery_score = account_value + (abs(total_notional) * 0.2) + max(all_time_realized, 0.0)
        recent_win_rate_rank = build_wallet_quality_rank(
            hit_rate,
            recent_closed_trade_count,
            recent_realized_pnl,
            account_value,
            hit_rate_30d=hit_rate_30d,
            closed_trade_count_30d=closed_trade_count_30d,
            pnl_30d=realized_pnl_30d,
            gross_profit_30d=gross_profit_30d,
            gross_loss_30d=gross_loss_30d,
            max_drawdown_pct=performance.get("month", {}).get("maxDrawdownPct", 0.0),
            margin_usage_pct=margin_usage_pct,
            unrealized_pnl=unrealized_pnl,
        )
        if wallet.address.lower() in ELITE_WALLET_OVERRIDES:
            recent_win_rate_rank = {
                **recent_win_rate_rank,
                "label": "Elite",
                "eliteOverride": True,
                "eliteEligible": True,
            }

        asset_quality = {
            coin: {
                "closedTrades": int(item["closedTrades"]),
                "winRate": round((item["wins"] / max(item["closedTrades"], 1.0)) * 100, 1),
                "pnl": round(item["pnl"], 2),
            }
            for coin, item in asset_trade_stats.items()
        }

        snapshot = {
            "address": wallet.address,
            "alias": wallet.alias,
            "notes": wallet.notes,
            "createdAt": wallet.created_at,
            "role": role,
            "fetchedAt": now_iso(),
            "accountValue": account_value,
            "withdrawable": withdrawable,
            "marginUsed": margin_used,
            "totalNotional": total_notional,
            "unrealizedPnl": unrealized_pnl,
            "realizedPnl": all_time_realized,
            "recentRealizedPnl": recent_realized_pnl,
            "realizedPnl30d": realized_pnl_30d,
            "recentWins": win_count,
            "recentLosses": loss_count,
            "recentClosedTrades": recent_closed_trade_count,
            "closedTrades30d": win_count_30d + loss_count_30d,
            "grossProfit30d": gross_profit_30d,
            "grossLoss30d": gross_loss_30d,
            "qualityClosedEvents30d": len(quality_event_pnls),
            "qualityNetPnl30d": round(sum(quality_event_pnls), 2),
            "qualityProfitFactor30d": (
                "inf" if quality_profit_factor == float("inf") else round(quality_profit_factor, 2)
            ),
            "qualityTopWinConcentrationPct": round(quality_top_win_concentration_pct, 1),
            "qualityHoldout6dEvents": len(holdout_event_pnls),
            "qualityHoldout6dNetPnl": round(sum(holdout_event_pnls), 2),
            "fills30d": fills_30d_count,
            "daysSinceLastFill": days_since_last_fill,
            "holdingOnly30d": holding_only_30d,
            "recentWinRateRank": recent_win_rate_rank,
            "assetQuality": asset_quality,
            "dataQuality": {
                "fillsOk": fills_ok,
                "portfolioOk": portfolio_ok,
                "ordersOk": orders_ok,
                "fillsError": fills_result.get("error", "") if isinstance(fills_result, dict) else "",
                "portfolioError": portfolio_result.get("error", "") if isinstance(portfolio_result, dict) else "",
                "ordersError": orders_result.get("error", "") if isinstance(orders_result, dict) else "",
                "fillsDegraded": False,
            },
            "openOrderCount": len(open_orders),
            "positions": positions,
            "positionCount": len(positions),
            "openOrders": normalized_orders,
            "recentFills": recent_fills,
            "exposure": {
                "long": long_exposure,
                "short": short_exposure,
                "net": long_exposure - short_exposure,
            },
            "cohorts": {
                "walletSize": classify_wallet_size(account_value),
                "profitability": classify_profitability(all_time_realized),
            },
            "performance": performance,
            "discoveryScore": round(discovery_score, 2),
            "hitRate": hit_rate,
        }

        cached_recent_fills = cached.get("recentFills", []) if isinstance(cached.get("recentFills"), list) else []
        merged_recent_fills: dict[tuple[Any, ...], dict[str, Any]] = {}
        for fill in [*cached_recent_fills, *recent_fills]:
            fill_time = int(to_float(fill.get("time")))
            if fill_time < now_ms - WALLET_RECENT_FILL_CACHE_RETENTION_MS:
                continue
            key = (
                normalize_position_coin(fill.get("coin")),
                str(fill.get("direction") or ""),
                fill_time,
                to_float(fill.get("price")),
                to_float(fill.get("size")),
            )
            merged_recent_fills[key] = fill
        snapshot["recentFills"] = sorted(
            merged_recent_fills.values(),
            key=lambda fill: int(to_float(fill.get("time"))),
            reverse=True,
        )[:WALLET_RECENT_FILL_CACHE_LIMIT]

        quality_refresh_succeeded = full_quality_refresh and fills_ok and portfolio_ok
        use_cached_quality = bool(cached) and not quality_refresh_succeeded
        if use_cached_quality:
            for field in WALLET_CACHED_QUALITY_FIELDS:
                if field in cached:
                    snapshot[field] = cached[field]
        if recent_fills:
            latest_live_fill_ms = max(int(to_float(fill.get("time"))) for fill in recent_fills)
            snapshot["holdingOnly30d"] = False
            snapshot["daysSinceLastFill"] = round(max(0, now_ms - latest_live_fill_ms) / (24 * 60 * 60 * 1000), 1)
        snapshot["dataQuality"].update(
            {
                "qualityRefreshAttempted": full_quality_refresh,
                "qualityRefreshSucceeded": quality_refresh_succeeded,
                "qualityCacheHit": use_cached_quality,
                "qualityRefreshedAt": cached.get("qualityRefreshedAt", "") if use_cached_quality else snapshot["fetchedAt"],
            }
        )
        return snapshot

    def cached_wallet_quality_snapshot(self, snapshot: dict[str, Any]) -> dict[str, Any]:
        return {
            **{field: snapshot.get(field) for field in WALLET_CACHED_QUALITY_FIELDS if field in snapshot},
            "recentFills": snapshot.get("recentFills", []),
            "qualityRefreshedAt": snapshot.get("fetchedAt", now_iso()),
            "refreshedAtMs": current_time_ms(),
        }

    def wallet_quality_refresh_addresses(
        self,
        wallets: list[TrackedWallet],
        cached_wallets: dict[str, Any],
    ) -> set[str]:
        addresses = [wallet.address.lower() for wallet in wallets]
        if not cached_wallets:
            return set(addresses)
        ordered = sorted(
            addresses,
            key=lambda address: (
                address in cached_wallets,
                int(to_float(cached_wallets.get(address, {}).get("refreshedAtMs"))),
            ),
        )
        return set(ordered[:WALLET_QUALITY_REFRESH_BATCH_SIZE])

    def build_holding_only_wallets(self, snapshots: list[dict[str, Any]], *, limit: int = 20) -> list[dict[str, Any]]:
        holders = []
        for wallet in snapshots:
            if not wallet.get("holdingOnly30d"):
                continue
            positions = wallet.get("positions", [])
            holders.append(
                {
                    "address": wallet.get("address", ""),
                    "alias": wallet.get("alias", ""),
                    "accountValue": round(to_float(wallet.get("accountValue")), 2),
                    "totalNotional": round(to_float(wallet.get("totalNotional")), 2),
                    "unrealizedPnl": round(to_float(wallet.get("unrealizedPnl")), 2),
                    "positionCount": len(positions),
                    "openOrderCount": int(to_float(wallet.get("openOrderCount"))),
                    "fills30d": int(to_float(wallet.get("fills30d"))),
                    "daysSinceLastFill": wallet.get("daysSinceLastFill"),
                    "topPosition": positions[0] if positions else None,
                }
            )
        return sorted(
            holders,
            key=lambda item: (abs(item["totalNotional"]), item["accountValue"]),
            reverse=True,
        )[: max(1, limit)]

    def dashboard(self) -> dict[str, Any]:
        wallets = self.store.list_wallets()
        raw_quality_cache = load_json_file(self.wallet_quality_cache_path, {})
        cached_wallets = (
            raw_quality_cache.get("wallets", {})
            if isinstance(raw_quality_cache, dict) and isinstance(raw_quality_cache.get("wallets"), dict)
            else {}
        )
        refresh_addresses = self.wallet_quality_refresh_addresses(wallets, cached_wallets)

        def fetch_snapshot(wallet: TrackedWallet) -> dict[str, Any]:
            address = wallet.address.lower()
            return self.fetch_wallet_snapshot(
                wallet,
                full_quality_refresh=address in refresh_addresses,
                cached_snapshot=cached_wallets.get(address),
            )

        with ThreadPoolExecutor(max_workers=min(max(len(wallets), 1), HYPERLIQUID_DASHBOARD_WORKERS)) as executor:
            snapshots = list(executor.map(fetch_snapshot, wallets)) if wallets else []
        cache_changed = False
        for snapshot in snapshots:
            address = str(snapshot.get("address") or "").lower()
            quality = snapshot.get("dataQuality", {}) if isinstance(snapshot.get("dataQuality"), dict) else {}
            if address and quality.get("qualityRefreshSucceeded"):
                cached_wallets[address] = self.cached_wallet_quality_snapshot(snapshot)
                cache_changed = True
            elif address in cached_wallets and snapshot.get("recentFills"):
                cached_wallets[address]["recentFills"] = snapshot.get("recentFills", [])
                cache_changed = True
        if cache_changed:
            tracked_addresses = {wallet.address.lower() for wallet in wallets}
            save_json_file(
                self.wallet_quality_cache_path,
                {
                    "version": 1,
                    "updatedAt": now_iso(),
                    "wallets": {
                        address: item
                        for address, item in cached_wallets.items()
                        if address in tracked_addresses and isinstance(item, dict)
                    },
                },
            )
        fill_ok_count = sum(1 for item in snapshots if item.get("dataQuality", {}).get("fillsOk"))
        wallets_with_recent_fills = sum(1 for item in snapshots if item.get("recentFills"))
        total_recent_fills = sum(len(item.get("recentFills", [])) for item in snapshots)
        total_positions = sum(len(item.get("positions", [])) for item in snapshots)
        fills_globally_degraded = (
            len(snapshots) >= 5
            and total_positions > 0
            and total_recent_fills == 0
        )
        if fills_globally_degraded:
            for item in snapshots:
                item["holdingOnly30d"] = False
                item.setdefault("dataQuality", {})["fillsDegraded"] = True
        snapshots.sort(key=lambda item: item["accountValue"], reverse=True)

        totals = {
            "walletsTracked": len(snapshots),
            "combinedAccountValue": round(sum(item["accountValue"] for item in snapshots), 2),
            "combinedNotional": round(sum(item["totalNotional"] for item in snapshots), 2),
            "combinedUnrealizedPnl": round(sum(item["unrealizedPnl"] for item in snapshots), 2),
            "combinedRealizedPnl": round(sum(item["realizedPnl"] for item in snapshots), 2),
            "bullishWallets": sum(1 for item in snapshots if item["exposure"]["net"] > 0),
            "bearishWallets": sum(1 for item in snapshots if item["exposure"]["net"] < 0),
            "moneyPrinterWallets": sum(1 for item in snapshots if item["cohorts"]["profitability"] == "Money Printer"),
            "holdingOnly30dWallets": sum(1 for item in snapshots if item.get("holdingOnly30d")),
            "dataQuality": {
                "fillsOkWallets": fill_ok_count,
                "walletsWithRecentFills": wallets_with_recent_fills,
                "totalRecentFills": total_recent_fills,
                "fillsGloballyDegraded": fills_globally_degraded,
            },
        }

        segment_map: dict[str, dict[str, Any]] = {}
        for item in snapshots:
            label = f'{item["cohorts"]["walletSize"]} / {item["cohorts"]["profitability"]}'
            bucket = segment_map.setdefault(
                label,
                {
                    "label": label,
                    "count": 0,
                    "combinedAccountValue": 0.0,
                    "combinedRealizedPnl": 0.0,
                    "combinedUnrealizedPnl": 0.0,
                    "netExposure": 0.0,
                },
            )
            bucket["count"] += 1
            bucket["combinedAccountValue"] += item["accountValue"]
            bucket["combinedRealizedPnl"] += item["realizedPnl"]
            bucket["combinedUnrealizedPnl"] += item["unrealizedPnl"]
            bucket["netExposure"] += item["exposure"]["net"]

        segments = sorted(
            (
                {
                    **bucket,
                    "combinedAccountValue": round(bucket["combinedAccountValue"], 2),
                    "combinedRealizedPnl": round(bucket["combinedRealizedPnl"], 2),
                    "combinedUnrealizedPnl": round(bucket["combinedUnrealizedPnl"], 2),
                    "netExposure": round(bucket["netExposure"], 2),
                }
                for bucket in segment_map.values()
            ),
            key=lambda bucket: (bucket["count"], bucket["combinedAccountValue"]),
            reverse=True,
        )

        sentiment = self.build_sentiment_summary(snapshots, DEFAULT_CONSENSUS_THRESHOLD)
        holding_only_wallets = self.build_holding_only_wallets(snapshots)

        return {
            "generatedAt": now_iso(),
            "markets": self.client.list_markets()[:30],
            "totals": totals,
            "segments": segments,
            "sentiment": sentiment,
            "holdingOnly30dWallets": holding_only_wallets,
            "wallets": snapshots,
            "savedWallets": [wallet.to_dict() for wallet in wallets],
        }

    def import_wallets(self, raw_text: str) -> dict[str, Any]:
        entries, invalid = parse_import_lines(raw_text)
        added = 0
        updated = 0
        saved_wallets = []
        for entry in entries:
            wallet, is_new = self.store.upsert_wallet(entry["address"], entry["alias"], entry["notes"])
            saved_wallets.append(wallet.to_dict())
            if is_new:
                added += 1
            else:
                updated += 1
        return {
            "added": added,
            "updated": updated,
            "invalid": invalid,
            "wallets": saved_wallets,
        }

    def scan_discovery_candidates(
        self,
        addresses: list[str],
        limit: int = 15,
        min_account_value: float = 0.0,
        min_realized_pnl: float = 0.0,
    ) -> dict[str, Any]:
        seen = set()
        candidates = []
        tracked = {wallet.address.lower() for wallet in self.store.list_wallets()}
        cleaned = []
        for address in addresses:
            normalized = normalize_address(address)
            if not normalized:
                continue
            lowered = normalized.lower()
            if lowered in seen or lowered in tracked:
                continue
            seen.add(lowered)
            cleaned.append(normalized)

        cleaned = cleaned[:MAX_DISCOVERY_BATCH]
        synthetic_wallets = [
            TrackedWallet(address=address, alias="", notes="Discovered from public trade flow", created_at=now_iso())
            for address in cleaned
        ]
        with ThreadPoolExecutor(max_workers=min(max(len(synthetic_wallets), 1), 8)) as executor:
            snapshots = list(executor.map(self.fetch_wallet_snapshot, synthetic_wallets)) if synthetic_wallets else []

        for snapshot in snapshots:
            if snapshot["accountValue"] < min_account_value:
                continue
            if snapshot["realizedPnl"] < min_realized_pnl:
                continue
            if snapshot["accountValue"] == 0 and snapshot["totalNotional"] == 0 and snapshot["realizedPnl"] == 0:
                continue
            candidates.append(snapshot)

        candidates.sort(
            key=lambda item: (item["discoveryScore"], item["accountValue"], item["realizedPnl"]),
            reverse=True,
        )

        return {
            "generatedAt": now_iso(),
            "scanned": len(cleaned),
            "candidates": candidates[: max(1, min(limit, 25))],
        }

    def get_alert_settings(self) -> dict[str, Any]:
        raw = load_json_file(self.alerts_path, {})
        config = self.resolve_alert_config(raw.get("config", {}) if isinstance(raw, dict) else {})
        state = raw.get("state", {}) if isinstance(raw, dict) else {}
        return {
            "enabled": bool(config.get("enabled")),
            "chatId": config.get("chatId", ""),
            "hasBotToken": bool(config.get("botToken")),
            "minConsensusWallets": int(config.get("minConsensusWallets", DEFAULT_CONSENSUS_THRESHOLD)),
            "trackHip3": bool(config.get("trackHip3", True)),
            "lastCheckedAt": state.get("lastCheckedAt"),
            "lastSentAt": state.get("lastSentAt"),
            "lastSummary": state.get("summary", {}),
            "topConvictionWallets": state.get("topConvictionWallets", {}),
        }

    def resolve_alert_config(self, stored_config: dict[str, Any]) -> dict[str, Any]:
        config = {
            "enabled": bool(stored_config.get("enabled")),
            "botToken": str(stored_config.get("botToken", "")).strip(),
            "chatId": str(stored_config.get("chatId", "")).strip(),
            "minConsensusWallets": max(
                1, int(stored_config.get("minConsensusWallets", DEFAULT_CONSENSUS_THRESHOLD))
            ),
            "trackHip3": bool(stored_config.get("trackHip3", True)),
        }

        if "ALERTS_ENABLED" in os.environ:
            config["enabled"] = env_flag("ALERTS_ENABLED", config["enabled"])
        if "TELEGRAM_BOT_TOKEN" in os.environ:
            config["botToken"] = os.environ.get("TELEGRAM_BOT_TOKEN", "").strip()
        if "TELEGRAM_CHAT_ID" in os.environ:
            config["chatId"] = os.environ.get("TELEGRAM_CHAT_ID", "").strip()
        if "MIN_CONSENSUS_WALLETS" in os.environ:
            config["minConsensusWallets"] = max(
                1, int(os.environ.get("MIN_CONSENSUS_WALLETS", str(config["minConsensusWallets"])))
            )
        if "TRACK_HIP3" in os.environ:
            config["trackHip3"] = env_flag("TRACK_HIP3", config["trackHip3"])

        return config

    def update_alert_settings(self, payload: dict[str, Any]) -> dict[str, Any]:
        raw = load_json_file(self.alerts_path, {})
        config = raw.get("config", {}) if isinstance(raw, dict) else {}
        state = raw.get("state", {}) if isinstance(raw, dict) else {}

        if "enabled" in payload:
            config["enabled"] = bool(payload.get("enabled"))
        if "botToken" in payload:
            config["botToken"] = str(payload.get("botToken", "")).strip()
        if "chatId" in payload:
            config["chatId"] = str(payload.get("chatId", "")).strip()
        if "minConsensusWallets" in payload:
            config["minConsensusWallets"] = max(1, int(payload.get("minConsensusWallets", DEFAULT_CONSENSUS_THRESHOLD)))
        if "trackHip3" in payload:
            config["trackHip3"] = bool(payload.get("trackHip3"))

        save_json_file(self.alerts_path, {"config": config, "state": state})
        return self.get_alert_settings()

    def top_conviction_wallet_addresses(self, wallets: list[dict[str, Any]], *, limit: int = TOP_CONVICTION_WALLET_COUNT) -> set[str]:
        ranked = self.rank_top_conviction_wallets(wallets)
        return {
            str(wallet.get("address") or "").lower()
            for wallet in ranked[:limit]
            if str(wallet.get("address") or "").strip()
        }

    def rank_top_conviction_wallets(self, wallets: list[dict[str, Any]]) -> list[dict[str, Any]]:
        return sorted(
            wallets,
            key=lambda wallet: (
                to_float(
                    wallet.get("recentWinRateRank", {}).get(
                        "convictionWeightScore",
                        wallet.get("recentWinRateRank", {}).get("score"),
                    )
                )
                if isinstance(wallet.get("recentWinRateRank"), dict)
                else 0.0,
                to_float(wallet.get("realizedPnl30d")),
                to_float(wallet.get("recentRealizedPnl")),
                to_float(wallet.get("unrealizedPnl")),
            ),
            reverse=True,
        )

    def is_toxic_conviction_wallet(self, wallet: dict[str, Any]) -> bool:
        return (
            to_float(wallet.get("realizedPnl30d")) < TOXIC_CONVICTION_WALLET_MAX_30D_PNL
            or to_float(wallet.get("unrealizedPnl")) < COUNTED_POSITION_MAX_UNREALIZED_LOSS
        )

    def is_monthly_quality_eligible(self, wallet: dict[str, Any]) -> bool:
        if "qualityClosedEvents30d" not in wallet:
            return not self.is_toxic_conviction_wallet(wallet)
        profit_factor_raw = wallet.get("qualityProfitFactor30d")
        profit_factor = float("inf") if profit_factor_raw == "inf" else to_float(profit_factor_raw)
        holdout_events = int(to_float(wallet.get("qualityHoldout6dEvents")))
        return (
            int(to_float(wallet.get("qualityClosedEvents30d"))) >= MONTHLY_QUALITY_MIN_CLOSED_EVENTS
            and to_float(wallet.get("qualityNetPnl30d")) > 0
            and profit_factor > MONTHLY_QUALITY_MIN_PROFIT_FACTOR
            and to_float(wallet.get("qualityTopWinConcentrationPct", 100.0))
            < MONTHLY_QUALITY_MAX_WIN_CONCENTRATION_PCT
            and (holdout_events == 0 or to_float(wallet.get("qualityHoldout6dNetPnl")) > 0)
            and not self.is_toxic_conviction_wallet(wallet)
        )

    def current_top_conviction_month(self) -> str:
        return datetime.fromtimestamp(current_time_ms() / 1000, timezone.utc).strftime("%Y-%m")

    def resolve_monthly_top_conviction_cohort(
        self,
        wallets: list[dict[str, Any]],
        state: dict[str, Any],
        *,
        month_key: str | None = None,
        limit: int = TOP_CONVICTION_WALLET_COUNT,
    ) -> tuple[set[str], dict[str, Any]]:
        active_month = month_key or self.current_top_conviction_month()
        wallet_by_address = {
            str(wallet.get("address") or "").lower(): wallet
            for wallet in wallets
            if str(wallet.get("address") or "").strip()
        }
        stored = state.get("topConvictionWallets", {}) if isinstance(state, dict) else {}
        stored_addresses = [
            str(address or "").lower()
            for address in (stored.get("addresses", []) if isinstance(stored, dict) else [])
            if str(address or "").strip()
        ]
        use_stored = isinstance(stored, dict) and stored.get("month") == active_month and stored_addresses

        ineligible_addresses = {
            address
            for address, wallet in wallet_by_address.items()
            if not self.is_monthly_quality_eligible(wallet)
        }
        selected: list[str] = []
        demoted: list[str] = []
        if use_stored:
            for address in stored_addresses:
                if address not in wallet_by_address or address in ineligible_addresses:
                    demoted.append(address)
                    continue
                if address not in selected:
                    selected.append(address)

        ranked = self.rank_top_conviction_wallets(wallets)
        for wallet in ranked:
            address = str(wallet.get("address") or "").lower()
            if not address or address in selected or address in ineligible_addresses:
                continue
            if not use_stored or len(selected) < limit:
                selected.append(address)
            if len(selected) >= limit:
                break

        selected = selected[:limit]
        cohort = {
            "month": active_month,
            "addresses": selected,
            "updatedAt": now_iso(),
            "demoted": demoted,
        }
        return set(selected), cohort

    def build_monthly_sentiment_summary(
        self,
        dashboard: dict[str, Any],
        min_wallets: int,
        state: dict[str, Any],
        *,
        persist: bool = False,
        stored_config: dict[str, Any] | None = None,
        month_key: str | None = None,
        position_lifecycle: dict[str, Any] | None = None,
    ) -> tuple[dict[str, Any], dict[str, Any]]:
        top_wallet_addresses, cohort = self.resolve_monthly_top_conviction_cohort(
            dashboard.get("wallets", []),
            state,
            month_key=month_key,
        )
        summary = self.build_sentiment_summary(
            dashboard.get("wallets", []),
            min_wallets,
            top_wallet_addresses=top_wallet_addresses,
            position_lifecycle=position_lifecycle or state.get("walletPositionLifecycle", {}),
        )
        summary["topConvictionWallets"] = cohort
        if persist and isinstance(state, dict) and state.get("topConvictionWallets") != cohort:
            save_json_file(
                self.alerts_path,
                {"config": stored_config or {}, "state": {**state, "topConvictionWallets": cohort}},
            )
        return summary, cohort

    def wallet_conviction_weight(
        self,
        wallet: dict[str, Any],
        top_wallet_addresses: set[str] | None = None,
        *,
        coin: str | None = None,
    ) -> float:
        rank = wallet.get("recentWinRateRank")
        if not isinstance(rank, dict):
            base_weight = 1.0
        else:
            score = to_float(rank.get("convictionWeightScore", rank.get("score")))
            label = str(rank.get("label") or "")
            if score <= 0 or label == "Unranked":
                base_weight = 1.0
            else:
                base_weight = round(max(0.5, min(score / ELITE_MIN_QUALITY_SCORE, 1.5)), 3)
        address = str(wallet.get("address") or "").lower()
        if address in BACKTEST_ELITE_WALLETS:
            base_weight = 1.5
        elif address in BACKTEST_STANDARD_WALLETS:
            base_weight = 1.0
        elif address in BACKTEST_REVIEW_WALLETS:
            base_weight = 0.5
        if not top_wallet_addresses:
            multiplier = 1.0
        else:
            multiplier = (
                TOP_CONVICTION_WALLET_MULTIPLIER
                if address in top_wallet_addresses
                else NON_TOP_CONVICTION_WALLET_MULTIPLIER
            )
        if coin:
            asset_quality = wallet.get("assetQuality", {})
            asset_stats = asset_quality.get(normalize_position_coin(coin), {}) if isinstance(asset_quality, dict) else {}
            closed_trades = int(to_float(asset_stats.get("closedTrades"))) if isinstance(asset_stats, dict) else 0
            if closed_trades >= ASSET_QUALITY_MIN_CLOSED_TRADES:
                asset_win_rate = to_float(asset_stats.get("winRate"))
                multiplier *= max(0.75, min(1.25, asset_win_rate / 60.0))
        if self.is_wallet_quarantined(wallet):
            multiplier *= 0.5
        weight = base_weight * multiplier
        return round(max(CONVICTION_WALLET_WEIGHT_MIN, min(weight, CONVICTION_WALLET_WEIGHT_MAX)), 3)

    def is_wallet_quarantined(self, wallet: dict[str, Any]) -> bool:
        rank = wallet.get("recentWinRateRank", {})
        if not isinstance(rank, dict):
            return False
        return (
            int(to_float(rank.get("sampleSize"))) >= RANKING_MIN_7D_CLOSED_TRADES
            and to_float(rank.get("pnlReturnPct")) <= WALLET_QUARANTINE_MIN_7D_RETURN_PCT
            and to_float(rank.get("winRate")) <= WALLET_QUARANTINE_MAX_7D_WIN_RATE
        )

    def wallet_fill_data_reliable(self, wallet: dict[str, Any]) -> bool:
        quality = wallet.get("dataQuality")
        if not isinstance(quality, dict):
            return True
        return bool(quality.get("fillsOk", True)) and not bool(quality.get("fillsDegraded"))

    def should_count_wallet_for_conviction(self, wallet: dict[str, Any]) -> bool:
        if not self.wallet_fill_data_reliable(wallet):
            return True
        if wallet.get("recentFills"):
            return True
        has_positions = bool(wallet.get("positions"))
        no_activity = int(to_float(wallet.get("fills30d"))) == 0 and int(to_float(wallet.get("closedTrades30d"))) == 0
        if has_positions and wallet.get("holdingOnly30d") and no_activity:
            return False
        return True

    def has_recent_position_fill(
        self,
        wallet: dict[str, Any],
        position: dict[str, Any],
        *,
        now_ms: int,
        event: str,
        window_ms: int,
    ) -> bool:
        recent_fills = wallet.get("recentFills", [])
        if not isinstance(recent_fills, list):
            return False
        coin = normalize_position_coin(position.get("coin"))
        side = str(position.get("side") or "").lower()
        if side not in {"long", "short"}:
            return False
        cutoff_ms = now_ms - window_ms
        for fill in recent_fills:
            if not isinstance(fill, dict):
                continue
            classified = self.classify_fill_direction(fill.get("direction"))
            if not classified:
                continue
            fill_side, fill_event = classified
            if fill_event != event or fill_side != side:
                continue
            if normalize_position_coin(fill.get("coin")).upper() != coin.upper():
                continue
            fill_time = int(to_float(fill.get("time")))
            if fill_time < cutoff_ms or fill_time > now_ms:
                continue
            if event == "add":
                fill_notional = to_float(fill.get("price")) * abs(to_float(fill.get("size")))
                position_value = abs(to_float(position.get("positionValue")))
                relative_add_value = position_value * RECENT_ADD_POSITION_MIN_PCT
                if fill_notional < POSITION_INCREASE_ALERT_MIN_DELTA and (
                    relative_add_value <= 0 or fill_notional < relative_add_value
                ):
                    continue
            return True
        return False

    def recent_position_add_metrics(
        self,
        wallet: dict[str, Any],
        position: dict[str, Any],
        *,
        now_ms: int,
        window_ms: int,
    ) -> dict[str, float]:
        coin = normalize_position_coin(position.get("coin"))
        side = str(position.get("side") or "").lower()
        cutoff_ms = now_ms - window_ms
        value = 0.0
        size = 0.0
        latest_time = 0
        for fill in wallet.get("recentFills", []):
            classified = self.classify_fill_direction(fill.get("direction"))
            if not classified or classified != (side, "add"):
                continue
            fill_time = int(to_float(fill.get("time")))
            fill_price = to_float(fill.get("price"))
            fill_size = abs(to_float(fill.get("size")))
            if fill_time < cutoff_ms or fill_time > now_ms or fill_price <= 0 or fill_size <= 0:
                continue
            if normalize_position_coin(fill.get("coin")) != coin:
                continue
            value += fill_price * fill_size
            size += fill_size
            latest_time = max(latest_time, fill_time)
        return {"value": value, "size": size, "latestTime": float(latest_time)}

    def max_signal_entry_distance_pct(self, coin: Any) -> float:
        normalized = normalize_position_coin(coin).upper()
        if normalized in {"BTC", "ETH"} or is_stock_like_position(normalized):
            return WALLET_SIGNAL_MAJOR_ASSET_MAX_ENTRY_DISTANCE_PCT
        if normalized in {"SOL", "HYPE"}:
            return WALLET_SIGNAL_HIGH_BETA_MAX_ENTRY_DISTANCE_PCT
        return WALLET_SIGNAL_MAX_ENTRY_DISTANCE_PCT

    def position_lifecycle_key(self, address: str, coin: str, side: str) -> str:
        return f"{address.lower()}:{normalize_position_coin(coin)}:{side.lower()}"

    def build_position_lifecycle(
        self,
        dashboard: dict[str, Any],
        previous: dict[str, Any] | None,
    ) -> dict[str, dict[str, Any]]:
        prior = previous if isinstance(previous, dict) else {}
        lifecycle: dict[str, dict[str, Any]] = {}
        for wallet in dashboard.get("wallets", []):
            address = str(wallet.get("address") or "")
            for position in wallet.get("positions", []):
                side = str(position.get("side") or "").lower()
                if side not in {"long", "short"}:
                    continue
                coin = normalize_position_coin(position.get("coin"))
                key = self.position_lifecycle_key(address, coin, side)
                previous_item = prior.get(key, {}) if isinstance(prior.get(key), dict) else {}
                opened_at = int(to_float(previous_item.get("openedAt"))) or current_time_ms()
                last_add_at = int(to_float(previous_item.get("lastAddAt")))
                last_add_price = to_float(previous_item.get("lastAddPrice"))
                for fill in wallet.get("recentFills", []):
                    classified = self.classify_fill_direction(fill.get("direction"))
                    if not classified:
                        continue
                    fill_side, event = classified
                    if event != "add" or fill_side != side or normalize_position_coin(fill.get("coin")) != coin:
                        continue
                    fill_time = int(to_float(fill.get("time")))
                    if fill_time >= last_add_at:
                        last_add_at = fill_time
                        last_add_price = to_float(fill.get("price"))
                        opened_at = min(opened_at, fill_time) if previous_item else fill_time
                lifecycle[key] = {
                    "openedAt": opened_at,
                    "lastAddAt": last_add_at,
                    "lastAddPrice": round(last_add_price, 8),
                    "updatedAt": current_time_ms(),
                }
        return lifecycle

    def has_verified_recent_activity(
        self,
        wallet: dict[str, Any],
        position: dict[str, Any],
        lifecycle: dict[str, Any] | None,
        *,
        now_ms: int,
        window_ms: int = WALLET_SIGNAL_ACTIVITY_WINDOW_MS,
    ) -> bool:
        if not self.wallet_fill_data_reliable(wallet):
            return False
        if self.has_recent_position_fill(wallet, position, now_ms=now_ms, event="add", window_ms=window_ms):
            return True
        address = str(wallet.get("address") or "")
        key = self.position_lifecycle_key(address, str(position.get("coin") or ""), str(position.get("side") or ""))
        item = lifecycle.get(key, {}) if isinstance(lifecycle, dict) else {}
        return int(to_float(item.get("lastAddAt"))) >= now_ms - window_ms

    def build_wallet_correlation_groups(self, snapshots: list[dict[str, Any]]) -> dict[str, str]:
        fingerprints: dict[str, set[str]] = {}
        for wallet in snapshots:
            if not self.wallet_fill_data_reliable(wallet):
                continue
            address = str(wallet.get("address") or "").lower()
            events: set[str] = set()
            for fill in wallet.get("recentFills", []):
                classified = self.classify_fill_direction(fill.get("direction"))
                if not classified:
                    continue
                side, event = classified
                fill_time = int(to_float(fill.get("time")))
                if fill_time <= 0:
                    continue
                events.add(
                    f'{normalize_position_coin(fill.get("coin"))}:{side}:{event}:{fill_time // (5 * 60 * 1000)}'
                )
            if events:
                fingerprints[address] = events

        parents = {address: address for address in fingerprints}

        def find(address: str) -> str:
            while parents[address] != address:
                parents[address] = parents[parents[address]]
                address = parents[address]
            return address

        for left, left_events in fingerprints.items():
            for right, right_events in fingerprints.items():
                if left >= right:
                    continue
                shared = len(left_events & right_events)
                union = len(left_events | right_events)
                if shared >= WALLET_CORRELATION_MIN_SHARED_EVENTS and union and shared / union >= WALLET_CORRELATION_MIN_JACCARD:
                    parents[find(right)] = find(left)

        return {address: find(address) for address in fingerprints}

    def signal_probability_score(self, item: dict[str, Any]) -> float:
        conviction = max(0.0, min(to_float(item.get("convictionScore")), 100.0))
        qnet = max(0.0, to_float(item.get("netIndependentWeightedWalletCount")))
        net_wallets = max(0, int(to_float(item.get("netIndependentWalletCount"))))
        wallet_count = max(0, int(to_float(item.get("independentWalletCount"))))
        fresh_activity = max(0, int(to_float(item.get("verifiedFreshIndependentWalletCount"))))
        top_wallets = max(0, int(to_float(item.get("independentTopWalletCount"))))
        opposite_weight = max(0.0, to_float(item.get("oppositeIndependentWeightedWalletCount")))
        side_weight = max(0.0, to_float(item.get("independentWeightedWalletCount")))

        qnet_score = min(1.0, qnet / 4.0)
        net_score = min(1.0, net_wallets / 4.0)
        activity_score = min(1.0, fresh_activity / 3.0)
        quality_score = min(1.0, top_wallets / max(wallet_count, 1))
        opposition_ratio = opposite_weight / max(side_weight + opposite_weight, 1.0)

        probability = (
            conviction * 0.40
            + qnet_score * 20
            + net_score * 15
            + activity_score * 15
            + quality_score * 10
            - opposition_ratio * 15
        )
        return round(max(0.0, min(probability, 99.0)), 1)

    def signal_rejection_reasons(self, item: dict[str, Any], probability: float) -> list[str]:
        reasons: list[str] = []
        if int(to_float(item.get("independentWalletCount"))) < ACTIONABLE_SIGNAL_MIN_INDEPENDENT_WALLETS:
            reasons.append("independent_wallet_count")
        if int(to_float(item.get("netIndependentWalletCount"))) < ACTIONABLE_SIGNAL_MIN_INDEPENDENT_NET_WALLETS:
            reasons.append("weak_net")
        if to_float(item.get("netIndependentWeightedWalletCount")) < ACTIONABLE_SIGNAL_MIN_QNET:
            reasons.append("weak_qnet")
        if int(to_float(item.get("verifiedFreshIndependentWalletCount"))) < ACTIONABLE_SIGNAL_MIN_VERIFIED_FRESH_WALLETS:
            reasons.append("insufficient_verified_activity")
        if int(to_float(item.get("netFreshIndependentWalletCount"))) < ACTIONABLE_SIGNAL_MIN_FRESH_NET_WALLETS:
            reasons.append("weak_fresh_net")
        if int(to_float(item.get("oppositeVerifiedFreshIndependentWalletCount"))) > ACTIONABLE_SIGNAL_MAX_OPPOSITE_FRESH_WALLETS:
            reasons.append("opposite_fresh_flow")
        if int(to_float(item.get("independentTopWalletCount"))) < ACTIONABLE_SIGNAL_MIN_TOP_WALLETS:
            reasons.append("insufficient_top_wallets")
        if to_float(item.get("freshAddVwap")) <= 0 or to_float(item.get("markPrice")) <= 0:
            reasons.append("missing_fresh_vwap")
        elif abs(to_float(item.get("entryDistancePct"))) > to_float(item.get("maxEntryDistancePct")):
            reasons.append("extended_from_fresh_vwap")
        if probability < ACTIONABLE_SIGNAL_PROBABILITY_THRESHOLD:
            reasons.append("low_probability")
        return reasons

    def build_high_conviction_signals(
        self,
        consensus: list[dict[str, Any]],
        *,
        threshold: float = ACTIONABLE_SIGNAL_PROBABILITY_THRESHOLD,
    ) -> list[dict[str, Any]]:
        signals = []
        for item in consensus:
            probability_score = self.signal_probability_score(item)
            rejection_reasons = self.signal_rejection_reasons(item, probability_score)
            if rejection_reasons:
                continue
            conviction_score = to_float(item.get("convictionScore"))
            side = str(item.get("side") or "").lower()
            action = signal_action_from_side(side)
            strength = "extreme" if probability_score >= EXTREME_SIGNAL_PROBABILITY_THRESHOLD else "high"
            signals.append(
                {
                    "coin": item.get("coin", "Unknown"),
                    "side": side,
                    "action": action,
                    "strength": strength,
                    "probabilityScore": probability_score,
                    "walletCount": int(to_float(item.get("walletCount"))),
                    "independentWalletCount": int(to_float(item.get("independentWalletCount"))),
                    "oppositeWalletCount": int(to_float(item.get("oppositeWalletCount"))),
                    "netWalletCount": int(to_float(item.get("netWalletCount"))),
                    "netIndependentWalletCount": int(to_float(item.get("netIndependentWalletCount"))),
                    "weightedWalletCount": round(to_float(item.get("weightedWalletCount")), 3),
                    "netWeightedWalletCount": round(to_float(item.get("netWeightedWalletCount")), 3),
                    "netIndependentWeightedWalletCount": round(
                        to_float(item.get("netIndependentWeightedWalletCount")), 3
                    ),
                    "recentAddWalletCount": int(to_float(item.get("recentAddWalletCount"))),
                    "freshActivityWalletCount": int(to_float(item.get("freshActivityWalletCount"))),
                    "verifiedFreshIndependentWalletCount": int(
                        to_float(item.get("verifiedFreshIndependentWalletCount"))
                    ),
                    "oppositeVerifiedFreshIndependentWalletCount": int(
                        to_float(item.get("oppositeVerifiedFreshIndependentWalletCount"))
                    ),
                    "netFreshIndependentWalletCount": int(to_float(item.get("netFreshIndependentWalletCount"))),
                    "freshWalletAddresses": list(item.get("freshWalletAddresses", [])),
                    "fillQualityUnknownWalletCount": int(to_float(item.get("fillQualityUnknownWalletCount"))),
                    "topWalletCount": int(to_float(item.get("topWalletCount"))),
                    "independentTopWalletCount": int(to_float(item.get("independentTopWalletCount"))),
                    "totalValue": round(to_float(item.get("totalValue")), 2),
                    "freshAddVwap": round(to_float(item.get("freshAddVwap")), 8),
                    "markPrice": round(to_float(item.get("markPrice")), 8),
                    "entryDistancePct": round(to_float(item.get("entryDistancePct")), 3),
                    "freshAddLatestTime": int(to_float(item.get("freshAddLatestTime"))),
                    "convictionScore": round(conviction_score, 1),
                    "threshold": round(to_float(threshold), 1),
                    "wallets": item.get("wallets", [])[:5],
                    "rationale": (
                        f'{int(to_float(item.get("independentWalletCount")))} independent wallets are {side} '
                        f'against {int(to_float(item.get("oppositeIndependentWalletCount")))} opposite wallets, '
                        f'net +{int(to_float(item.get("netIndependentWalletCount")))}, '
                        f'qnet +{to_float(item.get("netIndependentWeightedWalletCount")):.1f}.'
                    ),
                }
            )
        return sorted(
            signals,
            key=lambda item: (
                -item["probabilityScore"],
                -item["convictionScore"],
                -item["netWeightedWalletCount"],
                -item["netWalletCount"],
                -item["walletCount"],
                item["coin"],
                item["side"],
            ),
        )

    def is_active_for_conviction(
        self,
        wallet: dict[str, Any],
        position: dict[str, Any],
        lifecycle: dict[str, Any] | None,
        *,
        now_ms: int,
    ) -> bool:
        if not self.wallet_fill_data_reliable(wallet):
            return True
        if not wallet.get("holdingOnly30d"):
            return True
        return self.has_verified_recent_activity(wallet, position, lifecycle, now_ms=now_ms, window_ms=RANKING_WINDOW_MS)

    def build_sentiment_summary(
        self,
        snapshots: list[dict[str, Any]],
        min_wallets: int,
        *,
        top_wallet_addresses: set[str] | None = None,
        position_lifecycle: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        aggregate: dict[tuple[str, str], dict[str, Any]] = {}
        total_long = 0.0
        total_short = 0.0
        long_wallets: set[str] = set()
        short_wallets: set[str] = set()
        now_ms = current_time_ms()
        active_top_wallet_addresses = (
            top_wallet_addresses if top_wallet_addresses is not None else self.top_conviction_wallet_addresses(snapshots)
        )
        correlation_groups = self.build_wallet_correlation_groups(snapshots)

        for snapshot in snapshots:
            if not self.should_count_wallet_for_conviction(snapshot):
                continue
            address = str(snapshot.get("address") or "")
            for position in snapshot.get("positions", []):
                coin = normalize_position_coin(position.get("coin"))
                if not should_count_open_position(address, coin, position):
                    continue
                if not self.is_active_for_conviction(snapshot, position, position_lifecycle, now_ms=now_ms):
                    continue
                side = str(position.get("side") or "Flat").lower()
                position_value = to_float(position.get("positionValue"))
                if side not in {"long", "short"}:
                    continue
                wallet_weight = self.wallet_conviction_weight(snapshot, active_top_wallet_addresses, coin=coin)
                correlation_group = correlation_groups.get(address.lower(), address.lower())

                key = (coin, side)
                bucket = aggregate.setdefault(
                    key,
                    {
                        "coin": coin,
                        "side": side,
                        "walletCount": 0,
                        "totalValue": 0.0,
                        "totalSize": 0.0,
                        "weightedWalletCount": 0.0,
                        "wallets": [],
                        "walletAddresses": set(),
                        "walletGroups": set(),
                        "walletGroupWeights": {},
                        "recentAddWalletAddresses": set(),
                        "freshActivityWalletAddresses": set(),
                        "verifiedFreshWalletAddresses": set(),
                        "verifiedFreshWalletGroups": set(),
                        "freshAddValue": 0.0,
                        "freshAddSize": 0.0,
                        "freshAddLatestTime": 0,
                        "topWalletAddresses": set(),
                        "topWalletGroups": set(),
                        "fillQualityUnknownWalletAddresses": set(),
                        "quarantinedWalletAddresses": set(),
                    },
                )
                bucket["totalValue"] += position_value
                bucket["totalSize"] += abs(to_float(position.get("size")))
                if address and address not in bucket["walletAddresses"]:
                    bucket["walletAddresses"].add(address)
                    bucket["walletCount"] += 1
                    bucket["weightedWalletCount"] += wallet_weight
                    bucket["walletGroups"].add(correlation_group)
                    bucket["walletGroupWeights"][correlation_group] = max(
                        to_float(bucket["walletGroupWeights"].get(correlation_group)), wallet_weight
                    )
                    if address.lower() in active_top_wallet_addresses:
                        bucket["topWalletAddresses"].add(address)
                        bucket["topWalletGroups"].add(correlation_group)
                    if not self.wallet_fill_data_reliable(snapshot):
                        bucket["fillQualityUnknownWalletAddresses"].add(address)
                    if self.is_wallet_quarantined(snapshot):
                        bucket["quarantinedWalletAddresses"].add(address)
                    if self.has_recent_position_fill(
                        snapshot,
                        position,
                        now_ms=now_ms,
                        event="add",
                        window_ms=RANKING_WINDOW_MS,
                    ):
                        bucket["recentAddWalletAddresses"].add(address)
                    if self.has_recent_position_fill(
                        snapshot,
                        position,
                        now_ms=now_ms,
                        event="add",
                        window_ms=CLUSTERED_OPEN_ALERT_WINDOW_MS,
                    ):
                        bucket["freshActivityWalletAddresses"].add(address)
                    fresh_add = self.recent_position_add_metrics(
                        snapshot,
                        position,
                        now_ms=now_ms,
                        window_ms=WALLET_SIGNAL_ACTIVITY_WINDOW_MS,
                    )
                    if (
                        self.wallet_fill_data_reliable(snapshot)
                        and to_float(fresh_add.get("value")) >= FRESH_WALLET_FLOW_MIN_VALUE
                    ):
                        bucket["verifiedFreshWalletAddresses"].add(address)
                        bucket["verifiedFreshWalletGroups"].add(correlation_group)
                        if to_float(fresh_add.get("size")) > 0:
                            bucket["freshAddValue"] += to_float(fresh_add.get("value"))
                            bucket["freshAddSize"] += to_float(fresh_add.get("size"))
                            bucket["freshAddLatestTime"] = max(
                                int(bucket["freshAddLatestTime"]), int(to_float(fresh_add.get("latestTime")))
                            )
                    bucket["wallets"].append(
                        {
                            "address": address,
                            "alias": snapshot.get("alias", ""),
                            "value": round(position_value, 2),
                            "qualityWeight": wallet_weight,
                        }
                    )

                if side == "long":
                    total_long += position_value
                    if address:
                        long_wallets.add(address)
                else:
                    total_short += position_value
                    if address:
                        short_wallets.add(address)

        consensus = [
            {
                "coin": bucket["coin"],
                "side": bucket["side"],
                "walletCount": bucket["walletCount"],
                "independentWalletCount": len(bucket["walletGroups"]),
                "weightedWalletCount": round(bucket["weightedWalletCount"], 3),
                "independentWeightedWalletCount": round(sum(bucket["walletGroupWeights"].values()), 3),
                "recentAddWalletCount": len(bucket["recentAddWalletAddresses"]),
                "freshActivityWalletCount": len(bucket["freshActivityWalletAddresses"]),
                "verifiedFreshWalletCount": len(bucket["verifiedFreshWalletAddresses"]),
                "verifiedFreshIndependentWalletCount": len(bucket["verifiedFreshWalletGroups"]),
                "freshWalletAddresses": sorted(bucket["verifiedFreshWalletAddresses"]),
                "topWalletCount": len(bucket["topWalletAddresses"]),
                "independentTopWalletCount": len(bucket["topWalletGroups"]),
                "fillQualityUnknownWalletCount": len(bucket["fillQualityUnknownWalletAddresses"]),
                "quarantinedWalletCount": len(bucket["quarantinedWalletAddresses"]),
                "totalValue": round(bucket["totalValue"], 2),
                "markPrice": round(bucket["totalValue"] / bucket["totalSize"], 8)
                if bucket["totalSize"] > 0
                else 0.0,
                "freshAddVwap": round(bucket["freshAddValue"] / bucket["freshAddSize"], 8)
                if bucket["freshAddSize"] > 0
                else 0.0,
                "freshAddLatestTime": int(bucket["freshAddLatestTime"]),
                "wallets": sorted(bucket["wallets"], key=lambda item: item["value"], reverse=True),
            }
            for bucket in aggregate.values()
            if bucket["walletCount"] >= min_wallets
        ]
        coin_side_counts: dict[str, dict[str, float]] = {}
        coin_side_raw_counts: dict[str, dict[str, int]] = {}
        coin_side_independent_counts: dict[str, dict[str, int]] = {}
        coin_side_independent_weights: dict[str, dict[str, float]] = {}
        coin_side_fresh_independent_counts: dict[str, dict[str, int]] = {}
        for bucket in aggregate.values():
            side_counts = coin_side_counts.setdefault(str(bucket["coin"]), {"long": 0.0, "short": 0.0})
            side_counts[str(bucket["side"])] = to_float(bucket["weightedWalletCount"])
            raw_counts = coin_side_raw_counts.setdefault(str(bucket["coin"]), {"long": 0, "short": 0})
            raw_counts[str(bucket["side"])] = int(to_float(bucket["walletCount"]))
            independent_counts = coin_side_independent_counts.setdefault(str(bucket["coin"]), {"long": 0, "short": 0})
            independent_counts[str(bucket["side"])] = len(bucket["walletGroups"])
            independent_weights = coin_side_independent_weights.setdefault(str(bucket["coin"]), {"long": 0.0, "short": 0.0})
            independent_weights[str(bucket["side"])] = sum(bucket["walletGroupWeights"].values())
            fresh_independent_counts = coin_side_fresh_independent_counts.setdefault(
                str(bucket["coin"]), {"long": 0, "short": 0}
            )
            fresh_independent_counts[str(bucket["side"])] = len(bucket["verifiedFreshWalletGroups"])
        max_net_weighted_wallet_count = 0.0
        for item in consensus:
            side = str(item["side"])
            side_counts = coin_side_counts.get(str(item["coin"]), {})
            raw_counts = coin_side_raw_counts.get(str(item["coin"]), {})
            independent_counts = coin_side_independent_counts.get(str(item["coin"]), {})
            independent_weights = coin_side_independent_weights.get(str(item["coin"]), {})
            fresh_independent_counts = coin_side_fresh_independent_counts.get(str(item["coin"]), {})
            opposite_side = "short" if side == "long" else "long"
            side_wallet_count = int(to_float(item["walletCount"]))
            opposite_wallet_count = int(to_float(raw_counts.get(opposite_side)))
            net_wallet_count = max(0, side_wallet_count - opposite_wallet_count)
            side_weighted_wallet_count = to_float(item["weightedWalletCount"])
            opposite_weighted_wallet_count = to_float(side_counts.get(opposite_side))
            net_weighted_wallet_count = max(0.0, side_weighted_wallet_count - opposite_weighted_wallet_count)
            side_independent_wallet_count = int(to_float(item["independentWalletCount"]))
            opposite_independent_wallet_count = int(to_float(independent_counts.get(opposite_side)))
            net_independent_wallet_count = max(0, side_independent_wallet_count - opposite_independent_wallet_count)
            side_independent_weight = to_float(item["independentWeightedWalletCount"])
            opposite_independent_weight = to_float(independent_weights.get(opposite_side))
            net_independent_weight = max(0.0, side_independent_weight - opposite_independent_weight)
            side_fresh_independent_count = int(to_float(item["verifiedFreshIndependentWalletCount"]))
            opposite_fresh_independent_count = int(to_float(fresh_independent_counts.get(opposite_side)))
            net_fresh_independent_count = max(0, side_fresh_independent_count - opposite_fresh_independent_count)
            item["oppositeWalletCount"] = opposite_wallet_count
            item["netWalletCount"] = net_wallet_count
            item["oppositeWeightedWalletCount"] = round(opposite_weighted_wallet_count, 3)
            item["netWeightedWalletCount"] = round(net_weighted_wallet_count, 3)
            item["oppositeIndependentWalletCount"] = opposite_independent_wallet_count
            item["netIndependentWalletCount"] = net_independent_wallet_count
            item["oppositeIndependentWeightedWalletCount"] = round(opposite_independent_weight, 3)
            item["netIndependentWeightedWalletCount"] = round(net_independent_weight, 3)
            item["oppositeVerifiedFreshIndependentWalletCount"] = opposite_fresh_independent_count
            item["netFreshIndependentWalletCount"] = net_fresh_independent_count
            item["longWalletCount"] = int(to_float(raw_counts.get("long")))
            item["shortWalletCount"] = int(to_float(raw_counts.get("short")))
            item["longWeightedWalletCount"] = round(to_float(side_counts.get("long")), 3)
            item["shortWeightedWalletCount"] = round(to_float(side_counts.get("short")), 3)
            fresh_add_vwap = to_float(item.get("freshAddVwap"))
            mark_price = to_float(item.get("markPrice"))
            item["entryDistancePct"] = round(((mark_price / fresh_add_vwap) - 1.0) * 100.0, 3) if fresh_add_vwap > 0 and mark_price > 0 else 0.0
            item["maxEntryDistancePct"] = self.max_signal_entry_distance_pct(item.get("coin"))
            max_net_weighted_wallet_count = max(max_net_weighted_wallet_count, net_independent_weight)
        for item in consensus:
            net_score = (
                to_float(item["netIndependentWeightedWalletCount"]) / max_net_weighted_wallet_count
                if max_net_weighted_wallet_count
                else 0.0
            )
            item["convictionScore"] = round(net_score * 100, 1)
        consensus = sorted(
            consensus,
            key=lambda item: (
                -item["netWeightedWalletCount"],
                -item["netWalletCount"],
                -item["walletCount"],
                str(item.get("coin", "")).startswith("@"),
                item["coin"],
                item["side"],
            ),
        )
        hip3_consensus = [item for item in consensus if str(item.get("coin", "")).startswith("@")]
        signals = self.build_high_conviction_signals(consensus)

        overall_bias = "mixed"
        long_wallet_count = len(long_wallets)
        short_wallet_count = len(short_wallets)
        if long_wallet_count > short_wallet_count * 1.2:
            overall_bias = "bullish"
        elif short_wallet_count > long_wallet_count * 1.2:
            overall_bias = "bearish"

        return {
            "generatedAt": now_iso(),
            "overallBias": overall_bias,
            "consensus": consensus,
            "hip3Consensus": hip3_consensus,
            "signals": signals,
            "signalCount": len(signals),
            "longExposure": round(total_long, 2),
            "shortExposure": round(total_short, 2),
            "longWalletCount": long_wallet_count,
            "shortWalletCount": short_wallet_count,
            "walletCount": len(snapshots),
        }

    def build_large_position_snapshot(
        self,
        dashboard: dict[str, Any],
        *,
        min_value: float = NEW_POSITION_ALERT_MIN_VALUE,
    ) -> dict[str, dict[str, Any]]:
        positions: dict[str, dict[str, Any]] = {}
        for wallet in dashboard.get("wallets", []):
            address = str(wallet.get("address") or "")
            alias = str(wallet.get("alias") or "")
            for position in wallet.get("positions", []):
                side = str(position.get("side") or "Flat").lower()
                if side not in {"long", "short"}:
                    continue
                position_value = to_float(position.get("positionValue"))
                coin = normalize_position_coin(position.get("coin"))
                if not should_count_open_position(address, coin, position):
                    continue
                key = f"{address}:{coin}:{side}"
                bucket = positions.setdefault(
                    key,
                    {
                        "address": address,
                        "alias": alias,
                        "coin": coin,
                        "side": side,
                        "totalValue": 0.0,
                        "totalSize": 0.0,
                        "entryValue": 0.0,
                    },
                )
                size = abs(to_float(position.get("size")))
                bucket["totalValue"] += position_value
                bucket["totalSize"] += size
                bucket["entryValue"] += to_float(position.get("entryPx")) * size
        return {
            key: {
                **item,
                "totalValue": round(item["totalValue"], 2),
                "totalSize": round(item["totalSize"], 8),
                "entryPx": round(item["entryValue"] / item["totalSize"], 8) if item["totalSize"] > 0 else 0.0,
            }
            for key, item in positions.items()
            if item["totalValue"] >= min_value
        }

    def fill_price_key(self, address: str, coin: str, side: str, event: str) -> str:
        return f"{address}:{coin}:{side}:{event}"

    def classify_fill_direction(self, direction: Any) -> tuple[str, str] | None:
        normalized = str(direction or "").strip().lower()
        if not normalized:
            return None
        if "long" in normalized:
            side = "long"
        elif "short" in normalized:
            side = "short"
        else:
            return None

        if "close" in normalized:
            return side, "close"
        if "open" in normalized or "increase" in normalized:
            return side, "add"
        return None

    def build_recent_fill_price_map(
        self,
        dashboard: dict[str, Any],
        *,
        since_ms: int = 0,
    ) -> dict[str, dict[str, Any]]:
        buckets: dict[str, dict[str, Any]] = {}
        for wallet in dashboard.get("wallets", []):
            address = str(wallet.get("address") or "")
            if not address:
                continue
            for fill in wallet.get("recentFills", []):
                classified = self.classify_fill_direction(fill.get("direction"))
                if not classified:
                    continue
                fill_time = int(to_float(fill.get("time")))
                if since_ms and fill_time <= since_ms:
                    continue
                side, event = classified
                price = to_float(fill.get("price"))
                size = abs(to_float(fill.get("size")))
                if price <= 0 or size <= 0:
                    continue
                coin = normalize_position_coin(fill.get("coin"))
                key = self.fill_price_key(address, coin, side, event)
                bucket = buckets.setdefault(key, {"priceValue": 0.0, "size": 0.0, "latestTime": 0})
                bucket["priceValue"] += price * size
                bucket["size"] += size
                bucket["latestTime"] = max(int(bucket["latestTime"]), fill_time)

        return {
            key: {
                "price": round(item["priceValue"] / item["size"], 8) if item["size"] > 0 else 0.0,
                "size": round(item["size"], 8),
                "notional": round(item["priceValue"], 2),
                "latestTime": item["latestTime"],
            }
            for key, item in buckets.items()
            if item["size"] > 0
        }

    def classify_open_fill_side(self, direction: Any) -> str | None:
        normalized = str(direction or "").strip().lower()
        if "open" not in normalized:
            return None
        if "long" in normalized:
            return "long"
        if "short" in normalized:
            return "short"
        return None

    def build_clustered_open_position_alerts(
        self,
        dashboard: dict[str, Any],
        current_positions: dict[str, Any],
        *,
        now_ms: int | None = None,
        window_ms: int = CLUSTERED_OPEN_ALERT_WINDOW_MS,
        min_wallets: int = CLUSTERED_OPEN_ALERT_MIN_WALLETS,
    ) -> list[dict[str, Any]]:
        checked_ms = current_time_ms() if now_ms is None else now_ms
        cutoff_ms = checked_ms - window_ms
        current_map = current_positions if isinstance(current_positions, dict) else {}
        opened_positions: dict[str, dict[str, Any]] = {}

        for wallet in dashboard.get("wallets", []):
            address = str(wallet.get("address") or "")
            if not address:
                continue
            for fill in wallet.get("recentFills", []):
                side = self.classify_open_fill_side(fill.get("direction"))
                if not side:
                    continue
                fill_time = int(to_float(fill.get("time")))
                if fill_time < cutoff_ms or fill_time > checked_ms:
                    continue
                coin = normalize_position_coin(fill.get("coin"))
                if not should_count_position(address, coin):
                    continue
                position_key = f"{address}:{coin}:{side}"
                current_item = current_map.get(position_key)
                if not isinstance(current_item, dict):
                    continue
                if to_float(current_item.get("totalValue")) < FRESH_WALLET_FLOW_MIN_VALUE:
                    continue
                fill_price = to_float(fill.get("price"))
                fill_size = abs(to_float(fill.get("size")))
                if fill_price <= 0 or fill_size <= 0:
                    continue
                previous = opened_positions.setdefault(
                    position_key,
                    {
                        **current_item,
                        "openTime": fill_time,
                        "openFillValue": 0.0,
                        "openFillSize": 0.0,
                    },
                )
                previous["openTime"] = max(int(to_float(previous.get("openTime"))), fill_time)
                previous["openFillValue"] = to_float(previous.get("openFillValue")) + (fill_price * fill_size)
                previous["openFillSize"] = to_float(previous.get("openFillSize")) + fill_size

        groups: dict[str, dict[str, Any]] = {}
        for item in opened_positions.values():
            coin = str(item.get("coin") or "Unknown")
            side = str(item.get("side") or "")
            group = groups.setdefault(
                f"{coin}:{side}",
                {
                    "coin": coin,
                    "side": side,
                    "wallets": [],
                    "walletCount": 0,
                    "totalValue": 0.0,
                    "totalSize": 0.0,
                    "openFillValue": 0.0,
                    "openFillSize": 0.0,
                    "earliestOpenTime": 0,
                    "latestOpenTime": 0,
                    "windowMs": window_ms,
                },
            )
            total_value = to_float(item.get("totalValue"))
            total_size = to_float(item.get("totalSize"))
            open_time = int(to_float(item.get("openTime")))
            group["wallets"].append(item)
            group["walletCount"] += 1
            group["totalValue"] += total_value
            group["totalSize"] += total_size
            group["openFillValue"] += to_float(item.get("openFillValue"))
            group["openFillSize"] += to_float(item.get("openFillSize"))
            group["earliestOpenTime"] = (
                open_time
                if not group["earliestOpenTime"]
                else min(int(group["earliestOpenTime"]), open_time)
            )
            group["latestOpenTime"] = max(int(group["latestOpenTime"]), open_time)

        alerts = []
        for group in groups.values():
            if int(group["walletCount"]) < min_wallets:
                continue
            total_size = to_float(group.get("totalSize"))
            open_fill_size = to_float(group.get("openFillSize"))
            wallets = sorted(group["wallets"], key=lambda item: to_float(item.get("totalValue")), reverse=True)
            alerts.append(
                {
                    **group,
                    "wallets": wallets,
                    "totalValue": round(to_float(group.get("totalValue")), 2),
                    "totalSize": round(total_size, 8),
                    "entryPx": round(to_float(group.get("openFillValue")) / open_fill_size, 8)
                    if open_fill_size > 0
                    else 0.0,
                    "entryPriceSource": "fill_vwap",
                }
            )

        return sorted(alerts, key=lambda item: (item["walletCount"], item["totalValue"]), reverse=True)

    def summarize_large_position_changes(
        self,
        previous_positions: dict[str, Any],
        current_positions: dict[str, Any],
        fill_prices: dict[str, Any] | None = None,
        *,
        now_ms: int | None = None,
        open_window_ms: int | None = OPEN_POSITION_ALERT_WINDOW_MS,
    ) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]]]:
        previous_map = previous_positions if isinstance(previous_positions, dict) else {}
        current_map = current_positions if isinstance(current_positions, dict) else {}
        fill_price_map = fill_prices if isinstance(fill_prices, dict) else {}
        checked_ms = current_time_ms() if now_ms is None else now_ms
        added = []
        for key in current_map.keys() - previous_map.keys():
            item = dict(current_map[key])
            fill_price = fill_price_map.get(f"{key}:add", {})
            fill_time = int(to_float(fill_price.get("latestTime"))) if isinstance(fill_price, dict) else 0
            if open_window_ms and (fill_time <= 0 or fill_time < checked_ms - open_window_ms or fill_time > checked_ms):
                continue
            open_price = to_float(fill_price.get("price")) if isinstance(fill_price, dict) else 0.0
            if open_price > 0:
                item["entryPx"] = open_price
                item["entryPriceSource"] = "fill"
                item["openFillTime"] = fill_time
            added.append(item)
        closed = []
        for key in previous_map.keys() - current_map.keys():
            item = dict(previous_map[key])
            total_value = to_float(item.get("totalValue"))
            total_size = to_float(item.get("totalSize"))
            fill_price = fill_price_map.get(f"{key}:close", {})
            close_price = to_float(fill_price.get("price")) if isinstance(fill_price, dict) else 0.0
            if close_price > 0:
                item["closePrice"] = round(close_price, 8)
                item["closePriceSource"] = "fill"
            else:
                item["closePrice"] = round(total_value / total_size, 8) if total_size > 0 else 0.0
                item["closePriceSource"] = "snapshot"
            closed.append(item)
        increased = []
        for key in current_map.keys() & previous_map.keys():
            current_item = current_map[key]
            previous_item = previous_map[key]
            previous_value = to_float(previous_item.get("totalValue"))
            current_value = to_float(current_item.get("totalValue"))
            increase_value = current_value - previous_value
            increase_pct = (increase_value / previous_value) if previous_value > 0 else 0.0
            previous_size = to_float(previous_item.get("totalSize"))
            current_size = to_float(current_item.get("totalSize"))
            size_increase = current_size - previous_size
            fill_price = fill_price_map.get(f"{key}:add", {})
            fill_add_price = to_float(fill_price.get("price")) if isinstance(fill_price, dict) else 0.0
            current_price = (current_value / current_size) if current_size > 0 else 0.0
            add_price = fill_add_price if fill_add_price > 0 else current_price
            add_value = (size_increase * add_price) if size_increase > 0 and add_price > 0 else increase_value
            has_size_baseline = previous_size > 0 or current_size > 0
            is_size_add = has_size_baseline and size_increase > 0 and add_value >= POSITION_INCREASE_ALERT_MIN_DELTA
            is_legacy_value_jump = (
                not has_size_baseline
                and increase_value >= POSITION_INCREASE_ALERT_MIN_DELTA
                and increase_pct >= POSITION_INCREASE_ALERT_MIN_PCT
            )
            if not is_size_add and not is_legacy_value_jump:
                continue
            increased.append(
                {
                    **current_item,
                    "previousValue": round(previous_value, 2),
                    "increaseValue": round(increase_value, 2),
                    "increasePct": round(increase_pct, 4),
                    "previousSize": round(previous_size, 8),
                    "sizeIncrease": round(size_increase, 8),
                    "addValue": round(add_value, 2),
                    "addPrice": round(add_price, 8) if size_increase > 0 else 0.0,
                    "addPriceSource": "fill" if fill_add_price > 0 else "snapshot",
                }
            )
        return (
            sorted(added, key=lambda item: item["totalValue"], reverse=True),
            sorted(increased, key=lambda item: item.get("addValue", item["increaseValue"]), reverse=True),
            sorted(closed, key=lambda item: item["totalValue"], reverse=True),
        )

    def build_large_position_alert_changes(
        self,
        previous_positions: dict[str, Any],
        current_positions: dict[str, Any],
        fill_prices: dict[str, Any] | None = None,
        *,
        now_ms: int | None = None,
    ) -> dict[str, Any]:
        previous_positions = self.filter_counted_large_positions(previous_positions)
        current_positions = self.filter_counted_large_positions(current_positions)
        new_large_positions, increased_large_positions, closed_large_positions = self.summarize_large_position_changes(
            previous_positions,
            current_positions,
            fill_prices,
            now_ms=now_ms,
        )
        return {
            "biasChanged": False,
            "addedConsensus": [],
            "removedConsensus": [],
            "changedConsensus": [],
            "hip3Added": [],
            "hip3Removed": [],
            "clusteredOpenPositions": [],
            "newLargePositions": new_large_positions,
            "increasedLargePositions": increased_large_positions,
            "closedLargePositions": closed_large_positions,
            "addedSignals": [],
            "removedSignals": [],
            "changedSignals": [],
        }

    def filter_counted_large_positions(self, positions: dict[str, Any]) -> dict[str, Any]:
        position_map = positions if isinstance(positions, dict) else {}
        return {
            key: item
            for key, item in position_map.items()
            if (
                isinstance(item, dict)
                and should_count_position(item.get("address"), item.get("coin"))
                and to_float(item.get("totalValue")) >= LARGE_POSITION_ALERT_MIN_VALUE
            )
        }

    def filter_positions_to_tracked_wallets(
        self,
        positions: dict[str, Any],
        tracked_addresses: set[str],
    ) -> dict[str, Any]:
        position_map = positions if isinstance(positions, dict) else {}
        if not tracked_addresses:
            return {}
        return {
            key: item
            for key, item in position_map.items()
            if isinstance(item, dict) and str(item.get("address") or "").lower() in tracked_addresses
        }

    def alert_bucket(self, value: Any) -> str:
        numeric = abs(to_float(value))
        if numeric == 0:
            return "0"
        if numeric >= 1_000_000:
            step = 100_000
        elif numeric >= 100_000:
            step = 10_000
        elif numeric >= 1_000:
            step = 100
        elif numeric >= 10:
            step = 1
        elif numeric >= 1:
            step = 0.1
        else:
            step = 0.001
        return str(int(round(numeric / step)))

    def large_position_event_key(self, event: str, item: dict[str, Any]) -> str:
        address = str(item.get("address") or "")
        coin = str(item.get("coin") or "Unknown")
        side = str(item.get("side") or "")
        size_value = item.get("sizeIncrease") if event == "add" else item.get("totalSize")
        notional_value = item.get("addValue") if event == "add" else item.get("totalValue")
        return (
            f"position:{event}:{address}:{coin}:{side}:"
            f"s{self.alert_bucket(size_value)}:v{self.alert_bucket(notional_value)}"
        )

    def clustered_open_event_key(self, item: dict[str, Any]) -> str:
        coin = str(item.get("coin") or "Unknown")
        side = str(item.get("side") or "")
        wallets = item.get("wallets", [])
        addresses = sorted(str(wallet.get("address") or "") for wallet in wallets if isinstance(wallet, dict))
        return f"position:cluster-open:{coin}:{side}:{','.join(addresses)}"

    def consensus_event_key(self, event: str, item: dict[str, Any]) -> str:
        return (
            f'consensus:{event}:{item.get("coin", "Unknown")}:{item.get("side", "")}:'
            f'{item.get("walletCount", item.get("toWalletCount", ""))}'
        )

    def signal_event_key(self, event: str, item: dict[str, Any]) -> str:
        probability_bucket = int(to_float(item.get("probabilityScore")) // 5 * 5)
        fresh_signature = ",".join(
            sorted(str(address).lower() for address in item.get("freshWalletAddresses", []))
        )
        vwap = to_float(item.get("freshAddVwap"))
        return (
            f'signal:{event}:{item.get("coin", "Unknown")}:{item.get("side", "")}:'
            f'p{probability_bucket}:v{vwap:.8g}:{fresh_signature}'
        )

    def cmm_signal_event_key(self, event: str, item: dict[str, Any]) -> str:
        probability_bucket = int(to_float(item.get("probabilityScore")) // 5 * 5)
        return f'cmm-signal:{event}:{item.get("coin", "Unknown")}:{item.get("side", "")}:p{probability_bucket}'

    def collect_alert_event_keys(self, changes: dict[str, Any]) -> list[str]:
        keys: list[str] = []
        for event, field in (("added", "addedSignals"), ("changed", "changedSignals")):
            keys.extend(self.signal_event_key(event, item) for item in changes.get(field, []))
        keys.extend(self.signal_event_key("invalidated", item) for item in changes.get("removedSignals", []))
        for event, field in (("added", "addedCmmSignals"), ("changed", "changedCmmSignals")):
            keys.extend(self.cmm_signal_event_key(event, item) for item in changes.get(field, []))
        keys.extend(self.clustered_open_event_key(item) for item in changes.get("clusteredOpenPositions", []))
        keys.extend(self.large_position_event_key("open", item) for item in changes.get("newLargePositions", []))
        keys.extend(self.large_position_event_key("add", item) for item in changes.get("increasedLargePositions", []))
        keys.extend(self.large_position_event_key("close", item) for item in changes.get("closedLargePositions", []))
        return keys

    def filter_deduped_alert_changes(
        self,
        changes: dict[str, Any],
        dedupe_state: dict[str, Any],
        *,
        now_ms: int,
        cooldown_ms: int = ALERT_DEDUPE_COOLDOWN_MS,
    ) -> tuple[dict[str, Any], list[str]]:
        active_dedupe = dedupe_state if isinstance(dedupe_state, dict) else {}
        filtered = {**changes}
        suppressed: list[str] = []

        def is_suppressed(key: str) -> bool:
            last_sent_ms = int(to_float(active_dedupe.get(key)))
            return last_sent_ms > 0 and now_ms - last_sent_ms < cooldown_ms

        filtered["biasChanged"] = False

        for event, field in (
            ("added", "addedConsensus"),
            ("removed", "removedConsensus"),
            ("changed", "changedConsensus"),
        ):
            kept = []
            for item in changes.get(field, []):
                key = self.consensus_event_key(event, item)
                if is_suppressed(key):
                    suppressed.append(key)
                else:
                    kept.append(item)
            filtered[field] = kept

        for event, field in (
            ("added", "addedSignals"),
            ("changed", "changedSignals"),
            ("invalidated", "removedSignals"),
        ):
            kept = []
            for item in changes.get(field, []):
                key = self.signal_event_key(event, item)
                if is_suppressed(key):
                    suppressed.append(key)
                else:
                    kept.append(item)
            filtered[field] = kept

        for event, field in (("added", "addedCmmSignals"), ("changed", "changedCmmSignals")):
            kept = []
            for item in changes.get(field, []):
                key = self.cmm_signal_event_key(event, item)
                if is_suppressed(key):
                    suppressed.append(key)
                else:
                    kept.append(item)
            filtered[field] = kept

        kept_clustered_opens = []
        for item in changes.get("clusteredOpenPositions", []):
            key = self.clustered_open_event_key(item)
            if is_suppressed(key):
                suppressed.append(key)
            else:
                kept_clustered_opens.append(item)
        filtered["clusteredOpenPositions"] = kept_clustered_opens

        for event, field in (
            ("open", "newLargePositions"),
            ("add", "increasedLargePositions"),
            ("close", "closedLargePositions"),
        ):
            kept = []
            for item in changes.get(field, []):
                key = self.large_position_event_key(event, item)
                if is_suppressed(key):
                    suppressed.append(key)
                else:
                    kept.append(item)
            filtered[field] = kept

        return filtered, suppressed

    def update_alert_dedupe(
        self,
        dedupe_state: dict[str, Any],
        sent_keys: list[str],
        *,
        now_ms: int,
        cooldown_ms: int = ALERT_DEDUPE_COOLDOWN_MS,
    ) -> dict[str, int]:
        active_dedupe = dedupe_state if isinstance(dedupe_state, dict) else {}
        cutoff_ms = now_ms - cooldown_ms
        updated = {
            str(key): int(to_float(value))
            for key, value in active_dedupe.items()
            if int(to_float(value)) >= cutoff_ms
        }
        for key in sent_keys:
            updated[key] = now_ms
        return updated

    def signal_key(self, signal: dict[str, Any]) -> str:
        return f'{signal.get("coin", "Unknown")}:{signal.get("side", "")}'

    def filter_signals_for_alerts(self, signals: list[dict[str, Any]], track_hip3: bool) -> list[dict[str, Any]]:
        if track_hip3:
            return signals
        return [signal for signal in signals if not str(signal.get("coin", "")).startswith("@")]

    def summarize_signal_changes(
        self,
        previous: dict[str, Any],
        current: dict[str, Any],
        track_hip3: bool,
    ) -> dict[str, list[dict[str, Any]]]:
        previous_signals = {
            self.signal_key(item): item
            for item in self.filter_signals_for_alerts(previous.get("signals", []), track_hip3)
        }
        current_signals = {
            self.signal_key(item): item
            for item in self.filter_signals_for_alerts(current.get("signals", []), track_hip3)
        }
        invalidated_signals = {
            self.signal_key(item): item
            for item in current.get("invalidatedSignals", [])
            if isinstance(item, dict)
        }

        added = [current_signals[key] for key in current_signals.keys() - previous_signals.keys()]
        removed = [
            {
                **previous_signals[key],
                **invalidated_signals.get(key, {}),
                "status": "INVALIDATED",
                "invalidationReason": invalidated_signals.get(key, {}).get("invalidationReason", "ended"),
            }
            for key in previous_signals.keys() - current_signals.keys()
        ]
        changed = []
        for key in current_signals.keys() & previous_signals.keys():
            old_item = previous_signals[key]
            new_item = current_signals[key]
            old_score = to_float(old_item.get("probabilityScore", old_item.get("convictionScore")))
            new_score = to_float(new_item.get("probabilityScore", new_item.get("convictionScore")))
            score_delta = new_score - old_score
            old_wallet_count = int(to_float(old_item.get("walletCount")))
            new_wallet_count = int(to_float(new_item.get("walletCount")))
            old_fresh_addresses = {
                str(address).lower() for address in old_item.get("freshWalletAddresses", [])
            }
            new_fresh_addresses = {
                str(address).lower() for address in new_item.get("freshWalletAddresses", [])
            }
            added_fresh_wallets = sorted(new_fresh_addresses - old_fresh_addresses)
            old_vwap = to_float(old_item.get("freshAddVwap"))
            new_vwap = to_float(new_item.get("freshAddVwap"))
            vwap_delta_pct = abs((new_vwap / old_vwap - 1.0) * 100.0) if old_vwap > 0 and new_vwap > 0 else 0.0
            if (
                abs(score_delta) < SIGNAL_CONVICTION_ALERT_MIN_DELTA
                and not added_fresh_wallets
                and vwap_delta_pct < SIGNAL_RE_ALERT_VWAP_DELTA_PCT
            ):
                continue
            changed.append(
                {
                    **new_item,
                    "fromProbabilityScore": round(old_score, 1),
                    "toProbabilityScore": round(new_score, 1),
                    "probabilityDelta": round(score_delta, 1),
                    "fromWalletCount": old_wallet_count,
                    "toWalletCount": new_wallet_count,
                    "addedFreshWallets": added_fresh_wallets,
                    "freshVwapDeltaPct": round(vwap_delta_pct, 2),
                }
            )

        return {
            "addedSignals": sorted(added, key=lambda item: (item.get("probabilityScore", 0), item["walletCount"]), reverse=True),
            "removedSignals": sorted(removed, key=lambda item: (item.get("probabilityScore", 0), item["walletCount"]), reverse=True),
            "changedSignals": sorted(changed, key=lambda item: (abs(item["probabilityDelta"]), item["toWalletCount"]), reverse=True),
        }

    def summarize_changes(self, previous: dict[str, Any], current: dict[str, Any], track_hip3: bool) -> dict[str, Any]:
        previous_consensus = {
            f'{item["coin"]}:{item["side"]}': item for item in previous.get("consensus", [])
        }
        current_consensus = {
            f'{item["coin"]}:{item["side"]}': item for item in current.get("consensus", [])
        }

        added = [current_consensus[key] for key in current_consensus.keys() - previous_consensus.keys()]
        removed = [previous_consensus[key] for key in previous_consensus.keys() - current_consensus.keys()]
        changed = []
        for key in current_consensus.keys() & previous_consensus.keys():
            old_item = previous_consensus[key]
            new_item = current_consensus[key]
            old_wallet_count = int(old_item["walletCount"])
            new_wallet_count = int(new_item["walletCount"])
            wallet_delta = abs(new_wallet_count - old_wallet_count)
            wallet_delta_pct = wallet_delta / old_wallet_count if old_wallet_count > 0 else 0.0
            if (
                old_wallet_count != new_wallet_count
                and wallet_delta >= CONSENSUS_SIZE_ALERT_MIN_DELTA
                and wallet_delta_pct >= CONSENSUS_SIZE_ALERT_MIN_PCT
            ):
                changed.append(
                    {
                        "coin": new_item["coin"],
                        "side": new_item["side"],
                        "fromWalletCount": old_wallet_count,
                        "toWalletCount": new_wallet_count,
                        "convictionScore": new_item.get("convictionScore", 0.0),
                    }
                )

        hip3_added: list[dict[str, Any]] = []
        hip3_removed: list[dict[str, Any]] = []
        if track_hip3:
            previous_hip3 = {
                f'{item["coin"]}:{item["side"]}': item for item in previous.get("hip3Consensus", [])
            }
            current_hip3 = {
                f'{item["coin"]}:{item["side"]}': item for item in current.get("hip3Consensus", [])
            }
            hip3_added = [current_hip3[key] for key in current_hip3.keys() - previous_hip3.keys()]
            hip3_removed = [previous_hip3[key] for key in previous_hip3.keys() - current_hip3.keys()]

        signal_changes = self.summarize_signal_changes(previous, current, track_hip3)

        return {
            "biasChanged": previous.get("overallBias") != current.get("overallBias"),
            "addedConsensus": sorted(added, key=lambda item: (item["walletCount"], item["totalValue"]), reverse=True),
            "removedConsensus": sorted(removed, key=lambda item: (item["walletCount"], item["totalValue"]), reverse=True),
            "changedConsensus": sorted(changed, key=lambda item: (item["toWalletCount"], item["coin"]), reverse=True),
            "hip3Added": hip3_added,
            "hip3Removed": hip3_removed,
            **signal_changes,
        }

    def build_telegram_message(self, changes: dict[str, Any], summary: dict[str, Any], min_wallets: int) -> str:
        lines = [
            f"Wallet signal | Bias: {summary.get('overallBias', 'mixed')} | Min: {min_wallets}",
        ]

        actionable_signals = changes.get("addedSignals", []) + changes.get("changedSignals", [])
        if actionable_signals:
            lines.append("")
            lines.append(f"Actionable signals >={ACTIONABLE_SIGNAL_PROBABILITY_THRESHOLD:.0f}/100")
            for item in actionable_signals[:8]:
                move_note = ""
                if "fromProbabilityScore" in item:
                    move_note = f' ({to_float(item.get("fromProbabilityScore")):.0f}->{to_float(item.get("toProbabilityScore")):.0f})'
                activity_bits = []
                if int(to_float(item.get("freshActivityWalletCount"))):
                    activity_bits.append(f'{int(to_float(item.get("freshActivityWalletCount")))} 5m activity')
                if int(to_float(item.get("recentAddWalletCount"))):
                    activity_bits.append(f'{int(to_float(item.get("recentAddWalletCount")))} adds')
                if int(to_float(item.get("verifiedFreshIndependentWalletCount"))):
                    activity_bits.append(
                        f'{int(to_float(item.get("verifiedFreshIndependentWalletCount")))} verified 15m'
                    )
                activity_note = f', {"/".join(activity_bits)}' if activity_bits else ""
                cmm_note = ""
                if item.get("cmmConfirmation") == "confirmed":
                    cmm_note = (
                        f', CMM p{to_float(item.get("cmmProbabilityScore")):.0f}'
                        f'/trend {to_float(item.get("cmmTrendScore")):.0f}'
                    )
                fresh_note = ""
                if "netFreshIndependentWalletCount" in item:
                    fresh_note = (
                        f', fresh +{int(to_float(item.get("netFreshIndependentWalletCount")))}'
                        f'/-{int(to_float(item.get("oppositeVerifiedFreshIndependentWalletCount")))}'
                    )
                price_note = ""
                if to_float(item.get("freshAddVwap")) > 0:
                    price_note = (
                        f', VWAP ${format_price(to_float(item.get("freshAddVwap")))}'
                        f' live ~${format_price(to_float(item.get("markPrice")))}'
                        f' dist {to_float(item.get("entryDistancePct")):+.2f}%'
                    )
                lines.append(
                    f'- {str(item.get("status") or item.get("action", "watch")).upper()} '
                    f'{item["coin"]} {item["side"]}: '
                    f'p{to_float(item.get("probabilityScore")):.0f}{move_note}, '
                    f'{item["walletCount"]}w/{int(to_float(item.get("independentWalletCount", item["walletCount"])))}i '
                    f'net +{int(to_float(item.get("netIndependentWalletCount", item.get("netWalletCount"))))}, '
                    f'qnet +{to_float(item.get("netIndependentWeightedWalletCount", item.get("netWeightedWalletCount"))):.1f}, '
                    f'{format_money_compact(item.get("totalValue"))}{fresh_note}'
                    f'{activity_note}{price_note}{cmm_note}'
                )

        if changes.get("removedSignals"):
            lines.append("")
            lines.append("Invalidated signals")
            for item in changes["removedSignals"][:8]:
                lines.append(
                    f'- INVALIDATED {item.get("coin", "Unknown")} {item.get("side", "")}: '
                    f'{str(item.get("invalidationReason") or "ended").replace("_", " ")}'
                )

        cmm_signals = changes.get("addedCmmSignals", []) + changes.get("changedCmmSignals", [])
        if cmm_signals:
            lines.append("")
            lines.append(f"CMM cohort alerts >={CMM_ALERT_PROBABILITY_THRESHOLD:.0f}/100")
            for item in cmm_signals[:8]:
                move_note = ""
                if "fromProbabilityScore" in item:
                    move_note = f' ({to_float(item.get("fromProbabilityScore")):.0f}->{to_float(item.get("toProbabilityScore")):.0f})'
                cohorts = "/".join(str(component.get("segment")) for component in item.get("components", [])[:3])
                bias_pct = abs(to_float(item.get("valueBias"))) * 100
                lines.append(
                    f'- {str(item.get("action", "watch")).upper()} {item["coin"]} {item["side"]}: '
                    f'p{to_float(item.get("probabilityScore")):.0f}{move_note}, '
                    f'{item.get("cohortCount", 0)} cohorts, bias {bias_pct:.0f}%, '
                    f'{format_money_compact(item.get("totalValue"))}, {cohorts}'
                )

        if changes.get("clusteredOpenPositions"):
            lines.append("")
            lines.append(
                f"{CLUSTERED_OPEN_ALERT_MIN_WALLETS}+ opens >{format_money_compact(FRESH_WALLET_FLOW_MIN_VALUE)} in 5m"
            )
            for item in changes["clusteredOpenPositions"][:10]:
                size_note = ""
                if to_float(item.get("totalSize")) > 0:
                    size_note = f' sz {format_position_size(to_float(item.get("totalSize")))}'
                entry_note = ""
                if to_float(item.get("entryPx")) > 0:
                    entry_note = f' VWAP ${format_price(to_float(item.get("entryPx")))}'
                lines.append(
                    f'- {item["coin"]} {item["side"]}: {int(item.get("walletCount") or 0)} wallets, '
                    f'{format_money_compact(item["totalValue"])}{size_note}{entry_note}'
                )
                for wallet in item.get("wallets", [])[:5]:
                    lines.append(
                        f'  {wallet_label(wallet.get("alias", ""), wallet.get("address", ""))}: '
                        f'{format_money_compact(wallet.get("totalValue"))}'
                    )

        if changes["newLargePositions"]:
            lines.append("")
            lines.append(f"Open >{format_money_compact(NEW_POSITION_ALERT_MIN_VALUE)}")
            for item in changes["newLargePositions"][:10]:
                size_note = ""
                if to_float(item.get("totalSize")) > 0:
                    size_note = f' sz {format_position_size(to_float(item.get("totalSize")))}'
                entry_note = ""
                if to_float(item.get("entryPx")) > 0:
                    entry_label = "open VWAP" if item.get("entryPriceSource") == "fill" else "entry"
                    entry_note = f' {entry_label} ${format_price(to_float(item.get("entryPx")))}'
                lines.append(
                    f'- {wallet_label(item.get("alias", ""), item.get("address", ""))}: {item["coin"]} {item["side"]} {format_money_compact(item["totalValue"])}{size_note}{entry_note}'
                )

        if changes["closedLargePositions"]:
            lines.append("")
            lines.append(f"Closed >{format_money_compact(NEW_POSITION_ALERT_MIN_VALUE)}")
            for item in changes["closedLargePositions"][:10]:
                size_note = ""
                if to_float(item.get("totalSize")) > 0:
                    size_note = f' sz {format_position_size(to_float(item.get("totalSize")))}'
                close_note = ""
                if to_float(item.get("closePrice")) > 0:
                    price_marker = "@" if item.get("closePriceSource") == "fill" else "~$"
                    if price_marker == "@":
                        close_note = f' close @${format_price(to_float(item.get("closePrice")))}'
                    else:
                        close_note = f' last ~${format_price(to_float(item.get("closePrice")))}'
                lines.append(
                    f'- {wallet_label(item.get("alias", ""), item.get("address", ""))}: {item["coin"]} {item["side"]} {format_money_compact(item["totalValue"])}{size_note}{close_note}'
                )

        if changes["increasedLargePositions"]:
            lines.append("")
            lines.append(f"Added >{format_money_compact(POSITION_INCREASE_ALERT_MIN_DELTA)}")
            for item in changes["increasedLargePositions"][:10]:
                size_note = ""
                if to_float(item.get("sizeIncrease")) > 0:
                    size_note = f' +{format_position_size(to_float(item.get("sizeIncrease")))}'
                add_price_note = ""
                if to_float(item.get("addPrice")) > 0:
                    add_label = "recent add VWAP" if item.get("addPriceSource") == "fill" else "estimated add"
                    add_price_note = f' {add_label} ${format_price(to_float(item.get("addPrice")))}'
                add_value = to_float(item.get("addValue", item.get("increaseValue")))
                lines.append(
                    f'- {wallet_label(item.get("alias", ""), item.get("address", ""))}: {item["coin"]} {item["side"]} {format_money_compact(item["previousValue"])}->{format_money_compact(item["totalValue"])} (+{format_money_compact(add_value)}{size_note}{add_price_note})'
                )

        return "\n".join(lines)

    def build_summary_message(
        self,
        summary: dict[str, Any],
        min_wallets: int,
        *,
        title: str = "Current wallet sentiment",
        include_consensus: bool = True,
        include_hip3: bool = False,
        include_signals: bool = True,
    ) -> str:
        lines = [
            title,
            f"Bias: {summary.get('overallBias', 'mixed')}",
            f"Consensus threshold: {min_wallets} wallets",
            f'Wallets tracked: {summary.get("walletCount", 0)}',
        ]
        if include_signals:
            lines.append(f'Actionable signals: {summary.get("signalCount", len(summary.get("signals", [])))}')

        if include_signals:
            signals = summary.get("signals", [])
            lines.append("")
            lines.append("Signals:")
            if signals:
                for item in signals[:10]:
                    probability = to_float(item.get("probabilityScore", item.get("convictionScore")))
                    net_note = f', net +{int(to_float(item.get("netIndependentWalletCount", item.get("netWalletCount"))))}' if "netWalletCount" in item else ""
                    if "netWeightedWalletCount" in item:
                        net_note += f', qnet +{to_float(item.get("netIndependentWeightedWalletCount", item.get("netWeightedWalletCount"))):.1f}'
                    activity_bits = []
                    if int(to_float(item.get("freshActivityWalletCount"))):
                        activity_bits.append(f'{int(to_float(item.get("freshActivityWalletCount")))} 5m activity')
                    if int(to_float(item.get("recentAddWalletCount"))):
                        activity_bits.append(f'{int(to_float(item.get("recentAddWalletCount")))} adds')
                    if int(to_float(item.get("verifiedFreshIndependentWalletCount"))):
                        activity_bits.append(
                            f'{int(to_float(item.get("verifiedFreshIndependentWalletCount")))} verified 15m'
                        )
                    activity_note = f', {"/".join(activity_bits)}' if activity_bits else ""
                    lines.append(
                        f'- {str(item.get("action", "watch")).upper()} {item["coin"]} {item["side"]} '
                        f'({item["walletCount"]} wallets/{int(to_float(item.get("independentWalletCount", item["walletCount"])))} independent{net_note}, p{probability:.0f}/100{activity_note})'
                    )
            else:
                lines.append(f'- None at {ACTIONABLE_SIGNAL_PROBABILITY_THRESHOLD:.0f}+ probability')

        if include_consensus:
            consensus = summary.get("consensus", [])
            lines.append("")
            lines.append("Consensus:")
            main_consensus = [
                item
                for item in consensus
                if not str(item.get("coin", "")).startswith("@")
                and not is_commodity_like_position(item.get("coin"))
                and not is_stock_like_position(item.get("coin"))
            ]
            commodity_consensus = [item for item in consensus if is_commodity_like_position(item.get("coin"))]
            stock_consensus = [item for item in consensus if is_stock_like_position(item.get("coin"))]

            def append_consensus_items(items: list[dict[str, Any]]) -> None:
                for item in items[:10]:
                    net_note = f', net +{int(to_float(item.get("netIndependentWalletCount", item.get("netWalletCount"))))}' if "netWalletCount" in item else ""
                    if "netWeightedWalletCount" in item:
                        net_note += f', qnet +{to_float(item.get("netIndependentWeightedWalletCount", item.get("netWeightedWalletCount"))):.1f}'
                    lines.append(
                        f'- {item["coin"]} {item["side"]} ({item["walletCount"]} wallets/{int(to_float(item.get("independentWalletCount", item["walletCount"])))} independent{net_note}, conviction {item.get("convictionScore", 0):.0f}/100)'
                    )

            if main_consensus:
                append_consensus_items(main_consensus)
            else:
                lines.append("- None")
            if commodity_consensus:
                lines.append("")
                lines.append("Commodities consensus:")
                append_consensus_items(commodity_consensus)
            if stock_consensus:
                lines.append("")
                lines.append("Stocks / indices consensus:")
                append_consensus_items(stock_consensus)

        if include_hip3:
            hip3_consensus = summary.get("hip3Consensus", [])
            lines.append("")
            lines.append("HIP-3 consensus:")
            if hip3_consensus:
                for item in hip3_consensus[:10]:
                    lines.append(
                        f'- {item["coin"]} {item["side"]} ({item["walletCount"]} wallets)'
                    )
            else:
                lines.append("- None")

        lines.append("")
        lines.append(f'Checked at: {summary.get("generatedAt", now_iso())}')
        return "\n".join(lines)

    def live_sentiment_summary(self, min_wallets: int) -> dict[str, Any]:
        dashboard = self.dashboard()
        raw = load_json_file(self.alerts_path, {})
        stored_config = raw.get("config", {}) if isinstance(raw, dict) else {}
        state = raw.get("state", {}) if isinstance(raw, dict) else {}
        summary, _cohort = self.build_monthly_sentiment_summary(
            dashboard,
            min_wallets,
            state,
            persist=True,
            stored_config=stored_config,
        )
        return summary

    def build_position_groups(
        self,
        dashboard: dict[str, Any],
        *,
        min_value: float = MIN_POSITION_MESSAGE_VALUE,
        min_wallets: int = MIN_POSITION_MESSAGE_WALLETS,
        hip3_only: bool | None = None,
        stock_like_only: bool | None = None,
        commodity_like_only: bool | None = None,
        now_ms: int | None = None,
    ) -> list[dict[str, Any]]:
        groups: dict[tuple[str, str], dict[str, Any]] = {}
        checked_ms = current_time_ms() if now_ms is None else now_ms
        recent_add_cutoff = checked_ms - POSITION_RECENT_ADD_WINDOW_MS
        recent_adds: dict[str, dict[str, float]] = {}

        for wallet in dashboard.get("wallets", []):
            address = str(wallet.get("address") or "")
            if not address or not self.wallet_fill_data_reliable(wallet):
                continue
            for fill in wallet.get("recentFills", []):
                classified = self.classify_fill_direction(fill.get("direction"))
                if not classified or classified[1] != "add":
                    continue
                fill_time = int(to_float(fill.get("time")))
                fill_price = to_float(fill.get("price"))
                fill_size = abs(to_float(fill.get("size")))
                if fill_time < recent_add_cutoff or fill_time > checked_ms or fill_price <= 0 or fill_size <= 0:
                    continue
                fill_side, _ = classified
                key = self.position_lifecycle_key(address, normalize_position_coin(fill.get("coin")), fill_side)
                bucket = recent_adds.setdefault(key, {"value": 0.0, "size": 0.0, "lastAt": 0.0})
                bucket["value"] += fill_price * fill_size
                bucket["size"] += fill_size
                bucket["lastAt"] = max(bucket["lastAt"], fill_time)

        for wallet in dashboard.get("wallets", []):
            address = str(wallet.get("address") or "")
            for position in wallet.get("positions", []):
                position_value = to_float(position.get("positionValue"))
                raw_coin = str(position.get("coin") or "Unknown")
                coin = normalize_position_coin(raw_coin)
                if not should_count_open_position(address, coin, position):
                    continue
                is_hip3 = raw_coin.startswith("@")
                is_stock_like = is_stock_like_position(raw_coin)
                is_commodity_like = is_commodity_like_position(raw_coin)
                if hip3_only is True and not is_hip3:
                    continue
                if hip3_only is False and is_hip3:
                    continue
                if stock_like_only is True and not is_stock_like:
                    continue
                if stock_like_only is False and is_stock_like:
                    continue
                if commodity_like_only is True and not is_commodity_like:
                    continue
                if commodity_like_only is False and is_commodity_like:
                    continue
                side = str(position.get("side") or "Flat").lower()
                if side not in {"long", "short"}:
                    continue
                key = (coin, side)
                bucket = groups.setdefault(
                    key,
                    {
                        "coin": coin,
                        "side": side,
                        "walletCount": 0,
                        "positionCount": 0,
                        "totalValue": 0.0,
                        "totalSize": 0.0,
                        "entryValue": 0.0,
                        "entrySum": 0.0,
                        "entryCount": 0,
                        "walletAddresses": set(),
                        "recentAddValue": 0.0,
                        "recentAddSize": 0.0,
                        "recentAddWalletAddresses": set(),
                        "recentAddAt": 0,
                    },
                )
                size = abs(to_float(position.get("size")))
                bucket["positionCount"] += 1
                bucket["totalValue"] += position_value
                bucket["totalSize"] += size
                bucket["entryValue"] += to_float(position.get("entryPx")) * size
                if address and address not in bucket["walletAddresses"]:
                    bucket["walletAddresses"].add(address)
                    bucket["walletCount"] += 1
                    entry_px = to_float(position.get("entryPx"))
                    if entry_px > 0:
                        bucket["entrySum"] += entry_px
                        bucket["entryCount"] += 1
                recent_add = recent_adds.get(self.position_lifecycle_key(address, coin, side), {})
                if recent_add:
                    bucket["recentAddValue"] += to_float(recent_add.get("value"))
                    bucket["recentAddSize"] += to_float(recent_add.get("size"))
                    bucket["recentAddAt"] = max(int(bucket["recentAddAt"]), int(to_float(recent_add.get("lastAt"))))
                    if address:
                        bucket["recentAddWalletAddresses"].add(address)
        rows = []
        for item in groups.values():
            if item["walletCount"] < min_wallets or item["totalValue"] < min_value:
                continue
            total_size = to_float(item["totalSize"])
            entry_count = int(to_float(item["entryCount"]))
            entry_px = (
                round(to_float(item["entryValue"]) / total_size, 8)
                if total_size > 0
                else round(to_float(item["entrySum"]) / entry_count, 8)
                if entry_count > 0
                else 0.0
            )
            rows.append(
                {
                    "coin": item["coin"],
                    "side": item["side"],
                    "walletCount": item["walletCount"],
                    "positionCount": item["positionCount"],
                    "totalValue": round(item["totalValue"], 2),
                    "totalSize": round(total_size, 8),
                    "entryPx": entry_px,
                    "entryType": "size_weighted"
                    if total_size > 0
                    else "simple_average"
                    if entry_count > 0
                    else "",
                    "recentAddPx": round(to_float(item["recentAddValue"]) / to_float(item["recentAddSize"]), 8)
                    if to_float(item["recentAddSize"]) > 0
                    else 0.0,
                    "recentAddWalletCount": len(item["recentAddWalletAddresses"]),
                    "recentAddAt": int(item["recentAddAt"]),
                }
            )

        return sorted(
            rows,
            key=lambda item: (-item["walletCount"], item["coin"], item["side"]),
        )

    def cmm_signal_coins(self) -> list[str]:
        return [coin.upper() for coin in env_csv("CMM_SIGNAL_COINS", CMM_SIGNAL_DEFAULT_COINS)]

    def cmm_signal_segments(self) -> list[int]:
        return env_int_csv("CMM_SIGNAL_SEGMENTS", CMM_SIGNAL_DEFAULT_SEGMENTS)

    def cmm_contrarian_segments(self) -> list[int]:
        return env_int_csv("CMM_CONTRARIAN_SEGMENTS", CMM_CONTRARIAN_DEFAULT_SEGMENTS)

    def cmm_trend_enrichment_enabled(self) -> bool:
        return env_flag("CMM_TREND_ENRICHMENT", CMM_TREND_ENRICHMENT_ENABLED)

    def cmm_max_trend_coins(self) -> int:
        return max(0, env_int("CMM_SIGNAL_MAX_TREND_COINS", CMM_SIGNAL_MAX_TREND_COINS))

    def cmm_max_trend_requests(self) -> int:
        return max(0, env_int("CMM_SIGNAL_MAX_TREND_REQUESTS", CMM_SIGNAL_MAX_TREND_REQUESTS))

    def cmm_max_trend_segments_per_coin(self) -> int:
        return max(0, env_int("CMM_SIGNAL_MAX_TREND_SEGMENTS_PER_COIN", CMM_SIGNAL_MAX_TREND_SEGMENTS_PER_COIN))

    def cmm_cache_ttl_ms(self) -> int:
        return max(0, env_int("CMM_SIGNAL_CACHE_TTL_MINUTES", CMM_SIGNAL_CACHE_TTL_MINUTES)) * 60_000

    def cmm_rate_limit_backoff_ms(self) -> int:
        return max(1, env_int("CMM_RATE_LIMIT_BACKOFF_MINUTES", CMM_RATE_LIMIT_BACKOFF_MINUTES)) * 60_000

    @staticmethod
    def is_cmm_rate_limit_error(error: Exception) -> bool:
        message = str(error).lower()
        return "429" in message or "daily limit" in message or "rate limit" in message

    def cmm_signal_tier(self, probability: float, total_value: float) -> str:
        if total_value < CMM_ACTIONABLE_MIN_TOTAL_VALUE:
            return "watch"
        if probability >= CMM_ALERT_PROBABILITY_THRESHOLD:
            return "alert"
        if probability >= CMM_SIGNAL_PROBABILITY_THRESHOLD:
            return "actionable"
        return "watch"

    def cmm_contrarian_score(self, side: str, components: list[dict[str, Any]]) -> tuple[float, float]:
        if not components:
            return 0.0, 0.0
        total_weight = sum(to_float(item.get("weight")) for item in components)
        if total_weight <= 0:
            return 0.0, 0.0
        weak_bias = sum(to_float(item.get("valueBias")) * to_float(item.get("weight")) for item in components) / total_weight
        wants_opposite = (side == "long" and weak_bias < 0) or (side == "short" and weak_bias > 0)
        agrees_with_side = (side == "long" and weak_bias > 0) or (side == "short" and weak_bias < 0)
        if wants_opposite:
            return min(100.0, abs(weak_bias) * 100.0), weak_bias
        if agrees_with_side:
            return -min(40.0, abs(weak_bias) * 60.0), weak_bias
        return 0.0, weak_bias

    def cmm_metric_signal_component(self, coin: str, segment_id: int, metric: dict[str, Any]) -> dict[str, Any] | None:
        position_count = int(to_float(metric.get("positionCount")))
        long_count = int(to_float(metric.get("positionCountLong")))
        total_value = to_float(metric.get("totalPositionValue"))
        long_value = to_float(metric.get("totalPositionValueLong"))
        if position_count <= 0 or total_value <= 0:
            return None

        short_count = max(0, position_count - long_count)
        short_value = max(0.0, total_value - long_value)
        value_bias = ((long_value - short_value) / total_value) if total_value else 0.0
        count_bias = ((long_count - short_count) / position_count) if position_count else 0.0
        weight = CMM_SIGNAL_SEGMENT_WEIGHTS.get(segment_id, 0.5)
        side = "long" if value_bias > 0 else "short" if value_bias < 0 else "mixed"
        return {
            "coin": normalize_cmm_coin(coin).upper(),
            "segmentId": segment_id,
            "segment": CMM_SIGNAL_SEGMENT_LABELS.get(segment_id, f"Segment {segment_id}"),
            "side": side,
            "positionCount": position_count,
            "longCount": long_count,
            "shortCount": short_count,
            "totalValue": total_value,
            "longValue": long_value,
            "shortValue": short_value,
            "valueBias": value_bias,
            "countBias": count_bias,
            "weight": weight,
            "unrealizedPnl": to_float(metric.get("totalUnrealizedPnl")),
            "createdAt": metric.get("createdAt", ""),
        }

    def cmm_heatmap_signal_component(self, coin: str, segment_id: int, segment: dict[str, Any]) -> dict[str, Any] | None:
        position_count = int(to_float(first_present(segment, "count", "positionCount", "totalCount")))
        long_count = int(to_float(first_present(segment, "countLong", "longCount", "positionCountLong")))
        short_count = int(to_float(first_present(segment, "countShort", "shortCount", "positionCountShort")))
        total_value = to_float(first_present(segment, "totalValue", "positionValue", "totalPositionValue"))
        long_value = to_float(first_present(segment, "totalLongValue", "longValue", "totalPositionValueLong"))
        short_value = to_float(first_present(segment, "totalShortValue", "shortValue", "totalPositionValueShort"))
        total_size = abs(
            to_float(
                first_present(
                    segment,
                    "totalSize",
                    "size",
                    "positionSize",
                    "totalPositionSize",
                    "openInterest",
                )
            )
        )
        price = to_float(
            first_present(
                segment,
                "entryPrice",
                "entryPx",
                "avgEntryPrice",
                "averageEntryPrice",
                "avgPrice",
                "averagePrice",
            )
        )
        price_source = "entry" if price > 0 else ""
        raw_unrealized_pnl = first_present(segment, "unrealizedPnl", "unrealizedPnL", "totalUnrealizedPnl")
        if not short_value and total_value and long_value:
            short_value = max(0.0, total_value - long_value)
        if position_count <= 0 or total_value <= 0:
            return None

        if short_count <= 0:
            short_count = max(0, position_count - long_count)
        if "bias" in segment:
            value_bias = (to_float(segment.get("bias")) - 0.5) * 2.0
        else:
            value_bias = ((long_value - short_value) / total_value) if total_value else 0.0
        count_bias = ((long_count - short_count) / position_count) if position_count else 0.0
        weight = CMM_SIGNAL_SEGMENT_WEIGHTS.get(segment_id, 0.5)
        side = "long" if value_bias > 0 else "short" if value_bias < 0 else "mixed"
        return {
            "coin": normalize_cmm_coin(coin).upper(),
            "segmentId": segment_id,
            "segment": CMM_SIGNAL_SEGMENT_LABELS.get(segment_id, f"Segment {segment_id}"),
            "side": side,
            "positionCount": position_count,
            "longCount": long_count,
            "shortCount": short_count,
            "totalValue": total_value,
            "totalSize": total_size,
            "price": price,
            "priceSource": price_source,
            "longValue": long_value,
            "shortValue": short_value,
            "valueBias": value_bias,
            "countBias": count_bias,
            "weight": weight,
            "unrealizedPnl": to_float(raw_unrealized_pnl),
            "hasUnrealizedPnl": raw_unrealized_pnl is not None,
            "createdAt": segment.get("createdAt", ""),
        }

    def cmm_heatmap_rows(self, payload: Any) -> list[dict[str, Any]]:
        if isinstance(payload, list):
            return [item for item in payload if isinstance(item, dict)]
        if not isinstance(payload, dict):
            return []
        for key in ("items", "data", "results", "coins", "heatmap"):
            value = payload.get(key)
            if isinstance(value, list):
                return [item for item in value if isinstance(item, dict)]
        return []

    @staticmethod
    def cmm_position_rows(payload: Any) -> list[dict[str, Any]]:
        if isinstance(payload, list):
            return [item for item in payload if isinstance(item, dict)]
        if not isinstance(payload, dict):
            return []
        for key in ("positions", "items", "data", "results"):
            value = payload.get(key)
            if isinstance(value, list):
                return [item for item in value if isinstance(item, dict)]
        return []

    @staticmethod
    def cmm_component_entry_price(component: dict[str, Any], side: str) -> tuple[float, str]:
        reported_entry = to_float(component.get("price"))
        if reported_entry > 0:
            return reported_entry, "reported"

        if not component.get("hasUnrealizedPnl"):
            return 0.0, ""
        size = abs(to_float(component.get("totalSize")))
        value = abs(to_float(component.get("totalValue")))
        if size <= 0 or value <= 0:
            return 0.0, ""
        mark_price = value / size
        unrealized_pnl = to_float(component.get("unrealizedPnl"))
        implied_entry = mark_price - (unrealized_pnl / size) if side == "long" else mark_price + (unrealized_pnl / size)
        return (implied_entry, "implied") if implied_entry > 0 else (0.0, "")

    def score_cmm_components(
        self,
        coin: str,
        components: list[dict[str, Any]],
        *,
        contrarian_components: list[dict[str, Any]] | None = None,
        trend_score: float | None = None,
    ) -> dict[str, Any] | None:
        if not components:
            return None
        weighted_bias = sum(to_float(item.get("valueBias")) * to_float(item.get("weight")) for item in components)
        total_weight = sum(to_float(item.get("weight")) for item in components)
        if total_weight <= 0:
            return None
        aggregate_bias = weighted_bias / total_weight
        side = "long" if aggregate_bias > 0 else "short" if aggregate_bias < 0 else "mixed"
        if side == "mixed":
            return None

        agreeing = [item for item in components if item.get("side") == side]
        if len(agreeing) < 2:
            return None

        count_bias = sum(to_float(item.get("countBias")) * to_float(item.get("weight")) for item in components) / total_weight
        aligned_count_bias = abs(count_bias) if (count_bias > 0 and side == "long") or (count_bias < 0 and side == "short") else 0.0
        total_value = sum(to_float(item.get("totalValue")) for item in agreeing)
        total_size = sum(to_float(item.get("totalSize")) for item in agreeing)
        entry_components = [
            (*self.cmm_component_entry_price(item, side), abs(to_float(item.get("totalSize"))))
            for item in agreeing
        ]
        entry_components = [item for item in entry_components if item[0] > 0 and item[2] > 0]
        priced_size = sum(item[2] for item in entry_components)
        weighted_price_value = sum(item[0] * item[2] for item in entry_components)
        entry_sources = {item[1] for item in entry_components}
        if entry_sources == {"reported"}:
            price_source = "cohort-aggregate-entry"
        elif entry_sources:
            price_source = "cohort-implied-entry"
        else:
            price_source = ""
        total_positions = sum(int(to_float(item.get("positionCount"))) for item in agreeing)
        total_unrealized = sum(to_float(item.get("unrealizedPnl")) for item in agreeing)
        smart_score = abs(aggregate_bias) * 100.0
        count_score = aligned_count_bias * 100.0
        agreement_score = min(10.0, (len(agreeing) / 2.0) * 10.0)
        contrarian_score, weak_bias = self.cmm_contrarian_score(side, contrarian_components or [])
        pnl_score = 4.0 if total_unrealized > 0 else -10.0 if total_unrealized < -1_000_000 else 0.0
        trend_available = trend_score is not None
        normalized_trend_score = max(0.0, min(to_float(trend_score), 100.0)) if trend_available else 0.0
        base_probability = (smart_score * 0.58) + (count_score * 0.14) + (contrarian_score * 0.10)
        # Missing trend history is not evidence against a position. Normalize the
        # available components instead of treating an unavailable trend as zero.
        if trend_available:
            probability = clamp(base_probability + (normalized_trend_score * 0.14) + agreement_score + pnl_score)
        else:
            probability = clamp((base_probability / 0.82) + agreement_score + pnl_score)
        tier = self.cmm_signal_tier(probability, total_value)

        return {
            "source": "coinmarketman",
            "coin": normalize_cmm_coin(coin).upper(),
            "side": side,
            "action": signal_action_from_side(side),
            "probabilityScore": round(probability, 1),
            "signalTier": tier,
            "alertEligible": tier == "alert",
            "actionableEligible": tier in {"actionable", "alert"},
            "valueBias": round(aggregate_bias, 4),
            "countBias": round(count_bias, 4),
            "smartCohortScore": round(smart_score, 1),
            "trendScore": round(normalized_trend_score, 1) if trend_available else None,
            "trendAvailable": trend_available,
            "contrarianScore": round(contrarian_score, 1),
            "weakCohortBias": round(weak_bias, 4),
            "cohortCount": len(agreeing),
            "sampledCohortCount": len(components),
            "positionCount": total_positions,
            "totalValue": round(total_value, 2),
            "totalSize": round(total_size, 8),
            "price": round(weighted_price_value / priced_size, 8) if priced_size > 0 and weighted_price_value > 0 else 0.0,
            "priceSource": price_source,
            "entryCoveragePct": round((priced_size / total_size) * 100, 1) if total_size > 0 else 0.0,
            "unrealizedPnl": round(total_unrealized, 2),
            "components": agreeing,
            "strongComponents": components,
            "contrarianComponents": contrarian_components or [],
        }

    def build_cmm_signal_summary_from_heatmap(
        self,
        payload: Any,
        *,
        coins: list[str],
        segment_ids: list[int],
        contrarian_segment_ids: list[int],
        min_probability: float,
        diagnostics: dict[str, Any] | None = None,
    ) -> list[dict[str, Any]]:
        coin_filter = {coin.upper() for coin in coins}
        segment_filter = set(segment_ids)
        contrarian_filter = set(contrarian_segment_ids)
        rows = self.cmm_heatmap_rows(payload)
        if diagnostics is not None:
            diagnostics["heatmapRows"] = len(rows)
            diagnostics["smartComponents"] = 0
            diagnostics["contrarianComponents"] = 0
            diagnostics["scoredCandidates"] = 0
            diagnostics["lowValueCandidates"] = 0
        signals: list[dict[str, Any]] = []
        for row in rows:
            if not isinstance(row, dict):
                continue
            coin = str(row.get("coin") or "").upper()
            if not coin or (coin_filter and coin not in coin_filter):
                continue
            components: list[dict[str, Any]] = []
            contrarian_components: list[dict[str, Any]] = []
            for segment in row.get("segments", []):
                if not isinstance(segment, dict):
                    continue
                segment_id = int(to_float(segment.get("segmentId")))
                component = self.cmm_heatmap_signal_component(coin, segment_id, segment)
                if not component:
                    continue
                if segment_id in segment_filter:
                    components.append(component)
                    if diagnostics is not None:
                        diagnostics["smartComponents"] += 1
                elif segment_id in contrarian_filter:
                    contrarian_components.append(component)
                    if diagnostics is not None:
                        diagnostics["contrarianComponents"] += 1
            signal = self.score_cmm_components(coin, components, contrarian_components=contrarian_components)
            if signal and diagnostics is not None:
                diagnostics["scoredCandidates"] += 1
            if signal and to_float(signal.get("totalValue")) < CMM_SIGNAL_MIN_TOTAL_VALUE:
                if diagnostics is not None:
                    diagnostics["lowValueCandidates"] += 1
                continue
            if signal and to_float(signal.get("probabilityScore")) >= min_probability:
                signals.append(signal)
        return signals

    def cmm_metric_trend_score(
        self,
        side: str,
        metrics: list[dict[str, Any]],
        segment_id: int,
        coin: str,
    ) -> float | None:
        components = [
            self.cmm_metric_signal_component(coin, segment_id, metric)
            for metric in sorted(metrics, key=lambda item: str(item.get("createdAt") or ""))
            if isinstance(metric, dict)
        ]
        components = [component for component in components if component]
        if len(components) < 2:
            return None
        first_bias = to_float(components[0].get("valueBias"))
        last_bias = to_float(components[-1].get("valueBias"))
        aligned_delta = last_bias - first_bias if side == "long" else first_bias - last_bias
        return max(0.0, min(100.0, aligned_delta * 400.0))

    def enrich_cmm_signals_with_trends(self, signals: list[dict[str, Any]], timeframe: str) -> list[dict[str, Any]]:
        if not signals:
            return signals
        start = iso_hours_ago(CMM_SIGNAL_TREND_LOOKBACK_HOURS)
        end = now_iso()
        enriched: list[dict[str, Any]] = []
        metric_request_count = 0
        request_limit = self.cmm_max_trend_requests()
        trend_targets = {
            str(item.get("coin")).upper()
            for item in sorted(signals, key=lambda signal: to_float(signal.get("probabilityScore")), reverse=True)[
                : self.cmm_max_trend_coins()
            ]
        }
        for signal in signals:
            coin = str(signal.get("coin") or "").upper()
            if coin not in trend_targets:
                enriched.append(signal)
                continue
            side = str(signal.get("side") or "")
            segment_scores: list[float] = []
            latest_metric_ms = 0
            components = sorted(
                (component for component in signal.get("components", []) if isinstance(component, dict)),
                key=lambda component: to_float(component.get("weight")),
                reverse=True,
            )[: self.cmm_max_trend_segments_per_coin()]
            for component in components:
                if metric_request_count >= request_limit or getattr(self, "_cmm_trend_rate_limited", False):
                    break
                segment_id = int(to_float(component.get("segmentId")))
                try:
                    metric_request_count += 1
                    payload = self.cmm_client.position_metrics(
                        coin,
                        segment_id,
                        start=start,
                        end=end,
                        limit=4,
                        position_recency_timeframe=timeframe,
                    )
                except CoinMarketManApiError as exc:
                    if self.is_cmm_rate_limit_error(exc):
                        self._cmm_trend_rate_limited = True
                    continue
                metrics = payload.get("metrics", []) if isinstance(payload, dict) else []
                if metrics:
                    score = self.cmm_metric_trend_score(side, metrics, segment_id, coin)
                    if score is not None:
                        segment_scores.append(score)
                    metric_times = [iso_to_ms(metric.get("createdAt")) for metric in metrics if isinstance(metric, dict)]
                    if metric_times:
                        latest_metric_ms = max(latest_metric_ms, max(metric_times))
            if not segment_scores:
                enriched.append(signal)
                continue
            trend_score = sum(segment_scores) / len(segment_scores)
            rescored = self.score_cmm_components(
                coin,
                [item for item in signal.get("strongComponents", []) if isinstance(item, dict)],
                contrarian_components=[
                    item for item in signal.get("contrarianComponents", []) if isinstance(item, dict)
                ],
                trend_score=trend_score,
            )
            if rescored and latest_metric_ms:
                lag_minutes = max(0.0, (current_time_ms() - latest_metric_ms) / 60_000)
                rescored["latestMetricAt"] = (
                    datetime.fromtimestamp(latest_metric_ms / 1000, timezone.utc)
                    .isoformat()
                    .replace("+00:00", "Z")
                )
                rescored["metricLagMinutes"] = round(lag_minutes, 1)
                rescored["dataFreshness"] = "fresh" if lag_minutes <= 20 else "stale"
            enriched.append(rescored or signal)
        return enriched

    def build_cmm_signal_summary(
        self,
        *,
        coins: list[str] | None = None,
        segment_ids: list[int] | None = None,
        position_recency_timeframe: str | None = None,
        min_probability: float = CMM_WATCH_PROBABILITY_THRESHOLD,
    ) -> dict[str, Any]:
        if not self.cmm_client.token:
            return {
                "enabled": False,
                "error": "Missing COINMARKETMAN_API_TOKEN",
                "signals": [],
                "signalCount": 0,
                "generatedAt": now_iso(),
            }

        target_coins = coins or self.cmm_signal_coins()
        target_segments = segment_ids or self.cmm_signal_segments()
        contrarian_segments = self.cmm_contrarian_segments()
        timeframe = position_recency_timeframe or os.environ.get("CMM_SIGNAL_POSITION_RECENCY", "7d")
        signals: list[dict[str, Any]] = []
        below_threshold_signals: list[dict[str, Any]] = []
        errors: list[str] = []
        heatmap_failed = False
        heatmap_quota_limited = False
        self._cmm_trend_rate_limited = False
        diagnostics: dict[str, Any] = {
            "heatmapRows": 0,
            "smartComponents": 0,
            "contrarianComponents": 0,
            "scoredCandidates": 0,
            "lowValueCandidates": 0,
        }

        try:
            heatmap = self.cmm_client.positions_heatmap(opened_within=timeframe)
            all_heatmap_signals = self.build_cmm_signal_summary_from_heatmap(
                heatmap,
                coins=target_coins,
                segment_ids=target_segments,
                contrarian_segment_ids=contrarian_segments,
                min_probability=0.0,
                diagnostics=diagnostics,
            )
            if self.cmm_trend_enrichment_enabled():
                all_heatmap_signals = self.enrich_cmm_signals_with_trends(all_heatmap_signals, timeframe)
                if self._cmm_trend_rate_limited:
                    errors.append("trend metrics: CMM API rate limited")
            signals = [
                signal
                for signal in all_heatmap_signals
                if to_float(signal.get("probabilityScore")) >= min_probability
            ]
            below_threshold_signals = [
                signal
                for signal in all_heatmap_signals
                if to_float(signal.get("probabilityScore")) < min_probability
            ]
        except (AttributeError, CoinMarketManApiError) as exc:
            errors.append(f"heatmap: {exc}")
            heatmap_failed = True
            heatmap_quota_limited = "429" in str(exc) or "daily limit" in str(exc).lower()

        for coin in (target_coins or list(CMM_SIGNAL_FALLBACK_COINS)) if heatmap_failed and not heatmap_quota_limited else []:
            components: list[dict[str, Any]] = []
            start = iso_hours_ago(CMM_SIGNAL_TREND_LOOKBACK_HOURS)
            end = now_iso()
            for segment_id in target_segments:
                try:
                    payload = self.cmm_client.position_metrics(
                        coin,
                        segment_id,
                        start=start,
                        end=end,
                        limit=1,
                        position_recency_timeframe=timeframe,
                    )
                except CoinMarketManApiError as exc:
                    errors.append(f"{coin}:{segment_id}: {exc}")
                    continue
                metrics = payload.get("metrics", []) if isinstance(payload, dict) else []
                if not metrics:
                    continue
                component = self.cmm_metric_signal_component(str(coin), segment_id, metrics[0])
                if component:
                    components.append(component)
            signal = self.score_cmm_components(str(coin), components)
            if signal and to_float(signal.get("probabilityScore")) >= min_probability:
                signals.append(signal)

        signals.sort(
            key=lambda item: (
                to_float(item.get("probabilityScore")),
                to_float(item.get("totalValue")),
                int(to_float(item.get("positionCount"))),
            ),
            reverse=True,
        )
        return {
            "enabled": True,
            "source": "coinmarketman",
            "rateLimited": heatmap_quota_limited or self._cmm_trend_rate_limited,
            "timeframe": timeframe,
            "coins": target_coins,
            "segments": target_segments,
            "contrarianSegments": contrarian_segments,
            "watchThreshold": CMM_WATCH_PROBABILITY_THRESHOLD,
            "signalThreshold": CMM_SIGNAL_PROBABILITY_THRESHOLD,
            "alertThreshold": CMM_ALERT_PROBABILITY_THRESHOLD,
            "signals": signals,
            "belowThresholdSignals": sorted(
                below_threshold_signals,
                key=lambda item: (
                    to_float(item.get("probabilityScore")),
                    to_float(item.get("totalValue")),
                    int(to_float(item.get("positionCount"))),
                ),
                reverse=True,
            )[:5],
            "signalCount": len(signals),
            "diagnostics": diagnostics,
            "error": "; ".join(errors[:3]),
            "generatedAt": now_iso(),
        }

    def enrich_cmm_signals_with_position_entries(
        self,
        summary: dict[str, Any],
        *,
        limit: int = 3,
    ) -> dict[str, Any]:
        signals = summary.get("signals", []) if isinstance(summary, dict) else []
        if not self.cmm_client.token or not isinstance(signals, list):
            return summary

        enriched: list[dict[str, Any]] = []
        errors: list[str] = []
        missing_entries = 0
        entry_window_start = iso_hours_ago(30 * 24)
        entry_window_end = now_iso()
        for signal in signals:
            if not isinstance(signal, dict):
                continue
            if to_float(signal.get("price")) > 0 or missing_entries >= max(0, limit):
                enriched.append(signal)
                continue

            missing_entries += 1
            coin = normalize_cmm_coin(signal.get("coin")).upper()
            side = str(signal.get("side") or "").lower()
            try:
                payload = self.cmm_client.positions(
                    coin=coin,
                    segment_ids=self.cmm_signal_segments(),
                    limit=100,
                    start=entry_window_start,
                    end=entry_window_end,
                    open_only=True,
                )
            except CoinMarketManApiError as exc:
                errors.append(f"{coin} entries: {exc}")
                enriched.append(signal)
                continue

            entry_size = 0.0
            weighted_entry_value = 0.0
            position_count = 0
            seen_positions: set[str] = set()
            for position in self.cmm_position_rows(payload):
                if normalize_cmm_coin(position.get("coin")).upper() != coin:
                    continue
                if position.get("closeTime") not in (None, ""):
                    continue
                position_side = str(position.get("side") or "").lower()
                if position_side not in {"long", "short"}:
                    position_side = "long" if to_float(position.get("size")) > 0 else "short"
                if position_side != side:
                    continue
                entry_price = to_float(first_present(position, "entryPrice", "entryPx", "avgEntryPrice"))
                size = abs(to_float(position.get("size")))
                if entry_price <= 0 or size <= 0:
                    continue
                position_key = str(position.get("id") or position.get("positionId") or "")
                if not position_key:
                    position_key = f'{str(position.get("address") or "").lower()}:{coin}:{side}'
                if position_key in seen_positions:
                    continue
                seen_positions.add(position_key)
                entry_size += size
                weighted_entry_value += entry_price * size
                position_count += 1

            if entry_size > 0 and weighted_entry_value > 0:
                enriched.append(
                    {
                        **signal,
                        "price": round(weighted_entry_value / entry_size, 8),
                        "priceSource": "position-vwap-entry",
                        "entryCoveragePct": 100.0,
                        "entryPositionCount": position_count,
                    }
                )
            else:
                enriched.append(signal)

        return {**summary, "signals": enriched, "entryEnrichmentError": "; ".join(errors[:3])}

    def cmm_summary_cache_fresh(self, summary: dict[str, Any]) -> bool:
        ttl_ms = self.cmm_cache_ttl_ms()
        if ttl_ms <= 0 or not isinstance(summary, dict) or not summary.get("enabled"):
            return False
        generated_ms = iso_to_ms(summary.get("generatedAt"))
        if generated_ms <= 0:
            return False
        return current_time_ms() - generated_ms < ttl_ms

    def cmm_summary_rate_limited(self, summary: dict[str, Any]) -> bool:
        if not isinstance(summary, dict):
            return False
        return iso_to_ms(summary.get("rateLimitedUntil")) > current_time_ms()

    def build_cached_cmm_signal_summary(self, state: dict[str, Any] | None, *, force: bool = False) -> dict[str, Any]:
        cached = state.get("cmmSignals", {}) if isinstance(state, dict) else {}
        cached = cached if isinstance(cached, dict) else {}
        if not force and self.cmm_summary_rate_limited(cached):
            return {
                **cached,
                "cacheHit": True,
                "stale": True,
                "checkedAt": now_iso(),
            }
        if not force and self.cmm_summary_cache_fresh(cached):
            return {**cached, "cacheHit": True, "checkedAt": now_iso()}

        summary = self.build_cmm_signal_summary()
        if summary.get("rateLimited"):
            retry_at = datetime.fromtimestamp(
                (current_time_ms() + self.cmm_rate_limit_backoff_ms()) / 1000,
                timezone.utc,
            ).isoformat().replace("+00:00", "Z")
            if not cached.get("enabled") or not cached.get("signals"):
                return {**summary, "rateLimitedUntil": retry_at}
            return {
                **cached,
                "stale": True,
                "staleReason": summary.get("error", "CMM API rate limited"),
                "rateLimited": True,
                "rateLimitedUntil": retry_at,
                "checkedAt": summary.get("generatedAt", now_iso()),
            }
        return summary

    def cmm_signal_key(self, signal: dict[str, Any]) -> str:
        return f'cmm:{signal.get("coin", "Unknown")}:{signal.get("side", "")}'

    def cmm_signal_alert_eligible(self, signal: dict[str, Any]) -> bool:
        if signal.get("alertEligible"):
            return True
        return (
            to_float(signal.get("probabilityScore")) >= CMM_ALERT_PROBABILITY_THRESHOLD
            and to_float(signal.get("totalValue")) >= CMM_ACTIONABLE_MIN_TOTAL_VALUE
        )

    def summarize_cmm_signal_changes(
        self,
        previous: dict[str, Any],
        current: dict[str, Any],
    ) -> dict[str, list[dict[str, Any]]]:
        previous_signals = {
            self.cmm_signal_key(item): item
            for item in previous.get("signals", [])
            if isinstance(item, dict) and self.cmm_signal_alert_eligible(item)
        }
        current_signals = {
            self.cmm_signal_key(item): item
            for item in current.get("signals", [])
            if isinstance(item, dict) and self.cmm_signal_alert_eligible(item)
        }
        added = [current_signals[key] for key in current_signals.keys() - previous_signals.keys()]
        changed: list[dict[str, Any]] = []
        for key in current_signals.keys() & previous_signals.keys():
            old_item = previous_signals[key]
            new_item = current_signals[key]
            probability_delta = abs(
                to_float(new_item.get("probabilityScore")) - to_float(old_item.get("probabilityScore"))
            )
            if probability_delta >= SIGNAL_CONVICTION_ALERT_MIN_DELTA:
                changed.append(new_item)
        return {"addedCmmSignals": added, "changedCmmSignals": changed}

    def combined_wallet_cmm_probability(self, wallet_signal: dict[str, Any], cmm_signal: dict[str, Any]) -> float:
        wallet_score = to_float(wallet_signal.get("probabilityScore", wallet_signal.get("convictionScore")))
        cmm_score = to_float(cmm_signal.get("probabilityScore"))
        trend_score = to_float(cmm_signal.get("trendScore"))
        contrarian_score = max(0.0, to_float(cmm_signal.get("contrarianScore")))
        trend_available = bool(cmm_signal.get("trendAvailable", cmm_signal.get("trendScore") is not None))
        if not trend_available:
            return round(
                clamp(
                    (wallet_score * (0.45 / 0.85))
                    + (cmm_score * (0.30 / 0.85))
                    + (contrarian_score * (0.10 / 0.85))
                ),
                1,
            )
        return round(
            clamp(
                (wallet_score * 0.45)
                + (cmm_score * 0.30)
                + (trend_score * 0.15)
                + (contrarian_score * 0.10)
            ),
            1,
        )

    @staticmethod
    def is_wallet_native_signal(signal: dict[str, Any]) -> bool:
        return (
            int(to_float(signal.get("independentWalletCount"))) >= ACTIONABLE_SIGNAL_MIN_INDEPENDENT_WALLETS
            and int(to_float(signal.get("netIndependentWalletCount"))) >= ACTIONABLE_SIGNAL_MIN_INDEPENDENT_NET_WALLETS
            and int(to_float(signal.get("verifiedFreshIndependentWalletCount"))) >= ACTIONABLE_SIGNAL_MIN_VERIFIED_FRESH_WALLETS
            and int(to_float(signal.get("netFreshIndependentWalletCount"))) >= ACTIONABLE_SIGNAL_MIN_FRESH_NET_WALLETS
            and int(to_float(signal.get("oppositeVerifiedFreshIndependentWalletCount")))
            <= ACTIONABLE_SIGNAL_MAX_OPPOSITE_FRESH_WALLETS
            and int(to_float(signal.get("independentTopWalletCount"))) >= ACTIONABLE_SIGNAL_MIN_TOP_WALLETS
        )

    def apply_cmm_confirmation_to_summary(
        self,
        summary: dict[str, Any],
        cmm_summary: dict[str, Any],
        *,
        require_confirmation: bool = False,
    ) -> dict[str, Any]:
        if not cmm_summary.get("enabled"):
            return summary
        cmm_by_key = {
            self.cmm_signal_key(signal): signal
            for signal in cmm_summary.get("signals", [])
            if isinstance(signal, dict)
        }
        adjusted_signals: list[dict[str, Any]] = []
        vetoed_signals: list[dict[str, Any]] = []
        for signal in summary.get("signals", []):
            if not isinstance(signal, dict):
                continue
            coin = str(signal.get("coin") or "Unknown")
            side = str(signal.get("side") or "").lower()
            opposite_side = "short" if side == "long" else "long"
            same_cmm = cmm_by_key.get(f"cmm:{coin}:{side}")
            opposite_cmm = cmm_by_key.get(f"cmm:{coin}:{opposite_side}")
            original_probability = to_float(signal.get("probabilityScore", signal.get("convictionScore")))
            wallet_native = self.is_wallet_native_signal(signal)

            if same_cmm and to_float(same_cmm.get("probabilityScore")) >= CMM_SIGNAL_PROBABILITY_THRESHOLD:
                combined_probability = max(
                    original_probability,
                    self.combined_wallet_cmm_probability(signal, same_cmm),
                )
                adjusted_signals.append(
                    {
                        **signal,
                        "originalProbabilityScore": round(original_probability, 1),
                        "probabilityScore": combined_probability,
                        "cmmConfirmation": "confirmed",
                        "cmmProbabilityScore": to_float(same_cmm.get("probabilityScore")),
                        "cmmTrendScore": to_float(same_cmm.get("trendScore")),
                        "cmmContrarianScore": to_float(same_cmm.get("contrarianScore")),
                        "cmmCohortCount": int(to_float(same_cmm.get("cohortCount"))),
                    }
                )
                continue

            if opposite_cmm and to_float(opposite_cmm.get("probabilityScore")) >= CMM_SIGNAL_PROBABILITY_THRESHOLD:
                vetoed_signals.append(
                    {
                        **signal,
                        "invalidationReason": "cmm_conflict",
                        "cmmConflictProbabilityScore": to_float(opposite_cmm.get("probabilityScore")),
                    }
                )
                continue

            if not wallet_native:
                continue
            adjusted_signals.append(
                {
                    **signal,
                    "originalProbabilityScore": round(original_probability, 1),
                    "probabilityScore": round(original_probability, 1),
                    "cmmConfirmation": "unconfirmed",
                }
            )

        adjusted_signals.sort(
            key=lambda item: (
                -to_float(item.get("probabilityScore")),
                -to_float(item.get("netWeightedWalletCount")),
                -int(to_float(item.get("walletCount"))),
                str(item.get("coin")),
                str(item.get("side")),
            )
        )
        return {
            **summary,
            "signals": adjusted_signals,
            "signalCount": len(adjusted_signals),
            "vetoedSignals": vetoed_signals,
        }

    def apply_signal_lifecycle(
        self,
        summary: dict[str, Any],
        previous: dict[str, Any],
        *,
        now_ms: int,
    ) -> dict[str, Any]:
        if not summary.get("signals") and not previous.get("signals"):
            return summary
        previous_signals = {
            self.signal_key(item): item
            for item in previous.get("signals", [])
            if isinstance(item, dict)
        }
        current_signals = {
            self.signal_key(item): item
            for item in summary.get("signals", [])
            if isinstance(item, dict)
        }
        consensus = {
            self.signal_key(item): item
            for item in summary.get("consensus", [])
            if isinstance(item, dict)
        }
        vetoed = {
            self.signal_key(item): item
            for item in summary.get("vetoedSignals", [])
            if isinstance(item, dict)
        }
        active: list[dict[str, Any]] = []
        invalidated: list[dict[str, Any]] = []

        for key, signal in current_signals.items():
            prior = previous_signals.get(key)
            fresh_addresses = sorted({str(address).lower() for address in signal.get("freshWalletAddresses", [])})
            latest_fresh_at = int(to_float(signal.get("freshAddLatestTime"))) or now_ms
            if prior:
                prior_addresses = {
                    str(address).lower() for address in prior.get("freshWalletAddresses", [])
                }
                status = "CONFIRMED" if set(fresh_addresses) - prior_addresses else "ACTIVE"
                first_seen_at = int(to_float(prior.get("firstSeenAt"))) or now_ms
                latest_fresh_at = max(latest_fresh_at, int(to_float(prior.get("lastFreshAt"))))
            else:
                status = "NEW"
                first_seen_at = latest_fresh_at
            active.append(
                {
                    **signal,
                    "status": status,
                    "firstSeenAt": first_seen_at,
                    "lastFreshAt": latest_fresh_at,
                    "freshWalletAddresses": fresh_addresses,
                }
            )

        for key, prior in previous_signals.items():
            if key in current_signals:
                continue
            last_fresh_at = int(to_float(prior.get("lastFreshAt", prior.get("freshAddLatestTime"))))
            consensus_item = consensus.get(key)
            reason = ""
            if key in vetoed:
                reason = "cmm_conflict"
            elif consensus_item and int(
                to_float(consensus_item.get("oppositeVerifiedFreshIndependentWalletCount"))
            ) > ACTIONABLE_SIGNAL_MAX_OPPOSITE_FRESH_WALLETS:
                reason = "opposite_fresh_flow"
            elif consensus_item and int(to_float(consensus_item.get("netIndependentWalletCount"))) < 2:
                reason = "consensus_lost"
            elif not consensus_item:
                reason = "consensus_lost"
            elif last_fresh_at <= 0 or now_ms - last_fresh_at >= SIGNAL_LIFETIME_MS:
                reason = "expired"

            if reason:
                invalidated.append({**prior, "status": "INVALIDATED", "invalidationReason": reason})
            else:
                active.append({**prior, "status": "ACTIVE"})

        active.sort(
            key=lambda item: (
                -to_float(item.get("probabilityScore")),
                -int(to_float(item.get("verifiedFreshIndependentWalletCount"))),
                str(item.get("coin")),
            )
        )
        return {
            **summary,
            "signals": active,
            "signalCount": len(active),
            "invalidatedSignals": invalidated,
        }

    def update_signal_outcomes(
        self,
        previous: dict[str, Any],
        summary: dict[str, Any],
        *,
        now_ms: int,
    ) -> dict[str, Any]:
        records = {
            str(key): dict(value)
            for key, value in (previous.items() if isinstance(previous, dict) else [])
            if isinstance(value, dict)
            and now_ms - int(to_float(value.get("startedAt"))) <= SIGNAL_OUTCOME_RETENTION_MS
        }
        marks = {
            self.signal_key(item): to_float(item.get("markPrice"))
            for item in summary.get("consensus", [])
            if isinstance(item, dict) and to_float(item.get("markPrice")) > 0
        }

        for signal in summary.get("signals", []):
            if not isinstance(signal, dict) or signal.get("status") != "NEW":
                continue
            started_at = int(to_float(signal.get("firstSeenAt"))) or now_ms
            entry_price = to_float(signal.get("freshAddVwap"))
            if entry_price <= 0:
                continue
            record_key = f'{self.signal_key(signal)}:{started_at}'
            records.setdefault(
                record_key,
                {
                    "coin": signal.get("coin", "Unknown"),
                    "side": signal.get("side", ""),
                    "startedAt": started_at,
                    "entryPrice": round(entry_price, 8),
                    "probabilityScore": round(to_float(signal.get("probabilityScore")), 1),
                    "freshWalletCount": int(to_float(signal.get("verifiedFreshIndependentWalletCount"))),
                    "outcomes": {},
                },
            )

        for record in records.values():
            started_at = int(to_float(record.get("startedAt")))
            entry_price = to_float(record.get("entryPrice"))
            mark_price = marks.get(self.signal_key(record), 0.0)
            if started_at <= 0 or entry_price <= 0 or mark_price <= 0:
                continue
            outcomes = record.setdefault("outcomes", {})
            direction = 1.0 if str(record.get("side") or "").lower() == "long" else -1.0
            for label, horizon_ms in SIGNAL_OUTCOME_HORIZONS_MS.items():
                if label in outcomes or now_ms - started_at < horizon_ms:
                    continue
                outcomes[label] = {
                    "markPrice": round(mark_price, 8),
                    "returnPct": round(((mark_price / entry_price) - 1.0) * 100.0 * direction, 3),
                    "measuredAt": now_ms,
                }
        return records

    def cmm_asset_group(self, coin: Any) -> str:
        normalized_coin = normalize_cmm_coin(coin)
        if is_commodity_like_position(normalized_coin):
            return "Commodities"
        if is_stock_like_position(normalized_coin):
            return "Stocks / indices"
        return "Crypto"

    def cmm_tracked_confirmation_note(self, item: dict[str, Any], wallet_summary: dict[str, Any] | None) -> str:
        if not wallet_summary:
            return ""
        coin = normalize_cmm_coin(item.get("coin")).upper()
        side = str(item.get("side") or "").lower()
        for source_key in ("signals", "consensus"):
            for wallet_item in wallet_summary.get(source_key, []):
                if not isinstance(wallet_item, dict):
                    continue
                if normalize_cmm_coin(wallet_item.get("coin")).upper() != coin:
                    continue
                if str(wallet_item.get("side") or "").lower() != side:
                    continue
                wallet_count = int(to_float(wallet_item.get("walletCount")))
                if wallet_count <= 0:
                    continue
                qnet = to_float(wallet_item.get("netWeightedWalletCount"))
                if qnet > 0:
                    return f", tracked {wallet_count}w qnet {qnet:.1f}"
                return f", tracked {wallet_count}w"
        return ""

    def build_cmm_signals_message(
        self,
        cmm_summary: dict[str, Any] | None = None,
        *,
        wallet_summary: dict[str, Any] | None = None,
        limit: int = 10,
    ) -> str:
        summary = cmm_summary or self.build_cmm_signal_summary()
        lines = [
            "CMM cohort signals",
            f'Timeframe: {summary.get("timeframe", os.environ.get("CMM_SIGNAL_POSITION_RECENCY", "7d"))}',
            (
                f"Watch/action/alert: {CMM_WATCH_PROBABILITY_THRESHOLD:.0f}/"
                f"{CMM_SIGNAL_PROBABILITY_THRESHOLD:.0f}/{CMM_ALERT_PROBABILITY_THRESHOLD:.0f}"
            ),
            (
                f"Min gross cohort exposure: watch {format_money_compact(CMM_SIGNAL_MIN_TOTAL_VALUE)}, "
                f"action {format_money_compact(CMM_ACTIONABLE_MIN_TOTAL_VALUE)}"
            ),
        ]
        if not summary.get("enabled"):
            lines.append(f'- Disabled: {summary.get("error", "missing API token")}')
        else:
            if summary.get("stale"):
                lines.append(f'- Showing cached CMM data: {summary.get("staleReason", "live CMM unavailable")}')
        if summary.get("enabled") and summary.get("signals"):
            shown = summary.get("signals", [])[: max(1, limit)]
            by_group: dict[str, list[dict[str, Any]]] = {"Crypto": [], "Commodities": [], "Stocks / indices": []}
            for item in shown:
                if isinstance(item, dict):
                    by_group.setdefault(self.cmm_asset_group(item.get("coin")), []).append(item)
            index = 1
            for group in ("Crypto", "Commodities", "Stocks / indices"):
                group_items = by_group.get(group, [])
                if not group_items:
                    continue
                lines.append("")
                lines.append(f"{group}:")
                for item in group_items:
                    cohorts = "/".join(str(component.get("segment")) for component in item.get("components", [])[:3])
                    bias_pct = abs(to_float(item.get("valueBias"))) * 100
                    freshness_note = ""
                    if item.get("dataFreshness"):
                        freshness_note = f', {item.get("dataFreshness")} {to_float(item.get("metricLagMinutes")):.0f}m'
                    tracked_note = self.cmm_tracked_confirmation_note(item, wallet_summary)
                    price_note = ""
                    if to_float(item.get("price")) > 0:
                        source = "position VWAP entry" if item.get("priceSource") == "position-vwap-entry" else (
                            "aggregate implied entry" if item.get("priceSource") == "cohort-implied-entry" else "aggregate entry"
                        )
                        coverage = to_float(item.get("entryCoveragePct"))
                        coverage_note = f' ({coverage:.0f}% size)' if 0 < coverage < 99.5 else ""
                        price_note = f', {source} ${format_price(to_float(item.get("price")))}{coverage_note}'
                    trend_note = (
                        f'trend {to_float(item.get("trendScore")):.0f}'
                        if item.get("trendAvailable", item.get("trendScore") is not None)
                        else "trend n/a"
                    )
                    lines.append(
                        f'{index}. {str(item.get("signalTier", "watch")).upper()} '
                        f'{str(item.get("action", "watch")).upper()} {item["coin"]} {item["side"]} '
                        f'(p{to_float(item.get("probabilityScore")):.0f}/100, {item.get("cohortCount", 0)} cohorts, '
                        f'bias {bias_pct:.0f}%, {trend_note}, '
                        f'contra {to_float(item.get("contrarianScore")):.0f}, '
                        f'gross {format_money_compact(item.get("totalValue"))}{price_note}, {cohorts}{tracked_note}{freshness_note})'
                    )
                    index += 1
        elif summary.get("enabled"):
            lines.append("- No CMM watch candidates above threshold")
            below_threshold = summary.get("belowThresholdSignals", [])
            if below_threshold:
                lines.append("")
                lines.append("Strongest below watch:")
                for item in below_threshold[:5]:
                    price_note = ""
                    if to_float(item.get("price")) > 0:
                        source = "position VWAP entry" if item.get("priceSource") == "position-vwap-entry" else (
                            "aggregate implied entry" if item.get("priceSource") == "cohort-implied-entry" else "aggregate entry"
                        )
                        coverage = to_float(item.get("entryCoveragePct"))
                        coverage_note = f' ({coverage:.0f}% size)' if 0 < coverage < 99.5 else ""
                        price_note = f', {source} ${format_price(to_float(item.get("price")))}{coverage_note}'
                    trend_note = (
                        f'trend {to_float(item.get("trendScore")):.0f}'
                        if item.get("trendAvailable", item.get("trendScore") is not None)
                        else "trend n/a"
                    )
                    lines.append(
                        f'- {str(item.get("action", "watch")).upper()} {item["coin"]} {item["side"]} '
                        f'p{to_float(item.get("probabilityScore")):.0f}, '
                        f'bias {abs(to_float(item.get("valueBias"))) * 100:.0f}%, '
                        f'{trend_note}, gross {format_money_compact(item.get("totalValue"))}{price_note}'
                    )
            diagnostics = summary.get("diagnostics", {})
            if diagnostics:
                lines.append("")
                lines.append(
                    "Diagnostics: "
                    f'rows {int(to_float(diagnostics.get("heatmapRows")))}, '
                    f'smart {int(to_float(diagnostics.get("smartComponents")))}, '
                    f'contra {int(to_float(diagnostics.get("contrarianComponents")))}, '
                    f'candidates {int(to_float(diagnostics.get("scoredCandidates")))}, '
                    f'low value {int(to_float(diagnostics.get("lowValueCandidates")))}'
                )
        if summary.get("error") and summary.get("enabled"):
            lines.append(f'Partial error: {summary.get("error")}')
        if summary.get("entryEnrichmentError"):
            lines.append(f'Entry enrichment unavailable: {summary.get("entryEnrichmentError")}')
        lines.append("")
        lines.append(f'Checked at: {summary.get("generatedAt", now_iso())}')
        return "\n".join(lines)

    def build_signals_message(
        self,
        summary: dict[str, Any],
        *,
        title: str = "Actionable wallet signals",
        cmm_summary: dict[str, Any] | None = None,
    ) -> str:
        signals = summary.get("signals", [])
        lines = [title, f'Threshold: {ACTIONABLE_SIGNAL_PROBABILITY_THRESHOLD:.0f}/100 probability']
        if signals:
            for index, item in enumerate(signals[:20], start=1):
                probability = to_float(item.get("probabilityScore", item.get("convictionScore")))
                net_note = f', net +{int(to_float(item.get("netWalletCount")))}' if "netWalletCount" in item else ""
                if "netWeightedWalletCount" in item:
                    net_note += f', qnet +{to_float(item.get("netWeightedWalletCount")):.1f}'
                activity_bits = []
                if int(to_float(item.get("freshActivityWalletCount"))):
                    activity_bits.append(f'{int(to_float(item.get("freshActivityWalletCount")))} 5m activity')
                if int(to_float(item.get("recentAddWalletCount"))):
                    activity_bits.append(f'{int(to_float(item.get("recentAddWalletCount")))} adds')
                if int(to_float(item.get("verifiedFreshIndependentWalletCount"))):
                    activity_bits.append(
                        f'{int(to_float(item.get("verifiedFreshIndependentWalletCount")))} verified 15m'
                    )
                activity_note = f', {"/".join(activity_bits)}' if activity_bits else ""
                cmm_note = ""
                if item.get("cmmConfirmation") == "confirmed":
                    cmm_note = f', CMM p{to_float(item.get("cmmProbabilityScore")):.0f}'
                elif item.get("cmmConfirmation") == "conflict":
                    cmm_note = f', CMM conflict p{to_float(item.get("cmmConflictProbabilityScore")):.0f}'
                elif item.get("cmmConfirmation") == "unconfirmed":
                    cmm_note = ", CMM unconfirmed"
                price_note = ""
                if to_float(item.get("freshAddVwap")) > 0:
                    price_note = (
                        f', fresh VWAP ${format_price(to_float(item.get("freshAddVwap")))} '
                        f'live ~${format_price(to_float(item.get("markPrice")))} '
                        f'dist {to_float(item.get("entryDistancePct")):+.2f}%'
                    )
                fresh_note = ""
                if "netFreshIndependentWalletCount" in item:
                    fresh_note = (
                        f', fresh +{int(to_float(item.get("netFreshIndependentWalletCount")))}'
                        f'/-{int(to_float(item.get("oppositeVerifiedFreshIndependentWalletCount")))}'
                    )
                lines.append(
                    f'{index}. {str(item.get("status") or item.get("action", "watch")).upper()} '
                    f'{item["coin"]} {item["side"]} '
                    f'({item["walletCount"]} wallets{net_note}{fresh_note}, '
                    f'p{probability:.0f}/100{activity_note}{price_note}{cmm_note})'
                )
        else:
            lines.append("- No actionable signals right now")
        if cmm_summary is not None:
            lines.append("")
            lines.append(self.build_cmm_signals_message(cmm_summary, wallet_summary=summary))
        lines.append("")
        lines.append(f'Checked at: {summary.get("generatedAt", now_iso())}')
        return "\n".join(lines)

    def build_positions_message(self, dashboard: dict[str, Any], *, title: str = "Open positions now") -> str:
        lines = [title]
        position_groups = self.build_position_groups(
            dashboard,
            hip3_only=False,
            stock_like_only=False,
            commodity_like_only=False,
        )
        commodity_groups = self.build_position_groups(
            dashboard,
            hip3_only=False,
            stock_like_only=False,
            commodity_like_only=True,
        )
        stock_groups = self.build_position_groups(dashboard, hip3_only=False, stock_like_only=True)
        total_positions = sum(item["positionCount"] for item in position_groups + commodity_groups + stock_groups)

        if not position_groups and not commodity_groups and not stock_groups:
            lines.append("")
            lines.append("- No open positions")
        else:
            sections = [
                (
                    f"By wallet count ({MIN_POSITION_MESSAGE_WALLETS}+ wallets, "
                    f"{format_money_compact(MIN_POSITION_MESSAGE_VALUE)}+):",
                    position_groups,
                ),
                ("Commodities:", commodity_groups),
                ("Stocks / indices:", stock_groups),
            ]
            for heading, groups in sections:
                lines.append("")
                lines.append(heading)
                if groups:
                    for item in groups[:50]:
                        entry_note = ""
                        if to_float(item.get("entryPx")) > 0:
                            entry_label = "size-w entry" if item.get("entryType") == "size_weighted" else "avg entry"
                            entry_note = f', {entry_label} ${format_price(to_float(item.get("entryPx")))}'
                        recent_add_note = ""
                        if to_float(item.get("recentAddPx")) > 0:
                            recent_add_note = (
                                f', recent add VWAP ${format_price(to_float(item.get("recentAddPx")))} '
                                f'({int(to_float(item.get("recentAddWalletCount")))}w/7d)'
                            )
                        value_note = f', {format_money_thousands(to_float(item.get("totalValue")))}'
                        lines.append(
                            f'- {item["coin"]} {item["side"]} '
                            f'({item["walletCount"]} wallets, {item["positionCount"]} positions{value_note}{entry_note}{recent_add_note})'
                        )
                else:
                    lines.append("- None")

        lines.append("")
        lines.append(f"Position groups: {len(position_groups) + len(commodity_groups) + len(stock_groups)}")
        lines.append(f"Open positions: {total_positions}")
        lines.append(f'Checked at: {dashboard.get("generatedAt", now_iso())}')
        return "\n".join(lines)

    def build_wallet_rankings_message(self, dashboard: dict[str, Any], *, limit: int = 10) -> str:
        wallets = sorted(
            dashboard.get("wallets", []),
            key=lambda wallet: (
                to_float(wallet.get("recentWinRateRank", {}).get("score")),
                to_float(wallet.get("hitRate")),
                int(wallet.get("recentClosedTrades") or 0),
                to_float(wallet.get("recentRealizedPnl")),
            ),
            reverse=True,
        )

        lines = ["Wallet ranks by multi-period quality"]
        ranked_wallets = [
            wallet
            for wallet in wallets
            if str(wallet.get("recentWinRateRank", {}).get("label") or "Unranked") != "Unranked"
        ]
        if ranked_wallets:
            for index, wallet in enumerate(ranked_wallets[: max(1, limit)], start=1):
                rank = wallet.get("recentWinRateRank", {})
                lines.append(
                    f'{index}. {wallet_label(wallet.get("alias", ""), wallet.get("address", ""))}: '
                    f'{rank.get("label", "Unranked")} '
                    f'({to_float(rank.get("winRate")):.1f}% 7D WR, {int(rank.get("sampleSize") or 0)} 7D closes, '
                    f'{int(rank.get("sampleSize30d") or 0)} 30D closes, 30D PnL ${to_float(rank.get("pnl30d")):,.0f}, '
                    f'PF {rank.get("profitFactor", 0)}, DD {to_float(rank.get("maxDrawdownPct")):.1f}%, '
                    f'score {to_float(rank.get("score")):.1f}/100)'
                )
        else:
            lines.append("- Not enough recent closed trades yet")

        lines.append("")
        lines.append(f'Checked at: {dashboard.get("generatedAt", now_iso())}')
        return "\n".join(lines)

    def build_elite_wallet_positions_message(self, dashboard: dict[str, Any]) -> str:
        elite_wallets = sorted(
            [
                wallet
                for wallet in dashboard.get("wallets", [])
                if str(wallet.get("recentWinRateRank", {}).get("label") or "") == "Elite"
            ],
            key=lambda wallet: (
                to_float(wallet.get("recentWinRateRank", {}).get("score")),
                abs(to_float(wallet.get("totalNotional"))),
                to_float(wallet.get("accountValue")),
            ),
            reverse=True,
        )

        lines = ["Elite wallet positions"]
        if not elite_wallets:
            lines.append("- No Elite-ranked wallets right now")
            lines.append("")
            lines.append(f'Checked at: {dashboard.get("generatedAt", now_iso())}')
            return "\n".join(lines)

        for wallet in elite_wallets:
            rank = wallet.get("recentWinRateRank", {})
            positions = sorted(
                [
                    position
                    for position in wallet.get("positions", [])
                    if should_count_open_position(wallet.get("address"), position.get("coin"), position)
                ],
                key=lambda item: abs(to_float(item.get("positionValue"))),
                reverse=True,
            )
            lines.append("")
            lines.append(
                f'{wallet_label(wallet.get("alias", ""), wallet.get("address", ""))} '
                f'({to_float(rank.get("score")):.1f}/100, {to_float(rank.get("winRate")):.1f}% 7D WR, '
                f'{int(rank.get("sampleSize30d") or 0)} 30D closes, PF {rank.get("profitFactor", 0)}, '
                f'DD {to_float(rank.get("maxDrawdownPct")):.1f}%)'
            )
            if not positions:
                lines.append("- No open positions")
                continue
            for position in positions:
                size_note = ""
                if to_float(position.get("size")):
                    size_note = f', size {format_position_size(abs(to_float(position.get("size"))))}'
                entry_note = ""
                if to_float(position.get("entryPx")) > 0:
                    entry_note = f', entry ${format_price(to_float(position.get("entryPx")))}'
                pnl_note = ""
                if to_float(position.get("unrealizedPnl")):
                    pnl_note = f', uPnL ${to_float(position.get("unrealizedPnl")):,.0f}'
                lines.append(
                    f'- {position.get("coin", "Unknown")} {str(position.get("side", "")).lower()} '
                    f'{format_money_thousands(to_float(position.get("positionValue")))}{size_note}{entry_note}{pnl_note}'
                )

        lines.append("")
        lines.append(f"Elite wallets: {len(elite_wallets)}")
        lines.append(f'Checked at: {dashboard.get("generatedAt", now_iso())}')
        return "\n".join(lines)

    def build_position_wallets_message(self, dashboard: dict[str, Any], coin: str, side: str) -> str:
        normalized_coin = normalize_position_coin(coin.upper())
        normalized_side = str(side or "").lower()
        if normalized_side not in {"long", "short"}:
            return "Use /ticker long or /ticker short"

        wallets: dict[str, dict[str, Any]] = {}
        for wallet in dashboard.get("wallets", []):
            address = str(wallet.get("address") or "")
            for position in wallet.get("positions", []):
                position_coin = normalize_position_coin(position.get("coin"))
                position_side = str(position.get("side") or "").lower()
                if position_coin.upper() != normalized_coin.upper() or position_side != normalized_side:
                    continue
                if not should_count_open_position(address, position_coin, position):
                    continue
                bucket = wallets.setdefault(
                    address,
                    {
                        "address": address,
                        "alias": wallet.get("alias", ""),
                        "positionCount": 0,
                        "totalValue": 0.0,
                        "totalSize": 0.0,
                        "entryValue": 0.0,
                        "entrySum": 0.0,
                        "entryCount": 0,
                        "unrealizedPnl": 0.0,
                    },
                )
                position_value = to_float(position.get("positionValue"))
                size = abs(to_float(position.get("size")))
                entry_px = to_float(position.get("entryPx"))
                bucket["positionCount"] += 1
                bucket["totalValue"] += position_value
                bucket["totalSize"] += size
                bucket["entryValue"] += entry_px * size
                bucket["unrealizedPnl"] += to_float(position.get("unrealizedPnl"))
                if entry_px > 0:
                    bucket["entrySum"] += entry_px
                    bucket["entryCount"] += 1

        rows = sorted(wallets.values(), key=lambda item: item["totalValue"], reverse=True)
        total_value = sum(to_float(item.get("totalValue")) for item in rows)
        total_size = sum(to_float(item.get("totalSize")) for item in rows)
        entry_value = sum(to_float(item.get("entryValue")) for item in rows)
        entry_sum = sum(to_float(item.get("entrySum")) for item in rows)
        entry_count = sum(int(to_float(item.get("entryCount"))) for item in rows)
        position_count = sum(int(to_float(item.get("positionCount"))) for item in rows)

        entry_note = ""
        if total_size > 0:
            entry_note = f", size-w entry ${format_price(entry_value / total_size)}"
        elif entry_count > 0:
            entry_note = f", avg entry ${format_price(entry_sum / entry_count)}"

        lines = [
            f"{normalized_coin} {normalized_side} wallets",
            f"Wallets: {len(rows)} | Positions: {position_count} | Total: {format_money_thousands(total_value)}{entry_note}",
        ]

        if not rows:
            lines.append(f"- No {normalized_coin} {normalized_side} positions")
        else:
            for index, item in enumerate(rows[:50], start=1):
                size_note = ""
                if to_float(item.get("totalSize")) > 0:
                    size_note = f', size {format_position_size(to_float(item.get("totalSize")))}'
                item_entry_note = ""
                if to_float(item.get("totalSize")) > 0 and to_float(item.get("entryValue")) > 0:
                    item_entry_note = f', entry ${format_price(to_float(item.get("entryValue")) / to_float(item.get("totalSize")))}'
                elif int(to_float(item.get("entryCount"))) > 0:
                    item_entry_note = f', avg entry ${format_price(to_float(item.get("entrySum")) / int(to_float(item.get("entryCount"))))}'
                pnl_note = ""
                if to_float(item.get("unrealizedPnl")):
                    pnl_note = f', uPnL ${to_float(item.get("unrealizedPnl")):,.0f}'
                lines.append(
                    f'{index}. {item.get("address", "")}: '
                    f'{format_money_thousands(to_float(item.get("totalValue")))}{size_note}{item_entry_note}{pnl_note}'
                )

        lines.append("")
        lines.append(f'Checked at: {dashboard.get("generatedAt", now_iso())}')
        return "\n".join(lines)

    def build_hourly_update_message(self, dashboard: dict[str, Any], summary: dict[str, Any], min_wallets: int) -> str:
        return "\n\n".join(
            [
                self.build_summary_message(summary, min_wallets, title="Hourly wallet update", include_signals=False),
                self.build_positions_message(dashboard),
            ]
        )

    def split_message(self, message: str, limit: int = 3500) -> list[str]:
        if len(message) <= limit:
            return [message]

        chunks: list[str] = []
        current_lines: list[str] = []
        current_length = 0

        for line in message.splitlines():
            line_length = len(line) + 1
            if current_lines and current_length + line_length > limit:
                chunks.append("\n".join(current_lines))
                current_lines = [line]
                current_length = line_length
            else:
                current_lines.append(line)
                current_length += line_length

        if current_lines:
            chunks.append("\n".join(current_lines))

        return chunks

    def send_telegram_message(self, bot_token: str, chat_id: str, message: str) -> None:
        for chunk in self.split_message(message):
            payload = urllib.parse.urlencode({"chat_id": chat_id, "text": chunk}).encode("utf-8")
            request = urllib.request.Request(
                f"https://api.telegram.org/bot{bot_token}/sendMessage",
                data=payload,
                headers={"Content-Type": "application/x-www-form-urlencoded"},
            )
            with urllib.request.urlopen(request, timeout=20):
                continue

    def fetch_telegram_updates(self, bot_token: str, offset: int = 0) -> list[dict[str, Any]]:
        query = urllib.parse.urlencode({"offset": offset})
        request = urllib.request.Request(
            f"https://api.telegram.org/bot{bot_token}/getUpdates?{query}",
            headers={"Accept": "application/json"},
        )
        with urllib.request.urlopen(request, timeout=20) as response:
            payload = json.load(response)
        if not isinstance(payload, dict) or not payload.get("ok"):
            return []
        result = payload.get("result", [])
        return result if isinstance(result, list) else []

    def check_alerts(self, send_notification: bool = True) -> dict[str, Any]:
        raw = load_json_file(self.alerts_path, {})
        stored_config = raw.get("config", {}) if isinstance(raw, dict) else {}
        config = self.resolve_alert_config(stored_config)
        state = raw.get("state", {}) if isinstance(raw, dict) else {}
        min_wallets = max(1, int(config.get("minConsensusWallets", DEFAULT_CONSENSUS_THRESHOLD)))

        dashboard = self.dashboard()
        position_lifecycle = self.build_position_lifecycle(dashboard, state.get("walletPositionLifecycle", {}))
        summary, top_cohort = self.build_monthly_sentiment_summary(
            dashboard,
            min_wallets,
            state,
            position_lifecycle=position_lifecycle,
        )
        cmm_summary = self.build_cached_cmm_signal_summary(state)
        previous_summary = state.get("summary", {}) if isinstance(state, dict) else {}
        alert_summary = self.apply_cmm_confirmation_to_summary(
            summary,
            cmm_summary,
            require_confirmation=True,
        )
        dedupe_now_ms = current_time_ms()
        alert_summary = self.apply_signal_lifecycle(
            alert_summary,
            previous_summary,
            now_ms=dedupe_now_ms,
        )
        signal_outcomes = self.update_signal_outcomes(
            state.get("signalOutcomes", {}),
            alert_summary,
            now_ms=dedupe_now_ms,
        )
        previous_positions = state.get("largePositions", {}) if isinstance(state, dict) else {}
        previous_dedupe = state.get("alertDedupe", {}) if isinstance(state, dict) else {}
        current_positions = self.build_large_position_snapshot(dashboard)
        tracked_addresses = {
            str(wallet.get("address") or "").lower()
            for wallet in dashboard.get("wallets", [])
            if str(wallet.get("address") or "").strip()
        }
        previous_positions = self.filter_positions_to_tracked_wallets(previous_positions, tracked_addresses)
        fill_prices = self.build_recent_fill_price_map(dashboard, since_ms=iso_to_ms(state.get("lastCheckedAt")))
        # Keep HIP-3 available for explicit commands like /hip3, but exclude it
        # from automatic Telegram alerts and change-trigger decisions.
        changes = self.summarize_changes(previous_summary, alert_summary, track_hip3=False)
        # CMM strengthens or vetoes a fresh wallet signal, but never creates a
        # Telegram trade alert by itself.  Its full candidate list remains in /cmm.
        changes.update({"addedCmmSignals": [], "changedCmmSignals": []})
        position_changes = self.build_large_position_alert_changes(
            previous_positions,
            current_positions,
            fill_prices,
            now_ms=dedupe_now_ms,
        )
        fresh_flow_positions = self.build_large_position_snapshot(
            dashboard,
            min_value=FRESH_WALLET_FLOW_MIN_VALUE,
        )
        clustered_open_positions = self.build_clustered_open_position_alerts(
            dashboard,
            fresh_flow_positions,
            now_ms=dedupe_now_ms,
        )
        changes["clusteredOpenPositions"] = clustered_open_positions
        changes["newLargePositions"] = position_changes["newLargePositions"]
        changes["increasedLargePositions"] = position_changes["increasedLargePositions"]
        changes["closedLargePositions"] = position_changes["closedLargePositions"]
        changes, suppressed_alert_keys = self.filter_deduped_alert_changes(
            changes,
            previous_dedupe,
            now_ms=dedupe_now_ms,
        )
        alert_event_keys = self.collect_alert_event_keys(changes)

        should_notify = any(
            [
                changes["addedSignals"],
                changes["changedSignals"],
                changes["removedSignals"],
                changes["addedCmmSignals"],
                changes["changedCmmSignals"],
                changes["clusteredOpenPositions"],
                changes["newLargePositions"],
                changes["increasedLargePositions"],
                changes["closedLargePositions"],
            ]
        )

        sent = False
        error_message = ""
        if send_notification and should_notify and config.get("enabled"):
            if not config.get("botToken") or not config.get("chatId"):
                error_message = "Missing Telegram bot token or chat id"
            else:
                try:
                    self.send_telegram_message(
                        str(config["botToken"]),
                        str(config["chatId"]),
                        self.build_telegram_message(changes, alert_summary, min_wallets),
                    )
                    sent = True
                except (urllib.error.URLError, TimeoutError, ValueError) as exc:
                    error_message = str(exc)

        if not send_notification:
            return {
                "enabled": bool(config.get("enabled")),
                "hasBotToken": bool(config.get("botToken")),
                "chatId": config.get("chatId", ""),
                "sent": sent,
                "shouldNotify": should_notify,
                "error": error_message,
                "suppressedAlertCount": len(suppressed_alert_keys),
                "changes": changes,
                "summary": alert_summary,
                "cmmSignals": cmm_summary,
            }

        checked_at = now_iso()
        new_state = {
            **state,
            "lastCheckedAt": checked_at,
            "lastSentAt": checked_at if sent else state.get("lastSentAt"),
            "topConvictionWallets": top_cohort,
            "walletPositionLifecycle": position_lifecycle,
            "signalOutcomes": signal_outcomes,
        }
        if not should_notify or sent or not config.get("enabled"):
            new_state["summary"] = alert_summary
            new_state["largePositions"] = current_positions
            new_state["cmmSignals"] = cmm_summary
        if sent:
            new_state["alertDedupe"] = self.update_alert_dedupe(
                previous_dedupe,
                alert_event_keys,
                now_ms=dedupe_now_ms,
            )
        save_json_file(self.alerts_path, {"config": stored_config, "state": new_state})

        return {
            "enabled": bool(config.get("enabled")),
            "hasBotToken": bool(config.get("botToken")),
            "chatId": config.get("chatId", ""),
            "sent": sent,
            "shouldNotify": should_notify,
            "error": error_message,
            "suppressedAlertCount": len(suppressed_alert_keys),
            "changes": changes,
            "summary": alert_summary,
            "cmmSignals": cmm_summary,
        }

    def send_hourly_update(self, min_wallets: int, bot_token: str, chat_id: str) -> dict[str, Any]:
        dashboard = self.dashboard()
        raw = load_json_file(self.alerts_path, {})
        stored_config = raw.get("config", {}) if isinstance(raw, dict) else {}
        config = self.resolve_alert_config(stored_config)
        state = raw.get("state", {}) if isinstance(raw, dict) else {}
        position_lifecycle = self.build_position_lifecycle(dashboard, state.get("walletPositionLifecycle", {}))
        summary, top_cohort = self.build_monthly_sentiment_summary(
            dashboard,
            min_wallets,
            state,
            position_lifecycle=position_lifecycle,
        )
        cmm_summary = self.build_cached_cmm_signal_summary(state)
        previous_summary = state.get("summary", {}) if isinstance(state, dict) else {}
        alert_summary = self.apply_cmm_confirmation_to_summary(
            summary,
            cmm_summary,
            require_confirmation=True,
        )
        lifecycle_now_ms = current_time_ms()
        alert_summary = self.apply_signal_lifecycle(
            alert_summary,
            previous_summary,
            now_ms=lifecycle_now_ms,
        )
        signal_outcomes = self.update_signal_outcomes(
            state.get("signalOutcomes", {}),
            alert_summary,
            now_ms=lifecycle_now_ms,
        )
        current_positions = self.build_large_position_snapshot(dashboard)
        self.send_telegram_message(
            bot_token,
            chat_id,
            self.build_hourly_update_message(dashboard, summary, min_wallets),
        )
        previous_positions = state.get("largePositions", {}) if isinstance(state, dict) else {}
        previous_dedupe = state.get("alertDedupe", {}) if isinstance(state, dict) else {}
        tracked_addresses = {
            str(wallet.get("address") or "").lower()
            for wallet in dashboard.get("wallets", [])
            if str(wallet.get("address") or "").strip()
        }
        previous_positions = self.filter_positions_to_tracked_wallets(previous_positions, tracked_addresses)
        fill_prices = self.build_recent_fill_price_map(dashboard, since_ms=iso_to_ms(state.get("lastCheckedAt")))
        dedupe_now_ms = current_time_ms()
        position_changes = self.build_large_position_alert_changes(
            previous_positions,
            current_positions,
            fill_prices,
            now_ms=dedupe_now_ms,
        )
        fresh_flow_positions = self.build_large_position_snapshot(
            dashboard,
            min_value=FRESH_WALLET_FLOW_MIN_VALUE,
        )
        position_changes["clusteredOpenPositions"] = self.build_clustered_open_position_alerts(
            dashboard,
            fresh_flow_positions,
            now_ms=dedupe_now_ms,
        )
        position_changes, suppressed_alert_keys = self.filter_deduped_alert_changes(
            position_changes,
            previous_dedupe,
            now_ms=dedupe_now_ms,
        )
        alert_event_keys = self.collect_alert_event_keys(position_changes)
        should_send_position_alert = any(
            [
                position_changes["clusteredOpenPositions"],
                position_changes["newLargePositions"],
                position_changes["increasedLargePositions"],
                position_changes["closedLargePositions"],
            ]
        )
        position_alert_sent = False
        position_alert_error = ""
        if should_send_position_alert and config.get("enabled"):
            try:
                self.send_telegram_message(
                    bot_token,
                    chat_id,
                    self.build_telegram_message(position_changes, summary, min_wallets),
                )
                position_alert_sent = True
            except (urllib.error.URLError, TimeoutError, ValueError) as exc:
                position_alert_error = str(exc)

        synced_at = now_iso()
        new_state = {
            **state,
            "summary": alert_summary,
            "lastCheckedAt": synced_at,
            "lastHourlySyncedAt": synced_at,
            "topConvictionWallets": top_cohort,
            "walletPositionLifecycle": position_lifecycle,
            "signalOutcomes": signal_outcomes,
            "cmmSignals": cmm_summary,
        }
        if not should_send_position_alert or position_alert_sent or not config.get("enabled"):
            new_state["largePositions"] = current_positions
        if position_alert_sent:
            new_state["lastSentAt"] = synced_at
            new_state["alertDedupe"] = self.update_alert_dedupe(
                previous_dedupe,
                alert_event_keys,
                now_ms=dedupe_now_ms,
            )
        save_json_file(self.alerts_path, {"config": stored_config, "state": new_state})
        return {
            "sent": True,
            "positionAlertSent": position_alert_sent,
            "positionAlertError": position_alert_error,
            "suppressedAlertCount": len(suppressed_alert_keys),
            "summary": alert_summary,
        }


class AlertWorker:
    def __init__(self, service: WalletTrackerService, interval_seconds: int) -> None:
        self.service = service
        self.interval_seconds = max(60, interval_seconds)
        self._stop_event = threading.Event()
        self._thread: threading.Thread | None = None

    def start(self) -> None:
        if self._thread and self._thread.is_alive():
            return
        self._thread = threading.Thread(target=self._run, daemon=True, name="alert-worker")
        self._thread.start()

    def stop(self) -> None:
        self._stop_event.set()

    def _run(self) -> None:
        while not self._stop_event.is_set():
            try:
                self.service.check_alerts(send_notification=True)
            except Exception as exc:  # pragma: no cover - best-effort background loop
                print(f"Alert worker error: {exc}")
            self._stop_event.wait(self.interval_seconds)


class AppHandler(SimpleHTTPRequestHandler):
    store = WalletStore(WALLETS_FILE)
    service = WalletTrackerService(store, HyperliquidClient())

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, directory=str(STATIC_DIR), **kwargs)

    def log_message(self, format: str, *args: Any) -> None:
        if os.environ.get("QUIET_HTTP") == "1":
            return
        super().log_message(format, *args)

    def do_GET(self) -> None:
        path = urlparse(self.path).path

        if path == "/api/health":
            self.send_json({"ok": True, "timestamp": now_iso()})
            return

        if path == "/api/wallets":
            self.send_json({"wallets": [wallet.to_dict() for wallet in self.store.list_wallets()]})
            return

        if path == "/api/dashboard":
            self.send_json(self.service.dashboard())
            return

        if path == "/api/alerts/status":
            self.send_json(self.service.get_alert_settings())
            return

        if path == "/api/alerts/preview":
            self.send_json(self.service.check_alerts(send_notification=False))
            return

        if path == "/api/markets":
            self.send_json({"markets": self.service.client.list_markets()[:60]})
            return

        super().do_GET()

    def do_POST(self) -> None:
        path = urlparse(self.path).path

        if path == "/api/wallets":
            payload = self.read_json_body()
            address = normalize_address(str(payload.get("address", "")).strip())
            alias = str(payload.get("alias", "")).strip()
            notes = str(payload.get("notes", "")).strip()

            if not address:
                status, body = format_error("Wallet address must be a valid 42-character hex string.")
                self.send_json(body, status)
                return

            wallet, _ = self.store.upsert_wallet(address, alias, notes)
            self.send_json({"wallet": wallet.to_dict()}, HTTPStatus.CREATED)
            return

        if path == "/api/wallets/import":
            payload = self.read_json_body()
            result = self.service.import_wallets(str(payload.get("text", "")))
            self.send_json(result, HTTPStatus.CREATED)
            return

        if path == "/api/discovery/scan":
            payload = self.read_json_body()
            result = self.service.scan_discovery_candidates(
                addresses=payload.get("addresses", []),
                limit=int(payload.get("limit", 15)),
                min_account_value=to_float(payload.get("minAccountValue")),
                min_realized_pnl=to_float(payload.get("minRealizedPnl")),
            )
            self.send_json(result)
            return

        if path == "/api/alerts/config":
            payload = self.read_json_body()
            self.send_json(self.service.update_alert_settings(payload))
            return

        if path == "/api/alerts/check":
            self.send_json(self.service.check_alerts(send_notification=True))
            return

        status, body = format_error("Route not found.", HTTPStatus.NOT_FOUND)
        self.send_json(body, status)

    def do_DELETE(self) -> None:
        path = urlparse(self.path).path
        prefix = "/api/wallets/"
        if path.startswith(prefix):
            address = normalize_address(path[len(prefix) :])
            if not address:
                status, body = format_error("Wallet address must be a valid 42-character hex string.")
                self.send_json(body, status)
                return

            removed = self.store.remove_wallet(address)
            if not removed:
                status, body = format_error("Wallet not found.", HTTPStatus.NOT_FOUND)
                self.send_json(body, status)
                return

            self.send_json({"removed": True, "address": address})
            return

        status, body = format_error("Route not found.", HTTPStatus.NOT_FOUND)
        self.send_json(body, status)

    def read_json_body(self) -> dict[str, Any]:
        content_length = int(self.headers.get("Content-Length", "0"))
        raw = self.rfile.read(content_length).decode("utf-8") if content_length else "{}"
        return json.loads(raw or "{}")

    def send_json(self, payload: dict[str, Any], status: int = HTTPStatus.OK) -> None:
        body = json.dumps(payload).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)


def main() -> None:
    port = int(os.environ.get("PORT", "8000"))
    host = os.environ.get("HOST", "0.0.0.0")
    alert_interval = int(os.environ.get("ALERT_CHECK_INTERVAL_SECONDS", "0"))

    server = ThreadingHTTPServer((host, port), AppHandler)

    worker = None
    if alert_interval > 0:
        worker = AlertWorker(AppHandler.service, alert_interval)
        worker.start()
        print(f"Alert worker enabled with {alert_interval}s interval")

    print(f"Hyperliquid tracker running on http://{host}:{port}")
    try:
        server.serve_forever()
    finally:
        if worker:
            worker.stop()


if __name__ == "__main__":
    main()
