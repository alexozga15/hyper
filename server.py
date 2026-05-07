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
from datetime import datetime, timezone
from http import HTTPStatus
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any
from urllib.parse import urlparse


ROOT = Path(__file__).resolve().parent
STATIC_DIR = ROOT / "static"
DATA_DIR = Path(os.environ.get("DATA_DIR", str(ROOT / "data"))).resolve()
WALLETS_FILE = DATA_DIR / "tracked_wallets.json"
ALERTS_FILE = DATA_DIR / "alerts.json"
TELEGRAM_STATE_FILE = DATA_DIR / "telegram_bot_state.json"
HYPERLIQUID_INFO_URL = "https://api.hyperliquid.xyz/info"
HYPERLIQUID_WS_URLS = (
    "wss://api-ui.hyperliquid.xyz/ws",
    "wss://api.hyperliquid.xyz/ws",
)
HEX_ADDRESS_RE = re.compile(r"0x[a-fA-F0-9]{40}")
MAX_IMPORT_BATCH = 100
MAX_DISCOVERY_BATCH = 60
DEFAULT_CONSENSUS_THRESHOLD = 3
HIGH_CONVICTION_SIGNAL_THRESHOLD = 80.0
EXTREME_CONVICTION_SIGNAL_THRESHOLD = 90.0
SIGNAL_CONVICTION_ALERT_MIN_DELTA = 10.0
MIN_POSITION_MESSAGE_VALUE = 500_000
MIN_POSITION_MESSAGE_WALLETS = 2
NEW_POSITION_ALERT_MIN_VALUE = MIN_POSITION_MESSAGE_VALUE
POSITION_INCREASE_ALERT_MIN_DELTA = MIN_POSITION_MESSAGE_VALUE
POSITION_INCREASE_ALERT_MIN_PCT = 0.5
CONSENSUS_SIZE_ALERT_MIN_DELTA = 2
CONSENSUS_SIZE_ALERT_MIN_PCT = 0.5
RANKING_MIN_7D_CLOSED_TRADES = 5
RANKING_FULL_CONFIDENCE_7D_CLOSED_TRADES = 20
RANKING_MIN_30D_CLOSED_TRADES = 20
RANKING_FULL_CONFIDENCE_30D_CLOSED_TRADES = 30
RANKING_WINDOW_MS = 7 * 24 * 60 * 60 * 1000
HOLDING_ONLY_WINDOW_MS = 30 * 24 * 60 * 60 * 1000
OIL_POSITION_ALIASES = {"flx:OIL", "cash:WTI", "xyz:BRENTOIL", "xyz:CL"}
RAW_OIL_POSITION_NAMES = {"BRENTOIL", "CL", "WTI", "OIL"}
RAW_COMMODITY_POSITION_NAMES = RAW_OIL_POSITION_NAMES | {"GOLD", "SILVER", "COPPER", "NATGAS"}
RAW_STOCK_INDEX_POSITION_NAMES = {"SP500", "US500", "XYZ100", "NAS100", "NDX", "SPX"}
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


def env_flag(name: str, default: bool) -> bool:
    raw = os.environ.get(name)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "on"}


def to_float(value: Any) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0


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


def build_wallet_quality_rank(
    hit_rate: float,
    closed_trade_count: int,
    pnl_7d: float,
    account_value: float,
    *,
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
    win_rate_score = normalized_hit_rate * confidence_30d
    drawdown_control_score = clamp(100.0 - (to_float(max_drawdown_pct) * 5.0))
    margin_score = clamp(100.0 - max(0.0, to_float(margin_usage_pct) - 30.0) * 2.0)
    unrealized_return_pct = (to_float(unrealized_pnl) / account) * 100 if account > 0 else 0.0
    unrealized_score = return_score(unrealized_return_pct)
    open_health_score = (margin_score * 0.6) + (unrealized_score * 0.4)
    score = (
        return_score(pnl_7d_return_pct) * 0.25
        + return_score(pnl_30d_return_pct) * 0.20
        + profit_factor_score(profit_factor) * 0.15
        + return_score(expectancy_pct) * 0.15
        + win_rate_score * 0.10
        + drawdown_control_score * 0.10
        + open_health_score * 0.05
    )

    elite_eligible = (
        sample_size_30d >= RANKING_MIN_30D_CLOSED_TRADES
        and profit_factor >= 1.5
        and to_float(max_drawdown_pct) <= 20.0
        and to_float(margin_usage_pct) <= 60.0
    )

    if sample_size_7d < RANKING_MIN_7D_CLOSED_TRADES and sample_size_30d < RANKING_MIN_30D_CLOSED_TRADES:
        label = "Unranked"
    elif score >= 80 and elite_eligible:
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
        "hitRateScore": round(win_rate_score, 1),
        "pnlScore": round(return_score(pnl_7d_return_pct), 1),
        "pnl30dScore": round(return_score(pnl_30d_return_pct), 1),
        "profitFactorScore": round(profit_factor_score(profit_factor), 1),
        "drawdownScore": round(drawdown_control_score, 1),
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
    def post(self, payload: dict[str, Any]) -> Any:
        request = urllib.request.Request(
            HYPERLIQUID_INFO_URL,
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

    def fetch_wallet_snapshot(self, wallet: TrackedWallet) -> dict[str, Any]:
        now_ms = current_time_ms()
        cutoff_7d_ms = now_ms - RANKING_WINDOW_MS
        cutoff_30d_ms = now_ms - HOLDING_ONLY_WINDOW_MS
        with ThreadPoolExecutor(max_workers=5) as executor:
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
                "orders": executor.submit(self.client.safe_post, {"type": "openOrders", "user": wallet.address}, []),
                "fills": executor.submit(
                    self.client.safe_post,
                    {
                        "type": "userFillsByTime",
                        "user": wallet.address,
                        "startTime": cutoff_30d_ms,
                        "aggregateByTime": True,
                    },
                    [],
                ),
                "role": executor.submit(self.fetch_wallet_role, wallet.address),
                "portfolio": executor.submit(self.fetch_portfolio, wallet.address),
            }

        state = futures["state"].result()
        open_orders = futures["orders"].result()
        fills = futures["fills"].result()
        role = futures["role"].result()
        portfolio = futures["portfolio"].result()

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
        for fill in fills:
            closed_pnl = to_float(fill.get("closedPnl"))
            fill_time = int(to_float(fill.get("time")))
            last_fill_time = max(last_fill_time, fill_time)
            if fill_time >= cutoff_30d_ms:
                fills_30d_count += 1
                if closed_pnl != 0:
                    realized_pnl_30d += closed_pnl
                    if closed_pnl > 0:
                        gross_profit_30d += closed_pnl
                        win_count_30d += 1
                    elif closed_pnl < 0:
                        gross_loss_30d += abs(closed_pnl)
                        loss_count_30d += 1
            is_7d_closed_fill = fill_time >= cutoff_7d_ms and closed_pnl != 0
            if is_7d_closed_fill:
                recent_realized_pnl += closed_pnl
                if closed_pnl > 0:
                    win_count += 1
                elif closed_pnl < 0:
                    loss_count += 1

            if len(recent_fills) < 30:
                recent_fills.append(
                    {
                        "coin": fill.get("coin", "Unknown"),
                        "direction": fill.get("dir", "Unknown"),
                        "price": to_float(fill.get("px")),
                        "size": to_float(fill.get("sz")),
                        "closedPnl": closed_pnl,
                        "fee": to_float(fill.get("fee")),
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

        account_value = to_float(margin_summary.get("accountValue"))
        total_notional = to_float(margin_summary.get("totalNtlPos"))
        margin_used = to_float(margin_summary.get("totalMarginUsed"))
        withdrawable = to_float(state.get("withdrawable"))
        margin_usage_pct = (margin_used / account_value) * 100 if account_value > 0 else 0.0
        holding_only_30d = bool(positions) and fills_30d_count == 0 and len(open_orders) == 0
        days_since_last_fill = None
        if last_fill_time:
            days_since_last_fill = round(max(0, now_ms - last_fill_time) / (24 * 60 * 60 * 1000), 1)
        discovery_score = account_value + (abs(total_notional) * 0.2) + max(all_time_realized, 0.0)
        recent_win_rate_rank = build_wallet_quality_rank(
            hit_rate,
            recent_closed_trade_count,
            recent_realized_pnl,
            account_value,
            closed_trade_count_30d=win_count_30d + loss_count_30d,
            pnl_30d=realized_pnl_30d,
            gross_profit_30d=gross_profit_30d,
            gross_loss_30d=gross_loss_30d,
            max_drawdown_pct=performance.get("month", {}).get("maxDrawdownPct", 0.0),
            margin_usage_pct=margin_usage_pct,
            unrealized_pnl=unrealized_pnl,
        )

        return {
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
            "fills30d": fills_30d_count,
            "daysSinceLastFill": days_since_last_fill,
            "holdingOnly30d": holding_only_30d,
            "recentWinRateRank": recent_win_rate_rank,
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
        with ThreadPoolExecutor(max_workers=min(max(len(wallets), 1), 8)) as executor:
            snapshots = list(executor.map(self.fetch_wallet_snapshot, wallets)) if wallets else []
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

    def build_high_conviction_signals(
        self,
        consensus: list[dict[str, Any]],
        *,
        threshold: float = HIGH_CONVICTION_SIGNAL_THRESHOLD,
    ) -> list[dict[str, Any]]:
        signals = []
        for item in consensus:
            conviction_score = to_float(item.get("convictionScore"))
            if conviction_score < threshold:
                continue
            side = str(item.get("side") or "").lower()
            action = signal_action_from_side(side)
            strength = "extreme" if conviction_score >= EXTREME_CONVICTION_SIGNAL_THRESHOLD else "high"
            signals.append(
                {
                    "coin": item.get("coin", "Unknown"),
                    "side": side,
                    "action": action,
                    "strength": strength,
                    "walletCount": int(to_float(item.get("walletCount"))),
                    "totalValue": round(to_float(item.get("totalValue")), 2),
                    "convictionScore": round(conviction_score, 1),
                    "threshold": round(to_float(threshold), 1),
                    "wallets": item.get("wallets", [])[:5],
                    "rationale": (
                        f'{int(to_float(item.get("walletCount")))} wallets are {side} '
                        f'with {format_money_compact(to_float(item.get("totalValue")))} notional '
                        f'and {conviction_score:.0f}/100 conviction.'
                    ),
                }
            )
        return sorted(
            signals,
            key=lambda item: (item["convictionScore"], item["walletCount"], item["totalValue"]),
            reverse=True,
        )

    def build_sentiment_summary(self, snapshots: list[dict[str, Any]], min_wallets: int) -> dict[str, Any]:
        aggregate: dict[tuple[str, str], dict[str, Any]] = {}
        total_long = 0.0
        total_short = 0.0

        for snapshot in snapshots:
            for position in snapshot.get("positions", []):
                coin = normalize_position_coin(position.get("coin"))
                side = str(position.get("side") or "Flat").lower()
                position_value = to_float(position.get("positionValue"))
                if side not in {"long", "short"}:
                    continue

                key = (coin, side)
                bucket = aggregate.setdefault(
                    key,
                    {"coin": coin, "side": side, "walletCount": 0, "totalValue": 0.0, "wallets": []},
                )
                bucket["walletCount"] += 1
                bucket["totalValue"] += position_value
                bucket["wallets"].append(
                    {
                        "address": snapshot["address"],
                        "alias": snapshot.get("alias", ""),
                        "value": round(position_value, 2),
                    }
                )

                if side == "long":
                    total_long += position_value
                else:
                    total_short += position_value

        consensus = [
            {
                **bucket,
                "totalValue": round(bucket["totalValue"], 2),
                "wallets": sorted(bucket["wallets"], key=lambda item: item["value"], reverse=True),
            }
            for bucket in aggregate.values()
            if bucket["walletCount"] >= min_wallets
        ]
        max_wallet_count = max((item["walletCount"] for item in consensus), default=0)
        max_total_value = max((item["totalValue"] for item in consensus), default=0.0)
        for item in consensus:
            wallet_score = (item["walletCount"] / max_wallet_count) if max_wallet_count else 0.0
            value_score = (item["totalValue"] / max_total_value) if max_total_value else 0.0
            item["convictionScore"] = round(((wallet_score * 0.6) + (value_score * 0.4)) * 100, 1)
        consensus = sorted(
            consensus,
            key=lambda item: (item["convictionScore"], item["walletCount"], item["totalValue"]),
            reverse=True,
        )
        hip3_consensus = [item for item in consensus if str(item.get("coin", "")).startswith("@")]
        signals = self.build_high_conviction_signals(consensus)

        overall_bias = "mixed"
        if total_long > total_short * 1.2:
            overall_bias = "bullish"
        elif total_short > total_long * 1.2:
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
                if position_value < min_value:
                    continue
                coin = normalize_position_coin(position.get("coin"))
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
        }

    def summarize_large_position_changes(
        self,
        previous_positions: dict[str, Any],
        current_positions: dict[str, Any],
    ) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]]]:
        previous_map = previous_positions if isinstance(previous_positions, dict) else {}
        current_map = current_positions if isinstance(current_positions, dict) else {}
        added = [current_map[key] for key in current_map.keys() - previous_map.keys()]
        closed = [previous_map[key] for key in previous_map.keys() - current_map.keys()]
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
            has_size_baseline = previous_size > 0 or current_size > 0
            is_size_add = has_size_baseline and size_increase > 0 and increase_value >= POSITION_INCREASE_ALERT_MIN_DELTA
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
                    "addPrice": round(increase_value / size_increase, 8) if size_increase > 0 else 0.0,
                }
            )
        return (
            sorted(added, key=lambda item: item["totalValue"], reverse=True),
            sorted(increased, key=lambda item: item["increaseValue"], reverse=True),
            sorted(closed, key=lambda item: item["totalValue"], reverse=True),
        )

    def build_large_position_alert_changes(
        self,
        previous_positions: dict[str, Any],
        current_positions: dict[str, Any],
    ) -> dict[str, Any]:
        new_large_positions, increased_large_positions, closed_large_positions = self.summarize_large_position_changes(
            previous_positions,
            current_positions,
        )
        return {
            "biasChanged": False,
            "addedConsensus": [],
            "removedConsensus": [],
            "changedConsensus": [],
            "hip3Added": [],
            "hip3Removed": [],
            "newLargePositions": new_large_positions,
            "increasedLargePositions": increased_large_positions,
            "closedLargePositions": closed_large_positions,
            "addedSignals": [],
            "removedSignals": [],
            "changedSignals": [],
        }

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

        added = [current_signals[key] for key in current_signals.keys() - previous_signals.keys()]
        removed = [previous_signals[key] for key in previous_signals.keys() - current_signals.keys()]
        changed = []
        for key in current_signals.keys() & previous_signals.keys():
            old_item = previous_signals[key]
            new_item = current_signals[key]
            old_score = to_float(old_item.get("convictionScore"))
            new_score = to_float(new_item.get("convictionScore"))
            score_delta = new_score - old_score
            old_wallet_count = int(to_float(old_item.get("walletCount")))
            new_wallet_count = int(to_float(new_item.get("walletCount")))
            if abs(score_delta) < SIGNAL_CONVICTION_ALERT_MIN_DELTA and old_wallet_count == new_wallet_count:
                continue
            changed.append(
                {
                    **new_item,
                    "fromConvictionScore": round(old_score, 1),
                    "toConvictionScore": round(new_score, 1),
                    "convictionDelta": round(score_delta, 1),
                    "fromWalletCount": old_wallet_count,
                    "toWalletCount": new_wallet_count,
                }
            )

        return {
            "addedSignals": sorted(added, key=lambda item: (item["convictionScore"], item["walletCount"]), reverse=True),
            "removedSignals": sorted(removed, key=lambda item: (item["convictionScore"], item["walletCount"]), reverse=True),
            "changedSignals": sorted(changed, key=lambda item: (abs(item["convictionDelta"]), item["toWalletCount"]), reverse=True),
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
            f"Wallet alert | Bias: {summary.get('overallBias', 'mixed')} | Min: {min_wallets}",
        ]

        if changes["addedConsensus"]:
            lines.append("")
            lines.append("New consensus")
            for item in changes["addedConsensus"][:10]:
                lines.append(
                    f'- {item["coin"]} {item["side"]}: {item["walletCount"]}w, {format_money_compact(item["totalValue"])}, c{item.get("convictionScore", 0):.0f}'
                )

        if changes["changedConsensus"]:
            lines.append("")
            lines.append("Consensus changed")
            for item in changes["changedConsensus"][:10]:
                lines.append(
                    f'- {item["coin"]} {item["side"]}: {item["fromWalletCount"]}->{item["toWalletCount"]}w, c{item.get("convictionScore", 0):.0f}'
                )

        if changes["removedConsensus"]:
            lines.append("")
            lines.append("Consensus gone")
            for item in changes["removedConsensus"][:10]:
                lines.append(f'- {item["coin"]} {item["side"]}')

        if changes["addedSignals"]:
            lines.append("")
            lines.append("High-conviction signals")
            for item in changes["addedSignals"][:10]:
                lines.append(
                    f'- {str(item.get("action", "watch")).upper()} {item["coin"]}: {item["walletCount"]}w, {format_money_compact(item["totalValue"])}, c{to_float(item.get("convictionScore")):.0f}'
                )

        if changes["changedSignals"]:
            lines.append("")
            lines.append("Signal conviction changed")
            for item in changes["changedSignals"][:10]:
                lines.append(
                    f'- {str(item.get("action", "watch")).upper()} {item["coin"]}: c{to_float(item.get("fromConvictionScore")):.0f}->c{to_float(item.get("toConvictionScore")):.0f}, {item.get("fromWalletCount", 0)}->{item.get("toWalletCount", 0)}w'
                )

        if changes["removedSignals"]:
            lines.append("")
            lines.append("Signals cooled")
            for item in changes["removedSignals"][:10]:
                lines.append(f'- {str(item.get("action", "watch")).upper()} {item["coin"]} {item["side"]}')

        if changes["newLargePositions"]:
            lines.append("")
            lines.append(f"Open >{format_money_compact(NEW_POSITION_ALERT_MIN_VALUE)}")
            for item in changes["newLargePositions"][:10]:
                size_note = ""
                if to_float(item.get("totalSize")) > 0:
                    size_note = f' sz {format_position_size(to_float(item.get("totalSize")))}'
                entry_note = ""
                if to_float(item.get("entryPx")) > 0:
                    entry_note = f' @${format_price(to_float(item.get("entryPx")))}'
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
                entry_note = ""
                if to_float(item.get("entryPx")) > 0:
                    entry_note = f' @${format_price(to_float(item.get("entryPx")))}'
                lines.append(
                    f'- {wallet_label(item.get("alias", ""), item.get("address", ""))}: {item["coin"]} {item["side"]} {format_money_compact(item["totalValue"])}{size_note}{entry_note}'
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
                    add_price_note = f' add @${format_price(to_float(item.get("addPrice")))}'
                lines.append(
                    f'- {wallet_label(item.get("alias", ""), item.get("address", ""))}: {item["coin"]} {item["side"]} {format_money_compact(item["previousValue"])}->{format_money_compact(item["totalValue"])} (+{format_money_compact(item["increaseValue"])}{size_note}{add_price_note})'
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
            f'High-conviction signals: {summary.get("signalCount", len(summary.get("signals", [])))}',
        ]

        if include_signals:
            signals = summary.get("signals", [])
            lines.append("")
            lines.append("Signals:")
            if signals:
                for item in signals[:10]:
                    lines.append(
                        f'- {str(item.get("action", "watch")).upper()} {item["coin"]} {item["side"]} '
                        f'({item["walletCount"]} wallets, {format_money_compact(item["totalValue"])}, conviction {to_float(item.get("convictionScore")):.0f}/100)'
                    )
            else:
                lines.append(f'- None at {HIGH_CONVICTION_SIGNAL_THRESHOLD:.0f}+ conviction')

        if include_consensus:
            consensus = summary.get("consensus", [])
            lines.append("")
            lines.append("Consensus:")
            if consensus:
                for item in consensus[:10]:
                    lines.append(
                        f'- {item["coin"]} {item["side"]} ({item["walletCount"]} wallets, ${item["totalValue"]:,.0f}, conviction {item.get("convictionScore", 0):.0f}/100)'
                    )
            else:
                lines.append("- None")

        if include_hip3:
            hip3_consensus = summary.get("hip3Consensus", [])
            lines.append("")
            lines.append("HIP-3 consensus:")
            if hip3_consensus:
                for item in hip3_consensus[:10]:
                    lines.append(
                        f'- {item["coin"]} {item["side"]} ({item["walletCount"]} wallets, ${item["totalValue"]:,.0f})'
                    )
            else:
                lines.append("- None")

        lines.append("")
        lines.append(f'Checked at: {summary.get("generatedAt", now_iso())}')
        return "\n".join(lines)

    def live_sentiment_summary(self, min_wallets: int) -> dict[str, Any]:
        dashboard = self.dashboard()
        return self.build_sentiment_summary(dashboard["wallets"], min_wallets)

    def build_position_groups(
        self,
        dashboard: dict[str, Any],
        *,
        min_value: float = MIN_POSITION_MESSAGE_VALUE,
        min_wallets: int = MIN_POSITION_MESSAGE_WALLETS,
        hip3_only: bool | None = None,
        stock_like_only: bool | None = None,
        commodity_like_only: bool | None = None,
    ) -> list[dict[str, Any]]:
        groups: dict[tuple[str, str], dict[str, Any]] = {}

        for wallet in dashboard.get("wallets", []):
            for position in wallet.get("positions", []):
                position_value = to_float(position.get("positionValue"))
                raw_coin = str(position.get("coin") or "Unknown")
                coin = normalize_position_coin(raw_coin)
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
                        "walletAddresses": set(),
                    },
                )
                size = abs(to_float(position.get("size")))
                bucket["positionCount"] += 1
                bucket["totalValue"] += position_value
                bucket["totalSize"] += size
                bucket["entryValue"] += to_float(position.get("entryPx")) * size
                address = str(wallet.get("address") or "")
                if address and address not in bucket["walletAddresses"]:
                    bucket["walletAddresses"].add(address)
                    bucket["walletCount"] += 1

        return sorted(
            [
                {
                    "coin": item["coin"],
                    "side": item["side"],
                    "walletCount": item["walletCount"],
                    "positionCount": item["positionCount"],
                    "totalValue": round(item["totalValue"], 2),
                    "totalSize": round(item["totalSize"], 8),
                    "entryPx": round(item["entryValue"] / item["totalSize"], 8) if item["totalSize"] > 0 else 0.0,
                }
                for item in groups.values()
                if item["totalValue"] >= min_value and item["walletCount"] >= min_wallets
            ],
            key=lambda item: (item["walletCount"], item["totalValue"], item["coin"]),
            reverse=True,
        )

    def build_signals_message(self, summary: dict[str, Any], *, title: str = "High-conviction signals") -> str:
        signals = summary.get("signals", [])
        lines = [title, f'Threshold: {HIGH_CONVICTION_SIGNAL_THRESHOLD:.0f}/100 conviction']
        if signals:
            for index, item in enumerate(signals[:20], start=1):
                lines.append(
                    f'{index}. {str(item.get("action", "watch")).upper()} {item["coin"]} {item["side"]} '
                    f'({item["walletCount"]} wallets, {format_money_compact(item["totalValue"])}, conviction {to_float(item.get("convictionScore")):.0f}/100)'
                )
        else:
            lines.append("- No high-conviction signals right now")
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
                    f"By position (>= ${MIN_POSITION_MESSAGE_VALUE:,.0f}, {MIN_POSITION_MESSAGE_WALLETS}+ wallets):",
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
                        size_note = ""
                        if to_float(item.get("totalSize")) > 0:
                            size_note = f', size {format_position_size(to_float(item.get("totalSize")))}'
                        entry_note = ""
                        if to_float(item.get("entryPx")) > 0:
                            entry_note = f', entry ${format_price(to_float(item.get("entryPx")))}'
                        lines.append(
                            f'- {item["coin"]} {item["side"]} '
                            f'({item["walletCount"]} wallets, {item["positionCount"]} positions, {format_money_thousands(item["totalValue"])}{size_note}{entry_note})'
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
                wallet.get("positions", []),
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

    def build_hourly_update_message(self, dashboard: dict[str, Any], summary: dict[str, Any], min_wallets: int) -> str:
        return "\n\n".join(
            [
                self.build_summary_message(summary, min_wallets, title="Hourly wallet update"),
                self.build_signals_message(summary),
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
        summary = self.build_sentiment_summary(dashboard["wallets"], min_wallets)
        previous_summary = state.get("summary", {}) if isinstance(state, dict) else {}
        previous_positions = state.get("largePositions", {}) if isinstance(state, dict) else {}
        current_positions = self.build_large_position_snapshot(dashboard)
        # Keep HIP-3 available for explicit commands like /hip3, but exclude it
        # from automatic Telegram alerts and change-trigger decisions.
        changes = self.summarize_changes(previous_summary, summary, track_hip3=False)
        position_changes = self.build_large_position_alert_changes(previous_positions, current_positions)
        changes["newLargePositions"] = position_changes["newLargePositions"]
        changes["increasedLargePositions"] = position_changes["increasedLargePositions"]
        changes["closedLargePositions"] = position_changes["closedLargePositions"]

        should_notify = any(
            [
                changes["biasChanged"],
                changes["addedConsensus"],
                changes["removedConsensus"],
                changes["changedConsensus"],
                changes["addedSignals"],
                changes["removedSignals"],
                changes["changedSignals"],
                changes["newLargePositions"],
                changes["increasedLargePositions"],
                changes["closedLargePositions"],
            ]
        )

        sent = False
        error_message = ""
        if (
            send_notification
            and should_notify
            and config.get("enabled")
            and config.get("botToken")
            and config.get("chatId")
        ):
            try:
                self.send_telegram_message(
                    str(config["botToken"]),
                    str(config["chatId"]),
                    self.build_telegram_message(changes, summary, min_wallets),
                )
                sent = True
            except (urllib.error.URLError, TimeoutError, ValueError) as exc:
                error_message = str(exc)

        checked_at = now_iso()
        new_state = {
            **state,
            "summary": summary,
            "largePositions": current_positions,
            "lastCheckedAt": checked_at,
            "lastSentAt": checked_at if sent else state.get("lastSentAt"),
        }
        save_json_file(self.alerts_path, {"config": stored_config, "state": new_state})

        return {
            "enabled": bool(config.get("enabled")),
            "hasBotToken": bool(config.get("botToken")),
            "chatId": config.get("chatId", ""),
            "sent": sent,
            "shouldNotify": should_notify,
            "error": error_message,
            "changes": changes,
            "summary": summary,
        }

    def send_hourly_update(self, min_wallets: int, bot_token: str, chat_id: str) -> dict[str, Any]:
        dashboard = self.dashboard()
        summary = self.build_sentiment_summary(dashboard["wallets"], min_wallets)
        current_positions = self.build_large_position_snapshot(dashboard)
        self.send_telegram_message(
            bot_token,
            chat_id,
            self.build_hourly_update_message(dashboard, summary, min_wallets),
        )
        raw = load_json_file(self.alerts_path, {})
        stored_config = raw.get("config", {}) if isinstance(raw, dict) else {}
        config = self.resolve_alert_config(stored_config)
        state = raw.get("state", {}) if isinstance(raw, dict) else {}
        previous_positions = state.get("largePositions", {}) if isinstance(state, dict) else {}
        position_changes = self.build_large_position_alert_changes(previous_positions, current_positions)
        should_send_position_alert = any(
            [
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
            "summary": summary,
            "lastCheckedAt": synced_at,
            "lastHourlySyncedAt": synced_at,
        }
        if not should_send_position_alert or position_alert_sent or not config.get("enabled"):
            new_state["largePositions"] = current_positions
        if position_alert_sent:
            new_state["lastSentAt"] = synced_at
        save_json_file(self.alerts_path, {"config": stored_config, "state": new_state})
        return {
            "sent": True,
            "positionAlertSent": position_alert_sent,
            "positionAlertError": position_alert_error,
            "summary": summary,
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
