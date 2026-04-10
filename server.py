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
MIN_POSITION_MESSAGE_VALUE = 200_000
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
            }
        return periods

    def fetch_wallet_snapshot(self, wallet: TrackedWallet) -> dict[str, Any]:
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
                    {"type": "userFills", "user": wallet.address, "aggregateByTime": True},
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
        win_count = 0
        loss_count = 0
        for fill in fills[:30]:
            closed_pnl = to_float(fill.get("closedPnl"))
            recent_realized_pnl += closed_pnl
            if closed_pnl > 0:
                win_count += 1
            elif closed_pnl < 0:
                loss_count += 1

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

        account_value = to_float(margin_summary.get("accountValue"))
        total_notional = to_float(margin_summary.get("totalNtlPos"))
        margin_used = to_float(margin_summary.get("totalMarginUsed"))
        withdrawable = to_float(state.get("withdrawable"))
        discovery_score = account_value + (abs(total_notional) * 0.2) + max(all_time_realized, 0.0)

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
            "hitRate": (win_count / max(win_count + loss_count, 1)) * 100,
        }

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

        return {
            "generatedAt": now_iso(),
            "markets": self.client.list_markets()[:30],
            "totals": totals,
            "segments": segments,
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
                    {
                        "coin": coin,
                        "side": side,
                        "walletCount": 0,
                        "totalValue": 0.0,
                        "wallets": [],
                        "walletAddresses": set(),
                    },
                )
                bucket["totalValue"] += position_value
                bucket["wallets"].append(
                    {
                        "address": snapshot["address"],
                        "alias": snapshot.get("alias", ""),
                        "value": round(position_value, 2),
                    }
                )
                address = str(snapshot.get("address") or "")
                if address and address not in bucket["walletAddresses"]:
                    bucket["walletAddresses"].add(address)
                    bucket["walletCount"] += 1

                if side == "long":
                    total_long += position_value
                else:
                    total_short += position_value

        consensus = sorted(
            [
                {
                    "coin": bucket["coin"],
                    "side": bucket["side"],
                    "walletCount": bucket["walletCount"],
                    "totalValue": round(bucket["totalValue"], 2),
                    "wallets": sorted(bucket["wallets"], key=lambda item: item["value"], reverse=True),
                }
                for bucket in aggregate.values()
                if bucket["walletCount"] >= min_wallets
            ],
            key=lambda item: (item["walletCount"], item["totalValue"]),
            reverse=True,
        )
        hip3_consensus = [item for item in consensus if str(item.get("coin", "")).startswith("@")]

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
            "longExposure": round(total_long, 2),
            "shortExposure": round(total_short, 2),
            "walletCount": len(snapshots),
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
            if old_item["walletCount"] != new_item["walletCount"]:
                changed.append(
                    {
                        "coin": new_item["coin"],
                        "side": new_item["side"],
                        "fromWalletCount": old_item["walletCount"],
                        "toWalletCount": new_item["walletCount"],
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

        return {
            "biasChanged": previous.get("overallBias") != current.get("overallBias"),
            "addedConsensus": sorted(added, key=lambda item: (item["walletCount"], item["totalValue"]), reverse=True),
            "removedConsensus": sorted(removed, key=lambda item: (item["walletCount"], item["totalValue"]), reverse=True),
            "changedConsensus": sorted(changed, key=lambda item: (item["toWalletCount"], item["coin"]), reverse=True),
            "hip3Added": hip3_added,
            "hip3Removed": hip3_removed,
        }

    def build_telegram_message(self, changes: dict[str, Any], summary: dict[str, Any], min_wallets: int) -> str:
        lines = [
            f"Wallet sentiment update",
            f"Bias: {summary.get('overallBias', 'mixed')}",
            f"Consensus threshold: {min_wallets} wallets",
        ]

        if changes["addedConsensus"]:
            lines.append("")
            lines.append("New consensus:")
            for item in changes["addedConsensus"][:10]:
                lines.append(
                    f'- {item["coin"]} {item["side"]} ({item["walletCount"]} wallets, ${item["totalValue"]:,.0f})'
                )

        if changes["changedConsensus"]:
            lines.append("")
            lines.append("Consensus size changes:")
            for item in changes["changedConsensus"][:10]:
                lines.append(
                    f'- {item["coin"]} {item["side"]}: {item["fromWalletCount"]} -> {item["toWalletCount"]} wallets'
                )

        if changes["removedConsensus"]:
            lines.append("")
            lines.append("Consensus removed:")
            for item in changes["removedConsensus"][:10]:
                lines.append(f'- {item["coin"]} {item["side"]}')

        if changes["hip3Added"]:
            lines.append("")
            lines.append("New HIP-3 consensus:")
            for item in changes["hip3Added"][:10]:
                lines.append(
                    f'- {item["coin"]} {item["side"]} ({item["walletCount"]} wallets, ${item["totalValue"]:,.0f})'
                )

        if changes["hip3Removed"]:
            lines.append("")
            lines.append("HIP-3 consensus removed:")
            for item in changes["hip3Removed"][:10]:
                lines.append(f'- {item["coin"]} {item["side"]}')

        return "\n".join(lines)

    def build_summary_message(
        self,
        summary: dict[str, Any],
        min_wallets: int,
        *,
        title: str = "Current wallet sentiment",
        include_consensus: bool = True,
        include_hip3: bool = True,
    ) -> str:
        lines = [
            title,
            f"Bias: {summary.get('overallBias', 'mixed')}",
            f"Consensus threshold: {min_wallets} wallets",
            f'Wallets tracked: {summary.get("walletCount", 0)}',
        ]

        if include_consensus:
            consensus = summary.get("consensus", [])
            lines.append("")
            lines.append("Consensus:")
            if consensus:
                for item in consensus[:10]:
                    lines.append(
                        f'- {item["coin"]} {item["side"]} ({item["walletCount"]} wallets, ${item["totalValue"]:,.0f})'
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
                if position_value < min_value:
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
                        "walletAddresses": set(),
                    },
                )
                bucket["positionCount"] += 1
                bucket["totalValue"] += position_value
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
                }
                for item in groups.values()
            ],
            key=lambda item: (item["walletCount"], item["totalValue"], item["coin"]),
            reverse=True,
        )

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
        hip3_groups = self.build_position_groups(dashboard, hip3_only=True)
        total_positions = sum(len(wallet.get("positions", [])) for wallet in dashboard.get("wallets", []))

        if not position_groups and not commodity_groups and not stock_groups and not hip3_groups:
            lines.append("")
            lines.append("- No open positions")
        else:
            sections = [
                (f"By position (>= ${MIN_POSITION_MESSAGE_VALUE:,.0f}):", position_groups),
                ("Commodities:", commodity_groups),
                ("Stocks / indices:", stock_groups),
                ("HIP-3 positions:", hip3_groups),
            ]
            for heading, groups in sections:
                lines.append("")
                lines.append(heading)
                if groups:
                    for item in groups[:50]:
                        lines.append(
                            f'- {item["coin"]} {item["side"]} '
                            f'({item["walletCount"]} wallets, {item["positionCount"]} positions, ${item["totalValue"]:,.0f})'
                        )
                else:
                    lines.append("- None")

        lines.append("")
        lines.append(f"Position groups: {len(position_groups) + len(commodity_groups) + len(stock_groups) + len(hip3_groups)}")
        lines.append(f"Open positions: {total_positions}")
        lines.append(f'Checked at: {dashboard.get("generatedAt", now_iso())}')
        return "\n".join(lines)

    def build_hourly_update_message(self, dashboard: dict[str, Any], summary: dict[str, Any], min_wallets: int) -> str:
        return "\n\n".join(
            [
                self.build_summary_message(summary, min_wallets, title="Hourly wallet update"),
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
        track_hip3 = bool(config.get("trackHip3", True))

        dashboard = self.dashboard()
        summary = self.build_sentiment_summary(dashboard["wallets"], min_wallets)
        previous_summary = state.get("summary", {}) if isinstance(state, dict) else {}
        changes = self.summarize_changes(previous_summary, summary, track_hip3)

        should_notify = any(
            [
                changes["biasChanged"],
                changes["addedConsensus"],
                changes["removedConsensus"],
                changes["changedConsensus"],
                changes["hip3Added"],
                changes["hip3Removed"],
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

        new_state = {
            "summary": summary,
            "lastCheckedAt": now_iso(),
            "lastSentAt": now_iso() if sent else state.get("lastSentAt"),
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
        self.send_telegram_message(
            bot_token,
            chat_id,
            self.build_hourly_update_message(dashboard, summary, min_wallets),
        )
        return {"sent": True, "summary": summary}


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
        payload: dict[str, Any] = {}
        json_routes = {"/api/wallets", "/api/wallets/import", "/api/discovery/scan", "/api/alerts/config"}
        if path in json_routes:
            try:
                payload = self.read_json_body()
            except ValueError as exc:
                status, body = format_error(str(exc), HTTPStatus.BAD_REQUEST)
                self.send_json(body, status)
                return

        if path == "/api/wallets":
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
            result = self.service.import_wallets(str(payload.get("text", "")))
            self.send_json(result, HTTPStatus.CREATED)
            return

        if path == "/api/discovery/scan":
            result = self.service.scan_discovery_candidates(
                addresses=payload.get("addresses", []),
                limit=int(payload.get("limit", 15)),
                min_account_value=to_float(payload.get("minAccountValue")),
                min_realized_pnl=to_float(payload.get("minRealizedPnl")),
            )
            self.send_json(result)
            return

        if path == "/api/alerts/config":
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
        try:
            content_length = int(self.headers.get("Content-Length", "0"))
        except ValueError as exc:
            raise ValueError("Invalid Content-Length header.") from exc
        raw_bytes = self.rfile.read(content_length) if content_length else b"{}"
        try:
            raw = raw_bytes.decode("utf-8")
            payload = json.loads(raw or "{}")
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise ValueError("Request body must be valid JSON.") from exc
        if not isinstance(payload, dict):
            raise ValueError("Request body must be a JSON object.")
        return payload

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
