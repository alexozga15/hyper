import unittest
from datetime import datetime, timezone
from unittest.mock import patch

from coinmarketman import (
    CoinMarketManApiError,
    CoinMarketManClient,
    compact_wallet_rows,
    select_top_wallets,
    wallet_age_days,
)


class CoinMarketManClientTests(unittest.TestCase):
    def test_requires_api_token(self) -> None:
        client = CoinMarketManClient(token="", base_url="https://example.test")
        with self.assertRaises(CoinMarketManApiError):
            client.request("segments")

    def test_wallets_request_encodes_repeated_segment_ids(self) -> None:
        client = CoinMarketManClient(token="token", base_url="https://example.test")

        with patch.object(client, "request", return_value={}) as request:
            client.wallets(
                limit=100,
                order_by="perpPnl",
                segment_ids=[7, 8],
                has_open_positions=True,
            )

        request.assert_called_once_with(
            "wallets",
            {
                "offset": 0,
                "limit": 100,
                "orderBy": "perpPnl",
                "order": "desc",
                "segmentIds": ["7", "8"],
                "hasOpenPositions": "true",
            },
        )

    def test_position_metrics_uses_coin_and_segment_path(self) -> None:
        client = CoinMarketManClient(token="token", base_url="https://example.test")

        with patch.object(client, "request", return_value={}) as request:
            client.position_metrics("btc", 8, start="2026-06-01T00:00:00Z", limit=50)

        request.assert_called_once_with(
            "position-metrics/coin/BTC/segment/8",
            {
                "limit": 50,
                "positionRecencyTimeframe": "7d",
                "start": "2026-06-01T00:00:00Z",
            },
        )

    def test_encode_params_repeats_list_values(self) -> None:
        query = CoinMarketManClient._encode_params({"segmentIds": ["7", "8"], "limit": 50})
        self.assertEqual(query, "segmentIds=7&segmentIds=8&limit=50")


class CoinMarketManSelectionTests(unittest.TestCase):
    def test_wallet_age_days_parses_utc_values(self) -> None:
        now = datetime(2026, 6, 16, tzinfo=timezone.utc)
        self.assertEqual(wallet_age_days("2026-06-01T00:00:00.000Z", now), 15.0)
        self.assertIsNone(wallet_age_days("bad", now))

    def test_select_top_wallets_filters_by_age_equity_and_perp_pnl(self) -> None:
        now = datetime(2026, 6, 16, tzinfo=timezone.utc)
        payload = {
            "data": [
                {
                    "address": "0xgood",
                    "age": "2026-01-01T00:00:00.000Z",
                    "pnlMonth": 1_000_000,
                    "pnlAllTime": 10_000_000,
                    "profile": {
                        "address": "0xgood",
                        "totalEquity": 2_000_000,
                        "perpPnl": 5_000_000,
                        "openValue": 500_000,
                        "segments": [7, 8],
                    },
                },
                {
                    "address": "0xyoung",
                    "age": "2026-06-10T00:00:00.000Z",
                    "pnlMonth": 2_000_000,
                    "pnlAllTime": 20_000_000,
                    "profile": {
                        "totalEquity": 5_000_000,
                        "perpPnl": 6_000_000,
                        "openValue": 1_000_000,
                    },
                },
                {
                    "address": "0xsmall",
                    "age": "2026-01-01T00:00:00.000Z",
                    "pnlMonth": 3_000_000,
                    "pnlAllTime": 30_000_000,
                    "profile": {
                        "totalEquity": 10_000,
                        "perpPnl": 6_000_000,
                        "openValue": 1_000_000,
                    },
                },
            ]
        }

        selected = select_top_wallets(
            payload,
            min_age_days=30,
            min_total_equity=100_000,
            min_perp_pnl=1_000_000,
            require_open_positions=True,
            now=now,
        )

        self.assertEqual([item["address"] for item in selected], ["0xgood"])
        rows = compact_wallet_rows(selected)
        self.assertEqual(rows[0]["address"], "0xgood")
        self.assertEqual(rows[0]["segments"], [7, 8])


if __name__ == "__main__":
    unittest.main()
