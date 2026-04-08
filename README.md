# Hyperwatch Pro

Hyperwatch Pro is a lightweight Hyperliquid wallet tracker inspired by Hyperdash and CoinMarketMan. It stays dependency-free: a small Python backend talks to the official Hyperliquid API, and a static frontend provides a real-time cohort dashboard.

## Features

- Track public Hyperliquid wallets in a local watchlist
- Bulk import address lists with optional aliases and notes
- View sortable wallet analytics for account value, PnL, exposure, orders, and hit rate
- Use official Hyperliquid WebSocket feeds for live wallet refreshes
- Discover new wallet candidates automatically by watching public `trades` streams and then scoring discovered addresses
- Group wallets into familiar cohorts such as `Apex`, `Whale`, and `Money Printer`

## Run

```bash
python3 server.py
```

Then open [http://127.0.0.1:8000](http://127.0.0.1:8000).

## Deploy

This app can run as an always-on service on a host such as Render.

Important environment variables:

```text
HOST=0.0.0.0
PORT=8000
DATA_DIR=/var/data
ALERT_CHECK_INTERVAL_SECONDS=900
```

Notes:

- Use persistent storage for `DATA_DIR` so tracked wallets and alert settings survive restarts.
- The included [`render.yaml`](/Users/alexozga/Documents/New%20project%204/render.yaml) mounts a persistent disk at `/var/data`.
- The included [`Dockerfile`](/Users/alexozga/Documents/New%20project%204/Dockerfile) is enough for Docker-based platforms like Render or Railway.
- Set Telegram alert credentials through `POST /api/alerts/config` after deployment.

## Import format

Paste one wallet per line in any of these formats:

```text
0xabc...
Alias,0xabc...,notes
0xabc...,Alias,notes
```

## Notes

- Wallet metadata is saved locally in [`data/tracked_wallets.json`](/Users/alexozga/Documents/New%20project%204/data/tracked_wallets.json)
- All-time profitability is sourced from Hyperliquid's official `portfolio` endpoint
- Discovery works by collecting wallet addresses exposed in Hyperliquid's public `trades` WebSocket feed, then ranking those candidates with live wallet snapshots
