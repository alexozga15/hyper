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

## Free Option

If you want a free setup, use the included GitHub Actions workflow at [`.github/workflows/sentiment-alerts.yml`](/Users/alexozga/Documents/New%20project%204/.github/workflows/sentiment-alerts.yml).

How it works:

- GitHub Actions runs every 15 minutes and can also be started manually.
- The scheduled sentiment update runs every hour.
- Telegram secrets stay in GitHub Secrets, not in the repo.
- Previous alert state is stored in [`data/alerts.json`](/Users/alexozga/Documents/New%20project%204/data/alerts.json) and committed back to the repo so consensus changes are remembered between runs.

Required GitHub repository secrets:

```text
TELEGRAM_BOT_TOKEN=...
TELEGRAM_CHAT_ID=301411846
```

Then enable Actions in GitHub and run the `Sentiment Alerts` workflow once.

If you want to verify Telegram delivery without waiting for a real sentiment change, run [`.github/workflows/telegram-test.yml`](/Users/alexozga/Documents/New%20project%204/.github/workflows/telegram-test.yml). It sends a one-off test message and does not touch the saved alert baseline.

If you want on-demand bot replies, enable [`.github/workflows/telegram-commands.yml`](.github/workflows/telegram-commands.yml).

- It supports both `repository_dispatch` (instant trigger) and a scheduled fallback every 15 minutes.
- In webhook mode, the workflow replies from the dispatch payload directly. In polling mode, it falls back to Telegram `getUpdates`.
- Commands supported:
  - `/update`
  - `/sentiment`
  - `/consensus`
  - `/hip3`
  - `/positions`
  - `/help`

For near-real-time replies, deploy the Cloudflare Worker bridge in [`worker/`](worker/) and connect Telegram webhooks to it. The worker triggers `repository_dispatch` with the full Telegram update so the workflow runs right away when you message the bot. A starter Wrangler config now lives in [`worker/wrangler.toml`](/Users/alexozga/Documents/New%20project%204/worker/wrangler.toml).

The Telegram command cursor is stored in [`data/telegram_bot_state.json`](data/telegram_bot_state.json), so the bot only answers new messages once.

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
