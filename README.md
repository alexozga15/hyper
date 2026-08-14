# Hyperwatch Pro

Hyperwatch Pro is a lightweight Hyperliquid wallet tracker inspired by Hyperdash and CoinMarketMan. It stays dependency-free: a small Python backend talks to the official Hyperliquid API, and a static frontend provides a real-time cohort dashboard.

## Features

- Track public Hyperliquid wallets in a local watchlist
- Bulk import address lists with optional aliases and notes
- View sortable wallet analytics for account value, PnL, exposure, orders, and hit rate
- Use official Hyperliquid WebSocket feeds for live wallet refreshes
- Discover new wallet candidates automatically by watching public `trades` streams and then scoring discovered addresses
- Group wallets into familiar cohorts such as `Apex`, `Whale`, and `Money Printer`
- Generate high-conviction buy/sell signals from fresh, independent multi-wallet flow scoring 70+/100 probability

Wallet alerts require four independent wallets in the current position consensus plus three net independent wallets
adding at least $500K each in the same direction during a 15-minute window. Opposite fresh flow from two wallets or a
70+ CoinMarketMan signal in the opposite direction vetoes the alert. Signals use the recent-add VWAP, expire after two
hours without another add, and move through `NEW`, `CONFIRMED`, and `INVALIDATED` states. Alert outcomes are retained
for 30 days at 15-minute, 1-hour, 4-hour, 12-hour, and 24-hour horizons.

A separate candidate layer watches for three independent wallets adding the same side within 15 minutes with at
least $500K of aggregate fresh notional and no opposite fresh wallet. One current top-10 wallet makes it a `WATCH`;
two top-10 wallets or a same-direction CMM score of 70+ promotes it to `ACTIONABLE`. Only actionable candidates send
automatic Telegram alerts. Every candidate, including shadow and blocked cases, snapshots its wallets, top-10 cohort,
CMM context, and signal-time market price for 180-day forward evaluation at 4-hour, 12-hour, and 24-hour horizons.

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
COINMARKETMAN_API_TOKEN=...
HYPERWATCH_API_TOKEN=...
```

### API access control

Every `/api` route except `/api/health` is authenticated. `HYPERWATCH_API_TOKEN` is what gates it:

- **Token set:** requests must present it as `Authorization: Bearer <token>`, an `X-Api-Token` header, or the `hw_token` cookie. Anything else gets `401`.
- **Token unset:** only loopback callers are served, so `python3 server.py` keeps working locally with no configuration. Remote requests get `503` with an explanation. Set `ALLOW_UNAUTHENTICATED_LOOPBACK=0` to require a token even on localhost.

To use the dashboard in a browser against a remote deployment, visit `/api/session?token=<token>` once. That sets an `HttpOnly`, `SameSite=Strict` session cookie and the UI works normally from then on.

Until `HYPERWATCH_API_TOKEN` is set, all remote `/api` calls return `503`; the included [`render.yaml`](render.yaml) sets it automatically via `generateValue: true`.

Notes:

- Use persistent storage for `DATA_DIR` so tracked wallets and alert settings survive restarts.
- The included [`render.yaml`](render.yaml) mounts a persistent disk at `/var/data`.
- The included [`Dockerfile`](Dockerfile) is enough for Docker-based platforms like Render or Railway.
- Set Telegram alert credentials through `POST /api/alerts/config` after deployment.

## CoinMarketMan HyperTracker API

The helper in [`coinmarketman.py`](coinmarketman.py) reads the API key from `COINMARKETMAN_API_TOKEN`. Do not commit the token.

Useful commands:

```bash
COINMARKETMAN_API_TOKEN=... python3 coinmarketman.py segments
COINMARKETMAN_API_TOKEN=... python3 coinmarketman.py top-wallets --limit 10 --min-age-days 30 --min-total-equity 100000 --min-perp-pnl 1000000
COINMARKETMAN_API_TOKEN=... python3 coinmarketman.py cohort-summary 8 --position-age 7d
COINMARKETMAN_API_TOKEN=... python3 coinmarketman.py position-metrics BTC 8 --position-recency-timeframe 7d --limit 100
```

The official docs used for this helper are [Cohort Intelligence](https://docs.coinmarketman.com/endpoints/cohort-intelligence), [Trader & Wallet Data](https://docs.coinmarketman.com/endpoints/trader-and-wallet-data), and [Leaderboards](https://docs.coinmarketman.com/endpoints/leaderboards).

## Free Option

If you want a free setup, use the included GitHub Actions workflow at [`.github/workflows/sentiment-alerts.yml`](.github/workflows/sentiment-alerts.yml).

How it works:

- GitHub Actions checks alerts every 5 minutes and can also be started manually.
- The periodic Telegram update runs every 4 hours.
- Telegram secrets stay in GitHub Secrets, not in the repo.
- Previous alert state is stored in [`data/alerts.json`](data/alerts.json). The workflow uploads it as a
  run artifact rather than committing it back to `main`: the EC2 timer rewrites that same file
  continuously, and a `git rebase` inside CI could resurrect a stale snapshot over live state.
  Use the EC2 runner if you need state remembered across runs.

Required GitHub repository secrets:

```text
TELEGRAM_BOT_TOKEN=...
TELEGRAM_CHAT_ID=301411846
```

Then enable Actions in GitHub and run the `Sentiment Alerts` workflow once.

## EC2 Sentiment Alerts

For reliable five-minute checks, install the units in [`deploy/ec2`](deploy/ec2) on an
Ubuntu EC2 instance. The service keeps mutable alert state outside the Git checkout
in `/home/ubuntu/hyper-state`, loads secrets from
`/home/ubuntu/.config/hyper/sentiment-alerts.env`, and updates the checkout from
`main` before each run. Install the example environment file with mode `600`, replace
its placeholder values, then enable the timer:

```bash
sudo install -m 0644 deploy/ec2/hyper-sentiment.service /etc/systemd/system/
sudo install -m 0644 deploy/ec2/hyper-sentiment.timer /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now hyper-sentiment.timer
```

The GitHub `Sentiment Alerts` workflow remains available for manual recovery runs,
but has no schedule once the EC2 timer is active. Do not schedule both runners at the
same time because they keep separate alert baselines and can send duplicate alerts.
That workflow now starts from an empty baseline every time, since alert state is no
longer tracked in Git, so a manual run treats every open position as new and is worth
firing only when EC2 is actually down.

Automatic Telegram updates observe quiet hours from `23:00` to `07:00` in
`Europe/Warsaw`. Checks continue during that window and acknowledge state changes
without sending, so stale overnight alerts are not delivered in the morning. Telegram
commands remain available at all times. Configure the window with
`QUIET_HOURS_TIMEZONE`, `QUIET_HOURS_START`, and `QUIET_HOURS_END`.

If you want to verify Telegram delivery without waiting for a real sentiment change, run [`.github/workflows/telegram-test.yml`](.github/workflows/telegram-test.yml). It sends a one-off test message and does not touch the saved alert baseline.

If you want on-demand bot replies, enable [`.github/workflows/telegram-commands.yml`](.github/workflows/telegram-commands.yml).

- It supports both `repository_dispatch` (instant trigger) and a scheduled fallback every 5 minutes.
- In webhook mode, the workflow replies from the dispatch payload directly. In polling mode, it falls back to Telegram `getUpdates`.
- Commands supported:
  - `/update`
  - `/sentiment`
  - `/consensus`
  - `/signals`
  - `/cmm`
  - `/hip3`
  - `/positions`
  - `/ranks`
  - `/elite`
  - `/help`

For near-real-time replies, deploy the Cloudflare Worker bridge in [`worker/`](worker/) and connect Telegram webhooks to it. The worker triggers `repository_dispatch` with the full Telegram update so the workflow runs right away when you message the bot.

The Telegram command cursor is stored in `data/telegram_bot_state.json` under `DATA_DIR`, so the bot only answers new messages once.

### Runtime state is never tracked in Git

`data/alerts.json` and `data/telegram_bot_state.json` are rewritten on every run and
are therefore ignored, not tracked. Tracking them left the EC2 checkout permanently
dirty, and because each unit updates itself with `git pull --ff-only` under
`ExecStartPre=-`, the refused fast-forward was swallowed and deploys silently stopped
landing. A checkout that already carries modified copies needs them cleared once,
after confirming the live state under `DATA_DIR` (`/home/ubuntu/hyper-state`) is
untouched:

```bash
git -C /home/ubuntu/hyper-alerts checkout -- data/alerts.json data/telegram_bot_state.json && git -C /home/ubuntu/hyper-alerts pull --ff-only
```

The health monitor now reports `deploy checkout dirty` so the same freeze cannot go
unnoticed again.

### Health monitor

`hyper-health-monitor.timer` runs [`scripts/run_health_monitor.py`](scripts/run_health_monitor.py)
every ten minutes. It writes `data/health_monitor_state.json` and messages Telegram
only when the issue list changes, so a standing problem is reported once rather than
every ten minutes. It always exits `0`: a detected issue is a finding, not a failure
of the script, and the old non-zero exit made systemd mark the unit failed on every
unhealthy run — which is why real crashes were indistinguishable from a monitor doing
its job.

Alongside stale checks, cache coverage, degraded fills, disk space, and a persistent
HTTP 429, it watches for a signal pipeline that has gone quiet:

| Issue | Meaning | Tuned by |
| --- | --- | --- |
| `no signal of any tier in >24h` | Not even a shadow record was written. Shadow sampling fires whenever consensus moves, so this means the machinery broke. | `SIGNAL_PIPELINE_SILENCE_HOURS` |
| `no published or candidate signal in >7d` | The pipeline runs but never clears its gates. This is about thresholds being too tight, not breakage. | `PUBLISHED_SIGNAL_DROUGHT_DAYS` |

Neither fires before the first sentiment check has run, so a fresh install stays
quiet until it has had a chance to produce something.

## Wallet Signal Backtest

Run a walk-forward evaluation of fresh multi-wallet opening consensus:

```bash
python3 scripts/backtest_wallet_signals.py --days 30
```

The report is written to `data/wallet_signal_backtest.json`. It compares 3-wallet/10-minute, 4-wallet/30-minute, and 5-wallet/30-minute variants using only timestamped fills available at each event. Results include 1h, 4h, 12h, and 24h net returns, MFE/MAE, and a chronological 60/20/20 train-validation-test split. It does not claim to backtest CMM confirmation, wallet correlation, or historical top-10 membership, because those historical snapshots are not retained yet.

## Import format

Paste one wallet per line in any of these formats:

```text
0xabc...
Alias,0xabc...,notes
0xabc...,Alias,notes
```

## Notes

- Wallet metadata is saved locally in [`data/tracked_wallets.json`](data/tracked_wallets.json)
- All-time profitability is sourced from Hyperliquid's official `portfolio` endpoint
- Discovery works by collecting wallet addresses exposed in Hyperliquid's public `trades` WebSocket feed, then ranking those candidates with live wallet snapshots
