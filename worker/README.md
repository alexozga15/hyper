# Telegram Webhook Bridge (Cloudflare Worker)

This worker makes Telegram command replies near-real-time without running an always-on server.

## What it does

1. Telegram sends webhook updates to this Worker.
2. Worker forwards the full Telegram update to GitHub `repository_dispatch` (`telegram_update`).
3. `.github/workflows/telegram-commands.yml` runs immediately and replies via bot without polling Telegram.

## Deploy

1. Create a Cloudflare Worker and paste `worker/telegram-webhook-worker.js`.
2. Set Worker environment variables:
   - `WEBHOOK_SECRET` (random string)
   - `GITHUB_OWNER` (e.g. `alexozga15`)
   - `GITHUB_REPO` (e.g. `hyper`)
   - `GITHUB_DISPATCH_TOKEN` (GitHub token with repo + actions permissions)
3. Deploy Worker and copy URL, e.g. `https://hyper-telegram-bridge.<subdomain>.workers.dev`.
4. Register Telegram webhook:

```bash
curl "https://api.telegram.org/bot$TELEGRAM_BOT_TOKEN/setWebhook?url=https://YOUR-WORKER-URL/telegram/$WEBHOOK_SECRET"
```

## Verify

Send `/update` to your bot. GitHub Actions `Telegram Commands` should start immediately (event `repository_dispatch`) and respond quickly.

If you already deployed an older version of the Worker, redeploy it after updating `worker/telegram-webhook-worker.js` so the full Telegram payload is forwarded.
