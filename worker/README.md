# Telegram Webhook Bridge (Cloudflare Worker)

This worker makes Telegram command replies near-real-time without running an always-on server.

## What it does

1. Telegram sends webhook updates to this Worker.
2. Worker forwards the full Telegram update to GitHub `repository_dispatch` (`telegram_update`).
3. `.github/workflows/telegram-commands.yml` runs immediately and replies via bot without polling Telegram.

## Files

- `telegram-webhook-worker.js` contains the runtime code.
- `wrangler.toml` contains the deploy config. Update the `name` field before your first deploy.

## Deploy

1. Install Node.js if it is not already available.
2. From the `worker/` directory, authenticate Wrangler:

```bash
npx wrangler login
```

3. Set the required secrets:

```bash
npx wrangler secret put WEBHOOK_SECRET
npx wrangler secret put GITHUB_OWNER
npx wrangler secret put GITHUB_REPO
npx wrangler secret put GITHUB_DISPATCH_TOKEN
```

4. Deploy the Worker from the `worker/` directory:

```bash
npx wrangler deploy
```

5. Copy the Worker URL, for example `https://telegram-webhook-bridge.<subdomain>.workers.dev`, then register the Telegram webhook:

```bash
curl "https://api.telegram.org/bot$TELEGRAM_BOT_TOKEN/setWebhook?url=https://YOUR-WORKER-URL/telegram/$WEBHOOK_SECRET"
```

## Verify

Send `/update` to your bot. GitHub Actions `Telegram Commands` should start immediately (event `repository_dispatch`) and respond quickly.

If you already deployed an older version of the Worker, redeploy it after updating `worker/telegram-webhook-worker.js` so the full Telegram payload is forwarded.

## Local Notes

For local development, keep any `.dev.vars` file untracked. This repo ignores `worker/.dev.vars*` by default so secrets do not get committed.
