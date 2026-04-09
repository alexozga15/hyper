/**
 * Cloudflare Worker webhook bridge for Telegram -> GitHub Actions.
 *
 * Env vars required:
 * - WEBHOOK_SECRET: random string used in webhook path
 * - GITHUB_OWNER
 * - GITHUB_REPO
 * - GITHUB_DISPATCH_TOKEN (classic PAT with repo scope, or fine-grained token with Actions write)
 */

export default {
  async fetch(request, env) {
    if (request.method !== 'POST') {
      return new Response('Method Not Allowed', { status: 405 });
    }

    const url = new URL(request.url);
    const expectedPath = `/telegram/${env.WEBHOOK_SECRET}`;
    if (url.pathname !== expectedPath) {
      return new Response('Unauthorized', { status: 401 });
    }

    let body;
    try {
      body = await request.json();
    } catch {
      return new Response('Bad Request', { status: 400 });
    }

    const updateId = body?.update_id;
    const message = body?.message ?? body?.edited_message ?? null;
    const messageText = message?.text ?? '';
    const chatId = message?.chat?.id ?? null;

    const dispatchResp = await fetch(
      `https://api.github.com/repos/${env.GITHUB_OWNER}/${env.GITHUB_REPO}/dispatches`,
      {
        method: 'POST',
        headers: {
          Authorization: `Bearer ${env.GITHUB_DISPATCH_TOKEN}`,
          Accept: 'application/vnd.github+json',
          'Content-Type': 'application/json',
          'User-Agent': 'hyper-telegram-webhook',
        },
        body: JSON.stringify({
          event_type: 'telegram_update',
          client_payload: {
            update: body,
            update_id: updateId,
            text: messageText,
            chat_id: chatId,
          },
        }),
      }
    );

    if (!dispatchResp.ok) {
      const detail = await dispatchResp.text();
      return new Response(`Dispatch failed: ${detail}`, { status: 502 });
    }

    return new Response('ok', { status: 200 });
  },
};
