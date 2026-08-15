from __future__ import annotations

import json
import os
import urllib.error
import urllib.parse
import urllib.request
from typing import Any

from ratelimit import RequestRateLimiter

# Moni answers a handful of requests per refresh, so this is not a throughput
# control. It exists because any Moni error - a 429 among them - parks social
# context for six hours (MONI_ERROR_BACKOFF_MINUTES), and the requests are
# issued back to back with nothing between them.
MONI_REQUESTS_PER_SECOND = float(os.environ.get("MONI_REQUESTS_PER_SECOND", "2"))
GLOBAL_MONI_RATE_LIMITER = RequestRateLimiter(MONI_REQUESTS_PER_SECOND)


class MoniApiError(RuntimeError):
    pass


class MoniClient:
    BASE_URL = "https://api.discover.getmoni.io/api/v3"

    def __init__(
        self,
        api_key: str | None = None,
        timeout: int = 20,
        rate_limiter: RequestRateLimiter | None = None,
    ) -> None:
        self.api_key = (api_key if api_key is not None else os.environ.get("MONI_API_KEY", "")).strip()
        self.timeout = timeout
        self.rate_limiter = rate_limiter or GLOBAL_MONI_RATE_LIMITER

    @property
    def enabled(self) -> bool:
        return bool(self.api_key)

    def get(self, path: str, params: dict[str, Any] | None = None) -> dict[str, Any]:
        if not self.enabled:
            raise MoniApiError("Missing MONI_API_KEY")
        query = urllib.parse.urlencode(params or {})
        url = f"{self.BASE_URL}/{path.lstrip('/')}"
        if query:
            url = f"{url}?{query}"
        request = urllib.request.Request(
            url,
            headers={"Accept": "application/json", "Api-Key": self.api_key},
        )
        self.rate_limiter.wait()
        try:
            with urllib.request.urlopen(request, timeout=self.timeout) as response:
                payload = json.load(response)
        except urllib.error.HTTPError as exc:
            detail = exc.read().decode("utf-8", errors="replace")[:500]
            raise MoniApiError(f"Moni API returned HTTP {exc.code}: {detail}") from exc
        except (urllib.error.URLError, TimeoutError, ValueError) as exc:
            raise MoniApiError(f"Moni API request failed: {exc}") from exc
        if not isinstance(payload, dict):
            raise MoniApiError("Moni API returned an invalid response")
        return payload

    def api_key_status(self) -> dict[str, Any]:
        return self.get("status/api_key/")

    def smart_mentions_history(self, username: str, timeframe: str) -> dict[str, Any]:
        handle = username.strip().lstrip("@")
        if not handle:
            raise MoniApiError("Moni account handle is required")
        return self.get(
            f"accounts/{urllib.parse.quote(handle, safe='')}/history/smart_mentions_count/",
            {"timeframe": timeframe},
        )
