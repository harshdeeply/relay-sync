"""Optional HTTPS target adapter. The receiver must deduplicate X-Idempotency-Key."""
import json
from urllib.error import HTTPError, URLError
from urllib.parse import urlparse
from urllib.request import Request, build_opener, HTTPRedirectHandler

from .core import PermanentFailure, TemporaryFailure


class NoRedirects(HTTPRedirectHandler):
    def redirect_request(self, request, fp, code, msg, headers, newurl):
        return None


def http_adapter(url, token, opener=None):
    parsed = urlparse(url)
    if parsed.scheme != "https" or not parsed.netloc or parsed.username or parsed.password or parsed.fragment:
        raise ValueError("partner URL must be HTTPS without embedded credentials")
    if not token:
        raise ValueError("partner token is required")
    opener = opener or build_opener(NoRedirects())

    def send(event):
        body = json.dumps(event, sort_keys=True, separators=(",", ":")).encode()
        request = Request(url, body, {
            "Authorization": f"Bearer {token}", "Content-Type": "application/json",
            "X-Idempotency-Key": event["event_id"],
        }, method="POST")
        try:
            with opener.open(request, timeout=10) as response:
                if not 200 <= response.status < 300:
                    raise TemporaryFailure(f"unexpected partner response {response.status}")
        except HTTPError as exc:
            if exc.code in (408, 425, 429) or exc.code >= 500:
                raise TemporaryFailure(f"partner HTTP {exc.code}") from exc
            raise PermanentFailure(f"partner HTTP {exc.code}") from exc
        except (URLError, TimeoutError, OSError) as exc:
            raise TemporaryFailure("partner connection failed") from exc

    return send
