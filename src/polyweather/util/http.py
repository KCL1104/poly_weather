"""Small HTTP helpers shared by all read-only clients."""

from __future__ import annotations

import logging
import random
import time

import httpx

log = logging.getLogger(__name__)

RETRY_STATUS = {408, 425, 429, 500, 502, 503, 504}


def make_client(user_agent: str = "polyweather-research/0.1", timeout: float = 30.0) -> httpx.Client:
    # HTTP/1.1 only: some NOAA servers send malformed HTTP/2 headers.
    return httpx.Client(
        headers={"User-Agent": user_agent, "Accept-Encoding": "gzip"},
        timeout=httpx.Timeout(timeout, connect=15.0),
        follow_redirects=True,
        http2=False,
    )


def make_async_client(user_agent: str = "polyweather-research/0.1", timeout: float = 30.0) -> httpx.AsyncClient:
    return httpx.AsyncClient(
        headers={"User-Agent": user_agent, "Accept-Encoding": "gzip"},
        timeout=httpx.Timeout(timeout, connect=15.0),
        follow_redirects=True,
        http2=False,
    )


MAX_RETRY_AFTER_S = 60.0


def _retry_after_seconds(response: httpx.Response) -> float | None:
    """The Retry-After header as seconds, when it is a plain number."""
    try:
        return min(MAX_RETRY_AFTER_S, max(0.0, float(response.headers.get("retry-after", ""))))
    except ValueError:
        return None


def request_with_retry(
    client: httpx.Client,
    method: str,
    url: str,
    *,
    retries: int = 4,
    backoff: float = 1.5,
    ok: tuple[int, ...] = (200,),
    **kwargs,
) -> httpx.Response:
    """Issue a request, retrying transient failures with jittered backoff.

    Returns the response when its status is in `ok`; raises otherwise.
    """
    last: Exception | None = None
    for attempt in range(retries + 1):
        asked: float | None = None  # the wait the server asked for, if it named one
        try:
            r = client.request(method, url, **kwargs)
            if r.status_code in ok:
                return r
            if r.status_code not in RETRY_STATUS:
                r.raise_for_status()
                raise httpx.HTTPStatusError(f"unexpected status {r.status_code}", request=r.request, response=r)
            last = httpx.HTTPStatusError(f"status {r.status_code}", request=r.request, response=r)
            asked = _retry_after_seconds(r)
        except (httpx.TransportError, httpx.DecodingError) as e:
            last = e
        if attempt < retries:
            delay = backoff * (2**attempt) * (0.5 + random.random())
            if asked is not None:
                delay = asked + random.random() * 0.5
            log.warning("retry %s %s in %.1fs: %s", method, url, delay, last)
            time.sleep(delay)
    assert last is not None
    raise last


def get_json(client: httpx.Client, url: str, **kwargs):
    return request_with_retry(client, "GET", url, **kwargs).json()
