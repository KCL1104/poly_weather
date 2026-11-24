import httpx
import pytest

from polyweather.util import http


def _client(responses):
    calls = []

    def handler(request):
        calls.append(request)
        return responses[min(len(calls), len(responses)) - 1]

    return httpx.Client(transport=httpx.MockTransport(handler)), calls


@pytest.fixture
def sleeps(monkeypatch):
    waited = []
    monkeypatch.setattr(http.time, "sleep", waited.append)
    return waited


def test_retry_waits_as_long_as_the_server_asks(sleeps):
    client, calls = _client([httpx.Response(503, headers={"retry-after": "1"}), httpx.Response(200, json={"ok": True})])
    assert http.request_with_retry(client, "GET", "https://example.test/x").json() == {"ok": True}
    assert len(calls) == 2 and 1.0 <= sleeps[0] <= 1.5


def test_retry_backs_off_when_the_server_names_no_wait(sleeps):
    client, calls = _client([httpx.Response(503), httpx.Response(503), httpx.Response(200, json={})])
    http.request_with_retry(client, "GET", "https://example.test/x", backoff=1.5)
    assert len(calls) == 3 and 0.75 <= sleeps[0] <= 2.25 and 1.5 <= sleeps[1] <= 4.5


def test_a_very_long_requested_wait_is_capped_and_a_date_is_ignored(sleeps):
    assert http._retry_after_seconds(httpx.Response(429, headers={"retry-after": "86400"})) == http.MAX_RETRY_AFTER_S
    assert http._retry_after_seconds(httpx.Response(503, headers={"retry-after": "Wed, 25 Nov 2026 07:28:00 GMT"})) is None
    assert http._retry_after_seconds(httpx.Response(503)) is None


def test_gives_up_after_the_last_retry(sleeps):
    client, calls = _client([httpx.Response(503, headers={"retry-after": "0"})])
    with pytest.raises(httpx.HTTPStatusError):
        http.request_with_retry(client, "GET", "https://example.test/x", retries=2)
    assert len(calls) == 3 and len(sleeps) == 2
