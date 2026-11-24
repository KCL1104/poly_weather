import httpx
import pyarrow.parquet as pq

from polyweather.catalog import scanner
from polyweather.polymarket import prices
from polyweather.util import http


def test_price_archive_fetches_every_market_and_survives_one_that_fails(conn, nyc_event, tmp_path, monkeypatch):
    monkeypatch.setattr(http.time, "sleep", lambda s: None)
    scanner.ingest(conn, [nyc_event])
    event_id = conn.execute("SELECT event_id FROM events").fetchone()[0]
    markets = conn.execute("SELECT market_id, yes_token FROM markets WHERE event_id=? ORDER BY market_id", (event_id,)).fetchall()
    broken = markets[1]["yes_token"]
    start = int(prices.parse_iso(conn.execute("SELECT start_date FROM events").fetchone()[0]).timestamp())
    asked: list[str] = []

    def handler(request):
        token = request.url.params["token_id"]
        asked.append(token)
        if token == broken:
            return httpx.Response(503, headers={"retry-after": "0"}, json={"error": "request timed out"})
        return httpx.Response(200, json={"data": [
            {"timestamp": start + 60, "price": "0.25", "resolution_seconds": 60},
            {"timestamp": start + 120, "price": "0.26", "resolution_seconds": 60},
        ], "pagination": {"has_more": False}})

    client = httpx.Client(transport=httpx.MockTransport(handler))
    n, path = prices.archive_events(conn, client, tmp_path, [event_id], pause_s=0, workers=3)

    assert n == len(markets) and set(asked) == {m["yes_token"] for m in markets}
    table = pq.read_table(path).to_pydict()
    per_market = {m: table["market_id"].count(m) for m in set(table["market_id"])}
    assert per_market == {m["market_id"]: 2 for m in markets if m["yes_token"] != broken}
    assert table["market_id"] == sorted(table["market_id"], key=[m["market_id"] for m in markets].index)  # catalog order
    assert not list(tmp_path.rglob("*.tmp"))
