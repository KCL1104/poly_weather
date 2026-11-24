"""Download everything the retrospective study reads, into one SQLite file and one Parquet file.

  1. closed weather events since --since (Gamma), imported without freezing anything
  2. METAR history: AWC (about 30 days, all it serves) and IEM (--metar-start onward)
  3. NBM NBP bulletins from the public S3 archive (--nbm-start onward), US stations
  4. five-minute YES price history for the closed US markets (Data API)

Usage:
  python build_inputs.py retro.sqlite prices_5m_us.parquet \\
      --since 2026-10-01 --metar-start 2025-10-20 --nbm-start 2025-10-20 [--stations KLGA,KORD]

Takes roughly 25 minutes for 13 months and 11 stations; every step skips what it already has.
"""

from __future__ import annotations

import argparse
import datetime as dt
import logging
import threading
from concurrent.futures import ThreadPoolExecutor

import pyarrow as pa
import pyarrow.parquet as pq

from polyweather import stations
from polyweather.catalog import scanner
from polyweather.polymarket import gamma, prices
from polyweather.storage.db import connect
from polyweather.util.http import make_client
from polyweather.util.timeutil import UTC, parse_iso, utcnow
from polyweather.weather import awc, iem, nbm
from polyweather.weather.guidance import have_cycle, store_blocks

log = logging.getLogger("retro")
_local = threading.local()


def _client():
    if not hasattr(_local, "client"):
        _local.client = make_client(timeout=120)
    return _local.client


def fetch_nbm(conn, wanted: set[str], start: dt.date, end: dt.date) -> None:
    cycles = []
    day = start
    while day < end:
        for hour in nbm.NBP_CYCLES:
            c = dt.datetime(day.year, day.month, day.day, hour, tzinfo=UTC)
            if c < utcnow() - dt.timedelta(hours=2) and not have_cycle(conn, "nbp", c, wanted):
                cycles.append(c)
        day += dt.timedelta(days=1)
    log.info("nbm: %d cycles to fetch", len(cycles))

    def work(c):
        try:
            return c, nbm.fetch_cycle(_client(), c, "nbp", wanted, (nbm.S3,))
        except Exception as e:  # noqa: BLE001 - report and move on
            return c, e

    with ThreadPoolExecutor(8) as pool:
        for c, got in pool.map(work, cycles):
            if got is None or isinstance(got, Exception):
                log.warning("nbm: %s not available (%s)", c, got)
                continue
            blocks, published, source = got
            conn.execute("BEGIN")
            store_blocks(conn, "nbp", blocks, source, published)
            for b in blocks:
                nbm.store_txn(conn, nbm.parse_nbp(b))
            conn.execute("COMMIT")


def fetch_prices(conn, out_path: str, since: str, wanted: set[str]) -> None:
    rows = conn.execute(
        f"""SELECT m.market_id, m.yes_token, e.event_id, e.start_date, e.closed_time, e.end_date
            FROM markets m JOIN events e ON e.event_id = m.event_id JOIN resolution_specs s ON s.event_id = e.event_id
            WHERE s.status = 'ok' AND e.closed = 1 AND s.obs_date >= ? AND s.station IN ({','.join('?' * len(wanted))})""",
        (since, *sorted(wanted)),
    ).fetchall()
    log.info("prices: %d tokens", len(rows))

    def work(r):
        start = int(parse_iso(r["start_date"]).timestamp())
        stop = int(parse_iso(r["closed_time"] or r["end_date"]).timestamp()) + 3600
        try:
            return r, prices.fetch_history(_client(), r["yes_token"], start, stop, 300)
        except Exception as e:  # noqa: BLE001
            return r, e

    cols: dict[str, list] = {"market_id": [], "event_id": [], "ts": [], "price": []}
    with ThreadPoolExecutor(6) as pool:
        for r, pts in pool.map(work, rows):
            if isinstance(pts, Exception):
                log.warning("prices: market %s failed (%s)", r["market_id"], pts)
                continue
            for ts, price, _ in pts:
                cols["market_id"].append(r["market_id"])
                cols["event_id"].append(r["event_id"])
                cols["ts"].append(ts)
                cols["price"].append(price)
    pq.write_table(pa.table(cols), out_path, compression="zstd")


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("db")
    ap.add_argument("prices")
    ap.add_argument("--since", required=True, help="first event date to import, YYYY-MM-DD")
    ap.add_argument("--metar-start", required=True)
    ap.add_argument("--nbm-start", required=True)
    ap.add_argument("--stations", default=",".join(s.icao for s in stations.in_scope("us")))
    args = ap.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(message)s")
    logging.getLogger("httpx").setLevel(logging.WARNING)

    conn = connect(args.db)
    wanted = set(args.stations.split(","))
    sts = [stations.get(s) for s in sorted(wanted)]
    today = utcnow().date()
    with make_client(timeout=120) as client:
        r = scanner.ingest(conn, gamma.weather_events(client, closed=True, end_date_min=f"{args.since}T00:00:00Z"), detect_changes=False)
        log.info("catalog: %d closed events, %d unparsed", r.events_seen, r.unparsed)
        log.info("awc: %d new reports", awc.backfill(conn, client, sorted(wanted), days=31.5))
        start = dt.date.fromisoformat(args.metar_start)
        log.info("iem: %d new reports", iem.backfill(conn, client, sts, start, today + dt.timedelta(days=1)))
    fetch_nbm(conn, wanted, dt.date.fromisoformat(args.nbm_start), today + dt.timedelta(days=1))
    fetch_prices(conn, args.prices, args.since, wanted)
    from polyweather.ops import settlement_check

    s = settlement_check.run(conn, since=args.since, stations=sorted(wanted))
    log.info("settlement check: %d matched, %d mismatched, %d without data", s.matched, s.mismatched, s.no_data)


if __name__ == "__main__":
    main()
