"""A first look at H2a with one-minute prices: is there any window after a report?

H2a says a bracket whose upper bound has been passed by an observed temperature stays
mispriced for a while. This lists every bracket ruled out by a new routine report and
reads its YES price around the moment aviationweather.gov received that report.

Prices are the exchange's one-minute midpoints (kept about 7 days), so this shows
whether a window exists at all; it cannot show executable prices. A midpoint of
0.0005 means the YES bid is zero, i.e. NO cannot be bought below 1.00.

Usage: python h2a_window.py <retro.sqlite> <prices_1m.parquet> [--since YYYY-MM-DD] [--fetch]
       --fetch downloads the one-minute history for the events first.
"""

from __future__ import annotations

import argparse
import datetime as dt
import sqlite3
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from zoneinfo import ZoneInfo

import numpy as np
import pandas as pd

from common import until_iso
from polyweather import stations
from polyweather.polymarket import prices
from polyweather.resolution.settle import to_whole_unit
from polyweather.util.http import make_client
from polyweather.util.timeutil import parse_iso

UTC = dt.timezone.utc
OFFSETS = {"at_obs": None, "-1 min": -60, "receipt": 0, "+1 min": 60, "+2 min": 120, "+5 min": 300, "+15 min": 900}
_local = threading.local()


def events(conn, since: str):
    return conn.execute(
        """SELECT e.event_id, s.station, s.obs_date, e.start_date, e.closed_time, e.end_date
           FROM events e JOIN resolution_specs s ON s.event_id = e.event_id
           WHERE s.status='ok' AND s.unit='F' AND s.measure='high' AND e.closed=1
                 AND s.obs_date>=? AND s.obs_date<=?""", (since, until_iso())).fetchall()


def fetch(conn, since: str, path: str) -> None:
    rows = conn.execute(
        """SELECT m.market_id, m.yes_token, e.start_date, e.closed_time, e.end_date
           FROM markets m JOIN events e ON e.event_id = m.event_id JOIN resolution_specs s ON s.event_id = e.event_id
           WHERE s.status='ok' AND s.unit='F' AND s.measure='high' AND e.closed=1
                 AND s.obs_date>=? AND s.obs_date<=?""", (since, until_iso())).fetchall()
    now = int(time.time())

    def work(r):
        if not hasattr(_local, "client"):
            _local.client = make_client()
        start = max(int(parse_iso(r["start_date"]).timestamp()), now - 8 * 86400)
        stop = int(parse_iso(r["closed_time"] or r["end_date"]).timestamp()) + 3600
        try:
            return r["market_id"], prices.fetch_history(_local.client, r["yes_token"], start, stop, 60)
        except Exception:  # noqa: BLE001 - a market without history is simply absent
            return r["market_id"], []

    cols: dict[str, list] = {"market_id": [], "ts": [], "price": []}
    with ThreadPoolExecutor(6) as pool:
        for market_id, pts in pool.map(work, rows):
            for ts, price, _ in pts:
                cols["market_id"].append(market_id)
                cols["ts"].append(ts)
                cols["price"].append(price)
    pd.DataFrame(cols).to_parquet(path, compression="zstd")


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("db")
    ap.add_argument("prices")
    ap.add_argument("--since", default=(dt.datetime.now(UTC) - dt.timedelta(days=7)).strftime("%Y-%m-%d"))
    ap.add_argument("--fetch", action="store_true")
    args = ap.parse_args()
    conn = sqlite3.connect(args.db)
    conn.row_factory = sqlite3.Row
    if args.fetch:
        fetch(conn, args.since, args.prices)
    px = pd.read_parquet(args.prices).sort_values(["market_id", "ts"])
    series = {m: (g["ts"].to_numpy(), g["price"].to_numpy()) for m, g in px.groupby("market_id")}

    def at(market_id: str, t: int) -> float:
        s = series.get(market_id)
        if s is None:
            return np.nan
        i = np.searchsorted(s[0], t, side="right") - 1
        return float(s[1][i]) if i >= 0 and t - s[0][i] <= 300 else np.nan

    rows, n_days = [], 0
    for e in events(conn, args.since):
        zone = ZoneInfo(stations.get(e["station"]).tz)
        day = dt.date.fromisoformat(e["obs_date"])
        start = dt.datetime.combine(day, dt.time(0), zone).astimezone(UTC)
        fmt = lambda d: d.strftime("%Y-%m-%dT%H:%M:%SZ")  # noqa: E731
        markets = conn.execute("SELECT market_id, hi FROM markets WHERE event_id=? ORDER BY group_index", (e["event_id"],)).fetchall()
        reports = conn.execute(
            """SELECT obs_time, temp_tenths_c, receipt_time FROM metar_obs
               WHERE station=? AND obs_time>=? AND obs_time<? AND source LIKE 'awc%' AND receipt_time IS NOT NULL
                 AND COALESCE(metar_type,'METAR') != 'SPECI' AND temp_tenths_c IS NOT NULL ORDER BY obs_time""",
            (e["station"], fmt(start), fmt(start + dt.timedelta(days=1)))).fetchall()
        n_days += bool(reports)
        running = None
        for r in reports:
            value = to_whole_unit(r["temp_tenths_c"], "F")
            before, running = running, value if running is None else max(running, value)
            if before is None or running == before:
                continue
            received = int(parse_iso(r["receipt_time"]).timestamp())
            observed = int(parse_iso(r["obs_time"]).timestamp())
            for m in markets:
                if m["hi"] is not None and before <= m["hi"] < running:  # ruled out by this report
                    row = {"station": e["station"], "date": e["obs_date"], "lag_s": received - observed}
                    for name, off in OFFSETS.items():
                        row[name] = at(m["market_id"], observed if off is None else received + off)
                    rows.append(row)
    df = pd.DataFrame(rows).dropna(subset=["-1 min", "+1 min"])
    cols = list(OFFSETS)
    pd.set_option("display.width", 200)
    print(f"city-days: {n_days} | brackets ruled out by a report: {len(df)} | "
          f"median seconds from observation to AWC receipt: {df['lag_s'].median():.0f}")
    summary = pd.DataFrame({
        "mean": df[cols].mean(), "median": df[cols].median(), "p90": df[cols].quantile(0.9), "max": df[cols].max(),
        "share >= 0.02": (df[cols] >= 0.02).mean(), "share >= 0.05": (df[cols] >= 0.05).mean(),
    })
    print("\nYES midpoint of the ruled-out bracket, by time relative to AWC receipt:")
    print(summary.round(4).to_string())
    out = Path(__file__).parent / "data"
    out.mkdir(exist_ok=True)
    summary.round(5).to_csv(out / "h2a_window.csv")


if __name__ == "__main__":
    main()
