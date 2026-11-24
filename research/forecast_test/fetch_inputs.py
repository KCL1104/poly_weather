"""Download historical forecasts for the forecast-only test (see README.md here).

Three sources with unambiguous issue times, so "what was available at the decision
time" can be reconstructed exactly:

  mos    GFS LAMP (hourly cycles) and GFS MOS station guidance, from the IEM archive
  ecmwf  ECMWF IFS high-resolution forecast (open data, stream ``oper``): 3-hour
         maximum of 2 m temperature at the station's nearest grid point
  hrrr   NOAA HRRR 3 km: hourly 2 m temperature at the nearest grid point
  nbs    NBM hourly deterministic bulletin (max/min), the cycle just before each
         decision time, from the public NBM archive

Everything lands in one SQLite file. Each step skips what is already stored.

Usage: python fetch_inputs.py <forecast_test.sqlite> {mos|ecmwf|hrrr|nbs} --start 2026-08-01 --end 2026-11-24
"""

from __future__ import annotations

import argparse
import csv
import datetime as dt
import io
import logging
import sqlite3
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from zoneinfo import ZoneInfo

import httpx

from polyweather import stations
from polyweather.util.http import make_client, request_with_retry
from polyweather.weather import ecmwf_open

UTC = dt.timezone.utc
US = stations.in_scope("us")
log = logging.getLogger("fetch")

SCHEMA = """
CREATE TABLE IF NOT EXISTS mos (
    station TEXT, model TEXT, runtime TEXT, ftime TEXT, n_x REAL, tmp REAL,
    PRIMARY KEY (station, model, runtime, ftime));
CREATE TABLE IF NOT EXISTS mos_done (station TEXT, model TEXT, chunk TEXT, PRIMARY KEY (station, model, chunk));
CREATE TABLE IF NOT EXISTS ecmwf_oper (
    station TEXT, run TEXT, step INTEGER, valid TEXT, param TEXT, value_c REAL,
    PRIMARY KEY (station, run, step, param));
CREATE TABLE IF NOT EXISTS ecmwf_done (run TEXT, step INTEGER, PRIMARY KEY (run, step));
CREATE TABLE IF NOT EXISTS hrrr (
    station TEXT, cycle TEXT, fhr INTEGER, valid TEXT, tmp_c REAL,
    PRIMARY KEY (station, cycle, fhr));
CREATE TABLE IF NOT EXISTS hrrr_done (cycle TEXT, fhr INTEGER, status TEXT, PRIMARY KEY (cycle, fhr));
CREATE TABLE IF NOT EXISTS nbs (
    station TEXT, cycle TEXT, valid TEXT, kind TEXT, txn INTEGER, PRIMARY KEY (station, cycle, valid));
CREATE TABLE IF NOT EXISTS nbs_done (cycle TEXT PRIMARY KEY, status TEXT);
"""


def iso(d: dt.datetime) -> str:
    return d.astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")


def days(start: dt.date, end: dt.date):
    d = start
    while d <= end:
        yield d
        d += dt.timedelta(days=1)


# ---- IEM MOS archive ------------------------------------------------------------------

MOS_URL = "https://mesonet.agron.iastate.edu/cgi-bin/request/mos.py"


def fetch_mos(conn: sqlite3.Connection, start: dt.date, end: dt.date, models=("LAV", "GFS")) -> None:
    with make_client(timeout=180) as client:
        for model in models:
            for st in US:
                chunk_start = start
                while chunk_start <= end:
                    chunk_end = min(end + dt.timedelta(days=1), chunk_start + dt.timedelta(days=16 if model == "LAV" else 62))
                    key = f"{chunk_start}:{chunk_end}"
                    if not conn.execute("SELECT 1 FROM mos_done WHERE station=? AND model=? AND chunk=?", (st.icao, model, key)).fetchone():
                        r = request_with_retry(
                            client, "GET", MOS_URL, retries=6, backoff=4.0,
                            params={"station": st.icao, "model": model, "sts": f"{chunk_start}T00:00Z", "ets": f"{chunk_end}T00:00Z", "format": "csv"},
                        )
                        rows = []
                        for row in csv.DictReader(io.StringIO(r.text)):
                            f = lambda v: float(v) if v not in ("", None) else None  # noqa: E731
                            rows.append((st.icao, model, row["runtime"][:16], row["ftime"][:16], f(row.get("n_x")), f(row.get("tmp"))))
                        conn.executemany("INSERT OR IGNORE INTO mos VALUES (?,?,?,?,?,?)", rows)
                        conn.execute("INSERT OR IGNORE INTO mos_done VALUES (?,?,?)", (st.icao, model, key))
                        conn.commit()
                        log.info("mos %s %s %s: %d rows", model, st.icao, key, len(rows))
                        time.sleep(1.5)
                    chunk_start = chunk_end


# ---- ECMWF high-resolution forecast ------------------------------------------------------

# Steps whose 3-hour windows cover 12Z-03Z of the target day, by which run would be the
# latest one published at each decision time (runs appear about 7 h 40 min after run time).
ECMWF_STEPS = {0: (15, 18, 21, 24, 27, 39, 42, 45, 48, 51), 12: (27, 30, 33, 36, 39)}
ECMWF_PARAMS = ("mx2t3",)  # maximum over the 3 hours ending at the step; the bucket throttles, so one field per step


def fetch_ecmwf(conn: sqlite3.Connection, start: dt.date, end: dt.date) -> None:
    points = tuple((s.lat, s.lon) for s in US)
    with make_client(timeout=120) as client:
        for day in days(start, end):
            for hour, steps in ECMWF_STEPS.items():
                run = dt.datetime(day.year, day.month, day.day, hour, tzinfo=UTC)
                for step in steps:
                    if conn.execute("SELECT 1 FROM ecmwf_done WHERE run=? AND step=?", (iso(run), step)).fetchone():
                        continue
                    entries = ecmwf_open.fetch_index(client, run, step, "oper")
                    rows = []
                    if entries is not None:
                        fields = ecmwf_open.select_fields(entries, step, ECMWF_PARAMS)
                        for field, message in ecmwf_open.fetch_messages(client, ecmwf_open.file_url(run, step, "oper"), fields):
                            _, kelvin = ecmwf_open.decode_points(message, points)
                            for st, k in zip(US, kelvin):
                                if k is not None:
                                    rows.append((st.icao, iso(run), step, iso(run + dt.timedelta(hours=step)), field.param, round(k - 273.15, 2)))
                    conn.executemany("INSERT OR IGNORE INTO ecmwf_oper VALUES (?,?,?,?,?,?)", rows)
                    conn.execute("INSERT OR IGNORE INTO ecmwf_done VALUES (?,?)", (iso(run), step))
                    conn.commit()
            log.info("ecmwf %s done", day)


# ---- HRRR ----------------------------------------------------------------------------------

HRRR = "https://noaa-hrrr-bdp-pds.s3.amazonaws.com"
_local = threading.local()


def _client() -> httpx.Client:
    if not hasattr(_local, "client"):
        _local.client = make_client(timeout=120)
    return _local.client


def hrrr_wanted(start: dt.date, end: dt.date) -> list[tuple[dt.datetime, int]]:
    """(cycle, forecast hour) pairs covering 12Z-03Z of each target day for the three decision times."""
    wanted: set[tuple[dt.datetime, int]] = set()
    for day in days(start, end):
        window_start = dt.datetime(day.year, day.month, day.day, 12, tzinfo=UTC)
        window_end = window_start + dt.timedelta(hours=15)
        prev = day - dt.timedelta(days=1)
        cycles = {dt.datetime(prev.year, prev.month, prev.day, 6, tzinfo=UTC), dt.datetime(prev.year, prev.month, prev.day, 18, tzinfo=UTC)}
        for st in US:  # same-day cycle: two hours before 08:00 local
            local8 = dt.datetime.combine(day, dt.time(8), ZoneInfo(st.tz)).astimezone(UTC)
            cycles.add((local8 - dt.timedelta(hours=2)).replace(minute=0))
        for cycle in cycles:
            limit = 48 if cycle.hour % 6 == 0 else 18
            valid = max(window_start, cycle + dt.timedelta(hours=1))
            while valid <= window_end:
                fhr = int((valid - cycle).total_seconds() // 3600)
                if fhr <= limit:
                    wanted.add((cycle, fhr))
                valid += dt.timedelta(hours=1)
    return sorted(wanted)


def _hrrr_download(item: tuple[dt.datetime, int]):
    cycle, fhr = item
    url = f"{HRRR}/hrrr.{cycle:%Y%m%d}/conus/hrrr.t{cycle:%H}z.wrfsfcf{fhr:02d}.grib2"
    try:
        client = _client()
        r = request_with_retry(client, "GET", url + ".idx", ok=(200, 403, 404), retries=3)
        if r.status_code != 200:
            return item, "missing", None
        lines = r.text.splitlines()
        for i, line in enumerate(lines):
            if ":TMP:2 m above ground:" in line:
                start = int(line.split(":")[1])
                end = int(lines[i + 1].split(":")[1]) - 1 if i + 1 < len(lines) else None
                rng = f"bytes={start}-{end}" if end is not None else f"bytes={start}-"
                body = request_with_retry(client, "GET", url, ok=(206,), retries=3, headers={"Range": rng}).content
                return item, "ok", body
        return item, "no_field", None
    except Exception as e:  # noqa: BLE001 - recorded per file, the run goes on
        return item, f"error: {type(e).__name__}", None


def fetch_hrrr(conn: sqlite3.Connection, start: dt.date, end: dt.date) -> None:
    import eccodes

    done = {(r[0], r[1]) for r in conn.execute("SELECT cycle, fhr FROM hrrr_done WHERE status IN ('ok', 'missing', 'no_field')")}
    todo = [w for w in hrrr_wanted(start, end) if (iso(w[0]), w[1]) not in done]
    log.info("hrrr: %d fields to fetch", len(todo))
    nearest: list[int] | None = None
    n = 0
    with ThreadPoolExecutor(8) as pool:
        for (cycle, fhr), status, body in pool.map(_hrrr_download, todo):
            rows = []
            if body is not None:
                handle = eccodes.codes_new_from_message(body)
                try:
                    if nearest is None:  # the grid never changes; locate each station once
                        nearest = [eccodes.codes_grib_find_nearest(handle, s.lat, s.lon)[0]["index"] for s in US]
                    values = eccodes.codes_get_values(handle)
                    valid = cycle + dt.timedelta(hours=fhr)
                    rows = [(s.icao, iso(cycle), fhr, iso(valid), round(float(values[k]) - 273.15, 2)) for s, k in zip(US, nearest)]
                finally:
                    eccodes.codes_release(handle)
            conn.executemany("INSERT OR IGNORE INTO hrrr VALUES (?,?,?,?,?)", rows)
            conn.execute("INSERT OR REPLACE INTO hrrr_done VALUES (?,?,?)", (iso(cycle), fhr, status))
            n += 1
            if n % 200 == 0:
                conn.commit()
                log.info("hrrr: %d / %d", n, len(todo))
    conn.commit()


# ---- NBM hourly deterministic bulletin (NBS) ---------------------------------------------------

NBS_LAG = dt.timedelta(minutes=45)  # bulletins appear about 40 minutes after cycle time


def nbs_wanted(start: dt.date, end: dt.date) -> list[dt.datetime]:
    """The last cycle published before each station's three decision times."""
    wanted: set[dt.datetime] = set()
    for day in days(start, end):
        for st in US:
            for offset, hour in ((-1, 8), (-1, 18), (0, 8)):
                t = dt.datetime.combine(day + dt.timedelta(days=offset), dt.time(hour), ZoneInfo(st.tz)).astimezone(UTC)
                wanted.add((t - NBS_LAG).replace(minute=0, second=0, microsecond=0))
    return sorted(wanted)


def _nbs_download(cycle: dt.datetime):
    from polyweather.weather import nbm

    try:
        got = nbm.fetch_cycle(_client(), cycle, "nbs", {s.icao for s in US}, (nbm.S3,))
    except Exception as e:  # noqa: BLE001
        return cycle, f"error: {type(e).__name__}", []
    if got is None:
        return cycle, "missing", []
    rows = []
    for block in got[0]:
        for valid, _fhr, kind, value in nbm.parse_nbs_txn(block):
            rows.append((block.station, iso(cycle), iso(valid), kind, value))
    return cycle, "ok", rows


def fetch_nbs(conn: sqlite3.Connection, start: dt.date, end: dt.date) -> None:
    done = {r[0] for r in conn.execute("SELECT cycle FROM nbs_done WHERE status IN ('ok', 'missing')")}
    todo = [c for c in nbs_wanted(start, end) if iso(c) not in done and c < dt.datetime.now(UTC) - dt.timedelta(hours=1)]
    log.info("nbs: %d cycles to fetch", len(todo))
    n = 0
    with ThreadPoolExecutor(8) as pool:
        for cycle, status, rows in pool.map(_nbs_download, todo):
            conn.executemany("INSERT OR IGNORE INTO nbs VALUES (?,?,?,?,?)", rows)
            conn.execute("INSERT OR REPLACE INTO nbs_done VALUES (?,?)", (iso(cycle), status))
            n += 1
            if n % 100 == 0:
                conn.commit()
                log.info("nbs: %d / %d", n, len(todo))
    conn.commit()


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("db")
    ap.add_argument("what", choices=["mos", "ecmwf", "hrrr", "nbs"])
    ap.add_argument("--start", required=True)
    ap.add_argument("--end", required=True)
    args = ap.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(message)s")
    logging.getLogger("httpx").setLevel(logging.WARNING)
    conn = sqlite3.connect(args.db, timeout=120)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.executescript(SCHEMA)
    start, end = dt.date.fromisoformat(args.start), dt.date.fromisoformat(args.end)
    {"mos": fetch_mos, "ecmwf": fetch_ecmwf, "hrrr": fetch_hrrr, "nbs": fetch_nbs}[args.what](conn, start, end)
    log.info("%s finished", args.what)


if __name__ == "__main__":
    main()
