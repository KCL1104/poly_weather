"""Shared helpers for the retrospective study (see README.md in this directory)."""

from __future__ import annotations

import datetime as dt
import os
import sqlite3
from zoneinfo import ZoneInfo

import numpy as np
import pandas as pd

from polyweather import stations
from polyweather.resolution.settle import to_whole_unit

UTC = dt.timezone.utc
US = [s.icao for s in stations.in_scope("us")]
NBP_PUBLISH_LAG_MIN = 80  # bulletins appeared 62-80 minutes after cycle time in the archive


def study_until() -> dt.date | None:
    """Last observation date to use, from STUDY_UNTIL=YYYY-MM-DD; None means everything.

    The databases keep growing when they are rebuilt later. Setting this to the last
    date of the original run (2026-11-23) makes a rerun cover the same sample.
    """
    value = os.environ.get("STUDY_UNTIL")
    return dt.date.fromisoformat(value) if value else None


def until_iso() -> str:
    """``study_until`` as text for SQL comparisons with ISO dates."""
    return (study_until() or dt.date.max).isoformat()


def local_midnight_utc(station: str, day: dt.date) -> dt.datetime:
    zone = ZoneInfo(stations.get(station).tz)
    return dt.datetime.combine(day, dt.time(0), zone).astimezone(UTC)


def daily_extremes(conn: sqlite3.Connection, station: str, sources: tuple[str, ...] | None = None) -> pd.DataFrame:
    """Settlement-rule daily high/low in whole °F for every station-local day on record.

    Applies the market rule: routine reports only, hourly temperature remark first and
    body temperature otherwise, one report per observation time (AWC text preferred,
    then the latest seen), extreme of tenths °C, then convert and round half up.
    """
    q = "SELECT obs_time, metar_type, temp_tenths_c, source, first_seen_ms FROM metar_obs WHERE station=?"
    args: list = [station]
    if sources:
        q += f" AND source IN ({','.join('?' * len(sources))})"
        args += list(sources)
    df = pd.read_sql_query(q, conn, params=args)
    if df.empty:
        return pd.DataFrame(columns=["station", "date", "high_f", "low_f", "n"])
    df["rank"] = df["source"].str.startswith("awc").astype("int64") * 10**15 + df["first_seen_ms"]
    df = df.sort_values("rank").drop_duplicates("obs_time", keep="last")
    df = df[(df["metar_type"].fillna("METAR") != "SPECI") & df["temp_tenths_c"].notna()]
    t = pd.to_datetime(df["obs_time"], utc=True).dt.tz_convert(stations.get(station).tz)
    df["date"] = t.dt.date
    g = df.groupby("date")["temp_tenths_c"].agg(["max", "min", "count"]).reset_index()
    g["high_f"] = g["max"].astype(int).map(lambda v: to_whole_unit(v, "F"))
    g["low_f"] = g["min"].astype(int).map(lambda v: to_whole_unit(v, "F"))
    g["station"] = station
    if study_until():
        g = g[g["date"] <= study_until()]
    return g.rename(columns={"count": "n"})[["station", "date", "high_f", "low_f", "n"]]


def nbp_max_rows(conn: sqlite3.Connection) -> pd.DataFrame:
    """Every archived NBP daily-maximum row with the local date it refers to.

    The maximum labelled 00Z on UTC date D+1 is the daytime maximum of local date D
    for CONUS stations.
    """
    df = pd.read_sql_query(
        "SELECT station, cycle, valid_utc, fhr, mean, sd, p10, p25, p50, p75, p90 FROM nbm_txn WHERE kind='max'", conn
    )
    df["cycle"] = pd.to_datetime(df["cycle"], utc=True)
    df["valid"] = pd.to_datetime(df["valid_utc"], utc=True)
    df["date"] = (df["valid"] - pd.Timedelta(days=1)).dt.date
    df["available"] = df["cycle"] + pd.Timedelta(minutes=NBP_PUBLISH_LAG_MIN)
    if study_until():
        df = df[df["date"] <= study_until()]
    return df.dropna(subset=["p10", "p25", "p50", "p75", "p90"]).drop(columns=["valid_utc"])


def decayed_mean(values: np.ndarray, ages_days: np.ndarray, half_life: float) -> float:
    w = 0.5 ** (ages_days / half_life)
    return float(np.sum(w * values) / np.sum(w))
