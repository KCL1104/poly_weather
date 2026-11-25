"""H2b: the rest of the day, from LAMP's hourly temperatures.

For the brackets the observations have not ruled out, the day's final high is

    max(high observed so far, highest temperature still to come)

LAMP gives an hourly temperature forecast every hour. The highest of its values for
the hours left in the local day is the forecast of what is still to come. Its error
is taken from the same station and the same cycle hour over the past 30 days: what
the reports after that cycle actually reached, minus what that cycle had forecast.
The errors are summarised by their mean and standard deviation (normal distribution,
standard deviation floored), which turns the forecast into a probability for every
bracket under the market's rounding rule (spec, "當日 nowcast").

The ledger then follows the shared rules with a 5 percentage point threshold, quarter
Kelly capped at 20 USD, no NO above 97 cents, and no new position after 20:00 local
time (plan). A station needs 14 past days for a cycle hour before that hour trades.
"""

from __future__ import annotations

import datetime as dt
import math
import sqlite3
import statistics
from zoneinfo import ZoneInfo

from .. import stations
from ..util.timeutil import UTC, parse_iso
from . import h2a
from .ledger import Candidate, CityDay, LedgerRules

RULES = LedgerRules(name="H2b", model_version="h2b-lamp-1", min_edge=0.05, max_trade_usd=20.0, kelly_fraction=0.25,
                    max_price={"NO": 0.97})
HISTORY_DAYS = 30
MIN_HISTORY = 14
SD_FLOOR_F = 0.75
LAST_ENTRY_LOCAL_HOUR = 20
MAX_CYCLE_AGE = dt.timedelta(hours=3)
ASSUMED_AVAILABILITY = dt.timedelta(minutes=5)  # for cycles filled in from an archive, after the cycle time
MIN_COVERAGE = 0.8  # share of the remaining hours that must have a report for a past day to count


def _iso(t: dt.datetime) -> str:
    return t.astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")


def exact_f(tenths_c: int) -> float:
    return tenths_c * 0.18 + 32.0


def forecast_ahead(conn: sqlite3.Connection, station: str, cycle: dt.datetime, after: dt.datetime, day_end: dt.datetime,
                   seen_until_ms: int) -> float | None:
    """Highest LAMP temperature (°F) of ``cycle`` for the hours after ``after`` up to the end of the local day,
    from what this system had stored by ``seen_until_ms``."""
    row = conn.execute(
        """SELECT MAX(tmp_f) FROM lamp_hourly WHERE station=? AND cycle=? AND valid_utc>? AND valid_utc<=? AND tmp_f IS NOT NULL
           AND COALESCE(seen_ms, 0) <= ?""",
        (station, _iso(cycle), _iso(after), _iso(day_end), seen_until_ms),
    ).fetchone()
    return None if row[0] is None else float(row[0])


def reached_after(conn: sqlite3.Connection, station: str, after: dt.datetime, day_end: dt.datetime,
                  seen_until_ms: int) -> tuple[float | None, int]:
    """Highest counted temperature (exact °F) observed after ``after`` in that local day, and how many reports."""
    row = conn.execute(
        """SELECT MAX(temp_tenths_c), COUNT(DISTINCT obs_time) FROM metar_obs
           WHERE station=? AND obs_time>? AND obs_time<? AND temp_tenths_c IS NOT NULL AND COALESCE(metar_type, 'METAR') != 'SPECI'
             AND first_seen_ms <= ?""",
        (station, _iso(after), _iso(day_end), seen_until_ms),
    ).fetchone()
    return (None if row[0] is None else exact_f(row[0])), int(row[1])


def _local_day_end(station: str, moment: dt.datetime) -> dt.datetime:
    zone = ZoneInfo(stations.get(station).tz)
    local = moment.astimezone(zone)
    return dt.datetime.combine(local.date() + dt.timedelta(days=1), dt.time(0), zone).astimezone(UTC)


def past_errors(conn: sqlite3.Connection, station: str, cycle: dt.datetime, seen_until_ms: int) -> list[float]:
    """Observed minus forecast for the same cycle hour on each of the past ``HISTORY_DAYS`` days, using only
    what was in the archive by ``seen_until_ms`` (history fetched later must not change an earlier decision)."""
    out = []
    for k in range(1, HISTORY_DAYS + 1):
        past = cycle - dt.timedelta(days=k)
        row = conn.execute("SELECT fetched_ms FROM guidance_blocks WHERE product='lav' AND station=? AND cycle=?", (station, _iso(past))).fetchone()
        available = dt.datetime.fromtimestamp(row[0] / 1000, UTC) if row else past + ASSUMED_AVAILABILITY
        if available - past > MAX_CYCLE_AGE:  # archived late, in a catch-up: the forecast itself is still what was issued
            available = past + ASSUMED_AVAILABILITY
        end = _local_day_end(station, available)
        hours_left = (end - available).total_seconds() / 3600
        if hours_left < 1:
            continue
        forecast = forecast_ahead(conn, station, past, available, end, seen_until_ms)
        reached, n = reached_after(conn, station, available, end, seen_until_ms)
        if forecast is None or reached is None or n < MIN_COVERAGE * math.floor(hours_left):
            continue
        out.append(reached - forecast)
    return out


def _normal_cdf(z: float) -> float:
    return 0.5 * (1.0 + math.erf(z / math.sqrt(2.0)))


def bracket_probabilities(city_day: CityDay, observed_exact_f: float | None, center_f: float, sd_f: float) -> dict[str, float]:
    """P(the day's high lands in each bracket). The high is max(observed, a normal around ``center_f``),
    and a whole-degree bracket [lo, hi] covers exact values from lo - 0.5 up to hi + 0.5."""

    def below(x: float) -> float:  # P(final high < x)
        if observed_exact_f is not None and x <= observed_exact_f:
            return 0.0
        return _normal_cdf((x - center_f) / sd_f)

    out = {}
    for market, bracket in city_day.markets:
        upper = 1.0 if bracket.hi is None else below(bracket.hi + 0.5)
        lower = 0.0 if bracket.lo is None else below(bracket.lo - 0.5)
        out[market["market_id"]] = max(0.0, upper - lower)
    return out


def candidates(conn: sqlite3.Connection, city_day: CityDay, ts_ms: int, cycle: dt.datetime, run: str) -> tuple[list[Candidate], dict]:
    """Candidates for one decision, or none with the reason in ``info['skipped']``."""
    now = dt.datetime.fromtimestamp(ts_ms / 1000, UTC)
    info: dict = {"cycle": _iso(cycle)}
    local = now.astimezone(ZoneInfo(city_day.spec.tz or "UTC"))
    if local.hour >= LAST_ENTRY_LOCAL_HOUR:
        return [], {**info, "skipped": "after 20:00 local time"}
    if now - cycle > MAX_CYCLE_AGE:
        return [], {**info, "skipped": "forecast cycle too old"}
    ahead = forecast_ahead(conn, city_day.station, cycle, now, city_day.day_end, ts_ms)
    if ahead is None:
        return [], {**info, "skipped": "no forecast hours left in the day"}
    errors = past_errors(conn, city_day.station, cycle, ts_ms)
    if len(errors) < MIN_HISTORY:
        return [], {**info, "skipped": f"only {len(errors)} past days for this cycle hour (needs {MIN_HISTORY})"}
    mean, sd = statistics.fmean(errors), max(SD_FLOOR_F, statistics.pstdev(errors))
    high = h2a.observed_high(conn, city_day, ts_ms)
    observed = None if high.value_tenths_c is None else exact_f(high.value_tenths_c)
    probs = bracket_probabilities(city_day, observed, ahead + mean, sd)
    info.update(forecast_ahead_f=ahead, error_mean_f=round(mean, 3), error_sd_f=round(sd, 3), history_days=len(errors),
                observed_high=high.value_unit, observed_exact_f=None if observed is None else round(observed, 2))

    ruled_out = {m["market_id"] for m in h2a.ruled_out(city_day, high.value_unit)}
    held_yes = {r[0] for r in conn.execute(
        """SELECT m.group_index FROM paper_fills f JOIN markets m ON m.market_id = f.market_id
           WHERE f.run=? AND f.ledger=? AND f.variant='conservative' AND f.event_id=? AND f.side='YES'""",
        (run, RULES.name, city_day.event_id))}
    out = []
    for market, _bracket in city_day.markets:
        if market["market_id"] in ruled_out:
            continue  # H2a's territory: no model needed there
        p = probs[market["market_id"]]
        detail = {k: info[k] for k in ("cycle", "forecast_ahead_f", "error_mean_f", "error_sd_f", "history_days", "observed_high")}
        # never YES on two neighbouring brackets of one city-day: at most one of them can win
        if not ({market["group_index"] - 1, market["group_index"] + 1} & held_yes):
            out.append(Candidate(market, "YES", p, dict(detail)))
        out.append(Candidate(market, "NO", 1.0 - p, dict(detail)))
    return out, info


def newest_cycle(conn: sqlite3.Connection, station: str, ts_ms: int) -> dt.datetime | None:
    """The latest LAMP cycle this system had archived for the station by ``ts_ms``."""
    row = conn.execute(
        "SELECT MAX(cycle) FROM guidance_blocks WHERE product='lav' AND station=? AND fetched_ms<=?", (station, ts_ms)
    ).fetchone()
    return parse_iso(row[0]) if row and row[0] else None
