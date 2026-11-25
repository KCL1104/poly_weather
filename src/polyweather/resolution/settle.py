"""Reconstruct a market's qualifying value from archived METAR reports.

This is the single code path for both the after-the-fact settlement check and the
intraday running extreme (H2), so the two can never disagree about which reports
count or how a value is rounded.
"""

from __future__ import annotations

import datetime as dt
from dataclasses import dataclass
from typing import Iterable, Sequence

from ..catalog.brackets import Bracket, find_bracket
from .metar import parse_metar
from .spec import ResolutionSpec


@dataclass(frozen=True)
class Obs:
    """One archived report. ``seen_order`` breaks ties between versions of one obs time."""

    obs_time: dt.datetime  # UTC
    raw_text: str
    label: str | None = None  # 'METAR' / 'SPECI' as stated by the source, if known
    seen_order: int = 0


@dataclass(frozen=True)
class Settlement:
    n_reports: int  # reports in the local day after de-duplication
    n_counted: int  # of those, routine reports carrying a temperature
    value_tenths_c: int | None
    value_unit: int | None  # whole degrees in the market's unit
    deciding: tuple[str, ...] = ()  # raw text of the report(s) holding the extreme
    deciding_time: dt.datetime | None = None  # earliest obs time that reached the extreme
    # Longest stretch of the day (up to ``until``) with no counted report, in minutes:
    # from the start of the day, between reports, and to the end. A day with a long
    # hole can have missed its extreme.
    largest_hole_min: float | None = None


def to_whole_unit(tenths_c: int, unit: str) -> int:
    """Apply the market's conversion and round-half-up rule using integer arithmetic."""
    if unit == "F":
        # °F = °C * 9/5 + 32; in hundredths of a degree that is 18*t + 3200.
        return (18 * tenths_c + 3200 + 50) // 100
    if unit == "C":
        return (tenths_c + 5) // 10
    raise ValueError(f"unknown unit {unit!r}")


def _is_special(o: Obs) -> bool:
    label = o.label or parse_metar(o.raw_text).label
    return label == "SPECI"


def settle(spec: ResolutionSpec, observations: Iterable[Obs], *, until: dt.datetime | None = None) -> Settlement:
    """Qualifying value for ``spec`` from ``observations``.

    ``until`` limits the computation to reports observed at or before that instant,
    which gives the running extreme for the day so far.
    """
    if not spec.ok:
        raise ValueError("cannot settle an unparsed spec")
    day_start, day_end = spec.local_day_bounds_utc()

    # "The report the resolution source shows for a given observation time is the one
    # that counts": keep the most recently seen version of each observation time.
    latest: dict[dt.datetime, Obs] = {}
    for o in observations:
        if not (day_start <= o.obs_time < day_end):
            continue
        if until is not None and o.obs_time > until:
            continue
        cur = latest.get(o.obs_time)
        if cur is None or o.seen_order >= cur.seen_order:
            latest[o.obs_time] = o

    counted: list[tuple[int, Obs]] = []
    for o in latest.values():
        if _is_special(o):
            continue
        temp = parse_metar(o.raw_text).temp_tenths_c
        if temp is None:
            continue
        counted.append((temp, o))

    if not counted:
        return Settlement(len(latest), 0, None, None)

    pick = max if spec.measure == "high" else min
    extreme = pick(t for t, _ in counted)
    holders = sorted((o for t, o in counted if t == extreme), key=lambda o: o.obs_time)
    end = day_end if until is None else min(day_end, until)
    edges = [day_start, *sorted(o.obs_time for _, o in counted), end]
    return Settlement(
        n_reports=len(latest),
        n_counted=len(counted),
        value_tenths_c=extreme,
        value_unit=to_whole_unit(extreme, spec.unit or ""),
        deciding=tuple(o.raw_text for o in holders),
        deciding_time=holders[0].obs_time,
        largest_hole_min=max((b - a).total_seconds() for a, b in zip(edges, edges[1:])) / 60,
    )


def bracket_for(value_unit: int, brackets: Sequence[Bracket]) -> int | None:
    return find_bracket(brackets, value_unit)
