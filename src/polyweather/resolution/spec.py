"""Turn a market's rule text into a structured ResolutionSpec.

Parsing is deliberately strict. The full rule text must match, word for word, one of
the known templates in ``templates/``; only the station name, station code,
observation date and source URL may vary. Anything else (a reworded sentence, a new
source, a station missing from the registry, a window that does not cover the local
day) yields ``status='unparsed'`` with reasons, and an unparsed event never reaches a
strategy. A human then reads the new wording and, if the meaning is unchanged, adds a
template.

What the templates say is encoded once, in ``TEMPLATE_RULES``, and verified against
real settlements by the settlement check.
"""

from __future__ import annotations

import datetime as dt
import hashlib
import json
import re
from dataclasses import asdict, dataclass, field
from functools import lru_cache
from importlib import resources
from zoneinfo import ZoneInfo

from .. import stations
from ..util.timeutil import UTC, iso, parse_iso

PARSER_VERSION = "1"

_PLACEHOLDERS = ("station_name", "icao", "obs_date", "source_url")
_PLACEHOLDER_PATTERNS = {
    "station_name": r"[^\n()]{2,80}?",
    "icao": r"[A-Z][A-Z0-9]{3}",
    "obs_date": r"\d{1,2} [A-Z][a-z]{2} '\d{2}",
    "source_url": r"https://\S+",
}

AWC_URL = re.compile(
    r"^https://aviationweather\.gov/api/data/metar\?ids=(?P<icao>[A-Z][A-Z0-9]{3})&format=raw"
    r"&date=(?P<end>\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z)&hours=(?P<hours>\d{1,3})$"
)

# Semantics of each known template. Every template in this family shares the same
# report-selection and value rules; they differ only in measure and unit.
_COMMON_RULES = {
    "source_type": "awc_metar_raw",
    "excluded_labels": ["SPECI"],
    "value_order": ["tgroup_tenths_c", "body_whole_c"],
    "day_rule": "station_local_calendar_day",
    "rounding": "half_up",
    "duplicate_obs_time": "latest_shown",
}
TEMPLATE_RULES: dict[str, dict] = {
    "awc_metar_high_f_v1": {**_COMMON_RULES, "measure": "high", "unit": "F"},
    "awc_metar_low_f_v1": {**_COMMON_RULES, "measure": "low", "unit": "F"},
    "awc_metar_high_c_v1": {**_COMMON_RULES, "measure": "high", "unit": "C"},
    "awc_metar_low_c_v1": {**_COMMON_RULES, "measure": "low", "unit": "C"},
}


@dataclass(frozen=True)
class ResolutionSpec:
    status: str  # 'ok' or 'unparsed'
    reasons: tuple[str, ...] = ()
    template_id: str | None = None
    station: str | None = None
    station_name: str | None = None
    measure: str | None = None  # 'high' / 'low'
    unit: str | None = None  # 'F' / 'C'
    obs_date: str | None = None  # station-local calendar date, YYYY-MM-DD
    tz: str | None = None
    source_type: str | None = None
    source_url: str | None = None
    window_start_utc: str | None = None
    window_end_utc: str | None = None
    rules: dict = field(default_factory=dict)

    @property
    def ok(self) -> bool:
        return self.status == "ok"

    def to_json(self) -> str:
        return json.dumps(asdict(self), sort_keys=True, ensure_ascii=False)

    @staticmethod
    def from_json(s: str) -> "ResolutionSpec":
        d = json.loads(s)
        d["reasons"] = tuple(d.get("reasons") or ())
        return ResolutionSpec(**d)

    def local_day_bounds_utc(self) -> tuple[dt.datetime, dt.datetime]:
        """[start, end) of the station-local observation date, in UTC."""
        assert self.obs_date and self.tz
        zone = ZoneInfo(self.tz)
        day = dt.date.fromisoformat(self.obs_date)
        start = dt.datetime.combine(day, dt.time(0), zone)
        end = dt.datetime.combine(day + dt.timedelta(days=1), dt.time(0), zone)
        return start.astimezone(UTC), end.astimezone(UTC)

    def signature(self) -> str:
        """What must stay constant for a station's markets from one day to the next."""
        return f"{self.status}|{self.template_id}|{self.station}|{self.source_type}|{self.tz}"


def rules_hash(text: str | None) -> str:
    return hashlib.sha256((text or "").encode("utf-8")).hexdigest()


@lru_cache(maxsize=1)
def _templates() -> dict[str, re.Pattern[str]]:
    out: dict[str, re.Pattern[str]] = {}
    base = resources.files("polyweather.resolution").joinpath("templates")
    for template_id in TEMPLATE_RULES:
        text = base.joinpath(f"{template_id}.txt").read_text("utf-8")
        pattern = re.escape(text)
        for name in _PLACEHOLDERS:
            token = re.escape("{" + name + "}")
            first = f"(?P<{name}>{_PLACEHOLDER_PATTERNS[name]})"
            head, sep, tail = pattern.partition(token)
            if not sep:
                raise ValueError(f"template {template_id} lacks placeholder {name}")
            pattern = head + first + tail.replace(token, f"(?P={name})")
        out[template_id] = re.compile(pattern)
    return out


def match_template(rules_text: str) -> tuple[str, dict[str, str]] | None:
    text = (rules_text or "").replace("\r\n", "\n").strip()
    for template_id, pattern in _templates().items():
        m = pattern.fullmatch(text)
        if m:
            return template_id, m.groupdict()
    return None


def _parse_obs_date(s: str) -> dt.date | None:
    try:
        return dt.datetime.strptime(s, "%d %b '%y").date()
    except ValueError:
        return None


def parse_spec(
    *,
    rules_text: str | None,
    resolution_source: str | None,
    event_date: str | None,
    title: str | None = None,
    bracket_units: set[str] | None = None,
) -> ResolutionSpec:
    """Parse one event. ``bracket_units`` are the units found in its bracket titles."""
    reasons: list[str] = []
    src = (resolution_source or "").strip()
    m_url = AWC_URL.match(src)
    if not m_url:
        return ResolutionSpec(status="unparsed", reasons=("source_not_awc_metar",), source_url=src or None)

    matched = match_template(rules_text or "")
    if not matched:
        return ResolutionSpec(
            status="unparsed",
            reasons=("unknown_rules_template",),
            station=m_url.group("icao"),
            source_url=src,
        )
    template_id, groups = matched
    rules = dict(TEMPLATE_RULES[template_id])
    icao = groups["icao"]

    if groups["source_url"] != src:
        reasons.append("source_url_in_rules_differs_from_resolution_source")
    if icao != m_url.group("icao"):
        reasons.append("station_in_rules_differs_from_source_url")

    obs = _parse_obs_date(groups["obs_date"])
    if obs is None:
        reasons.append("obs_date_unreadable")
    elif event_date and obs.isoformat() != event_date[:10]:
        reasons.append("obs_date_differs_from_event_date")

    station = stations.get(icao)
    if station is None:
        reasons.append("station_not_in_registry")

    if title:
        expected = "highest" if rules["measure"] == "high" else "lowest"
        if not title.strip().lower().startswith(f"{expected} temperature"):
            reasons.append("title_measure_differs_from_rules")
    if bracket_units is not None and bracket_units != {rules["unit"]}:
        reasons.append("bracket_unit_differs_from_rules")

    end = parse_iso(m_url.group("end"))
    start = end - dt.timedelta(hours=int(m_url.group("hours")))
    spec = ResolutionSpec(
        status="ok",
        template_id=template_id,
        station=icao,
        station_name=groups["station_name"],
        measure=rules["measure"],
        unit=rules["unit"],
        obs_date=obs.isoformat() if obs else None,
        tz=station.tz if station else None,
        source_type=rules["source_type"],
        source_url=src,
        window_start_utc=iso(start),
        window_end_utc=iso(end),
        rules=rules,
    )
    if obs and station:
        day_start, day_end = spec.local_day_bounds_utc()
        if not (start <= day_start and end >= day_end):
            reasons.append("source_window_does_not_cover_local_day")

    if reasons:
        return ResolutionSpec(**{**asdict(spec), "status": "unparsed", "reasons": tuple(reasons)})
    return spec
