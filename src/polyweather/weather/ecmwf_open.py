"""ECMWF open data: IFS ensemble (stream ``enfo``) point forecasts, read directly.

ECMWF publishes its real-time forecasts under CC-BY-4.0 in the public bucket
``ecmwf-forecasts`` (eu-central-1). A run lives under
``{YYYYMMDD}/{HH}z/ifs/0p25/enfo/`` (before the 2024-02-28 06Z run:
``{YYYYMMDD}/{HH}z/0p25/enfo/``; the first run on the 0.25 degree grid is 2024-01-31
06Z) with one ``{YYYYMMDDHH}0000-{step}h-enfo-ef.grib2`` per forecast step holding
every field of every member, 3-4.6 GB. Next to it sits a ``.index`` file, one JSON
object per GRIB message with its MARS keys and ``_offset``/``_length``. We read the
index (1-2 MB), pick the messages we want and fetch only those with HTTP Range
requests: a global 2 m temperature field is 0.3-0.7 MB, so one step of all members
costs 16-34 MB instead of the whole file. Messages of one parameter are scattered
through the file, so this is about one request per member; ranges that happen to
touch are merged.

The bucket throttles. At 1-2 requests a second 10-20 % of them are answered
``503 SlowDown`` in bursts of a few seconds; an unpaced run (4-5 requests a second)
was cut off after about 370 requests and refused for nearly five minutes. Requests
are therefore spaced ``REQUEST_INTERVAL`` apart and a refused one is retried until
the burst is over. One step of all members then takes 30-40 s (measured), which
puts the 25 steps out to 72 h at roughly a quarter of an hour (not yet timed in
full).

Members: 1..50 are the perturbed forecasts (``type=pf``, GRIB ``number``); member 0
is the control. Through the 2026-05-12 00Z run the control is in the same file as
``type=cf``. From 2026-05-12 06Z on the enfo files hold only the 50 perturbed
members and member 0 is taken from stream ``oper`` (``...-{step}h-oper-fc.grib2``):
on 2026-05-12 00Z the two 2t fields were bit-identical, and ``oper`` has exactly the
step list of ``enfo`` for all four runs. The messages themselves do not say so.

Steps are 0..144 h every 3 h for all four runs, then 150..360 h every 6 h for 00Z and
12Z. In November 2026 files appear 7 h 40 min (00Z/12Z) or 7 h 04 min (06Z/18Z)
after run time, all steps within 4-7 minutes. ``2t`` is instantaneous;
``mx2t3``/``mn2t3`` (in the files since 2024-11-13) are extremes over the 3 hours
ending at the step and do not exist at step 0 (the 0 h file carries all-zero
placeholders labelled step 3, which the step filter drops); beyond 144 h they become
``mx2t6``/``mn2t6``.

Grid: regular 0.25 degree lat/lon, 1440 x 721, rows from 90N to 90S, each row
starting at 180E and running east, i.e. longitudes -180..179.75. The geometry is
read from every message rather than assumed. A station gets the nearest grid point
(no interpolation, no land-sea mask), up to ~20 km away, so a coastal airport can
land on a sea point. Since 2026-10-21 fields are packed with 9 bits, which quantises
2t to 0.25 K (1/32 K before).

Decoding needs ecCodes (``pip install polyweather[ecmwf]``); it is imported only
inside the functions that decode, so this module imports without it.
"""

from __future__ import annotations

import datetime as dt
import json
import logging
import re
import time
import xml.etree.ElementTree as ET
from dataclasses import dataclass
from functools import lru_cache
from typing import Iterable, Sequence

import httpx

from ..stations import Station
from ..util.http import request_with_retry
from ..util.timeutil import UTC, iso, parse_iso, utcnow
from .ensemble import EnsembleSample

log = logging.getLogger(__name__)

BUCKET = "https://ecmwf-forecasts.s3.eu-central-1.amazonaws.com"
MODEL = "ecmwf_ifs025"
FIRST_RUN = dt.datetime(2024, 1, 31, 6, tzinfo=UTC)  # first run with 0.25 degree enfo files
IFS_PREFIX_FROM = dt.datetime(2024, 2, 28, 6, tzinfo=UTC)  # '{HH}z/ifs/0p25/' from this run on
N_MEMBERS = 51
# GRIB shortName -> EnsembleSample.variable
VARIABLES = {"2t": "t2m", "mx2t3": "mx2t3", "mn2t3": "mn2t3", "mx2t6": "mx2t6", "mn2t6": "mn2t6"}
SETTLE = dt.timedelta(minutes=2)  # a run counts as published once its newest file is this old
REQUEST_INTERVAL = 0.5  # seconds between requests to the bucket (module-wide, not thread-safe)
THROTTLE_PATIENCE = 900.0  # seconds to keep retrying one request while the bucket answers 503 SlowDown
_FILE_TYPE = {"enfo": "ef", "oper": "fc"}
_MEMBER_TYPES = {"cf", "pf", "fc"}
_S3_NS = "{http://s3.amazonaws.com/doc/2006-03-01/}"
_STEP_KEY = re.compile(r"-(\d+)h-enfo-ef\.(grib2|index)$")
_KELVIN = 273.15
_PLAUSIBLE_K = (150.0, 350.0)


@dataclass(frozen=True)
class Field:
    """One GRIB message of the step file, as located by the index."""

    param: str
    member: int  # 0 = control
    offset: int
    length: int


@dataclass(frozen=True)
class Grid:
    """Regular lat/lon grid stored row by row.

    Value ``j * ni + i`` is at latitude ``lat0 + j * dlat`` and longitude
    ``lon0 + i * dlon`` (increments are signed; ECMWF: lat0=90, dlat=-0.25, lon0=180,
    dlon=0.25).
    """

    ni: int
    nj: int
    lat0: float
    lon0: float
    dlat: float
    dlon: float


# --- requests --------------------------------------------------------------------

_last_request = 0.0


def _pace() -> None:
    """Sleep until ``REQUEST_INTERVAL`` has passed since the previous request."""
    global _last_request
    wait = _last_request + REQUEST_INTERVAL - time.monotonic()
    if wait > 0:
        time.sleep(wait)
    _last_request = time.monotonic()


def _get(client: httpx.Client, url: str, *, ok: tuple[int, ...] = (200,), **kwargs) -> httpx.Response:
    """Paced GET via the shared retry helper that waits out the bucket's throttling.

    ``503 SlowDown`` is routine here and can last minutes, longer than the helper's
    few attempts, so it is retried after 1, 2, 4 ... 30 s for up to
    ``THROTTLE_PATIENCE``. Everything else is left to the helper.
    """
    deadline = time.monotonic() + THROTTLE_PATIENCE
    delay = 1.0
    while True:
        _pace()
        r = request_with_retry(client, "GET", url, ok=(*ok, 503), **kwargs)
        if r.status_code != 503:
            return r
        if time.monotonic() + delay > deadline:
            r.raise_for_status()
        log.log(logging.INFO if delay >= 16 else logging.DEBUG, "ecmwf: bucket throttling, next try in %.0fs", delay)
        time.sleep(delay)
        delay = min(delay * 2, 30.0)


# --- bucket layout ---------------------------------------------------------------


def run_prefix(run: dt.datetime, stream: str = "enfo") -> str:
    run = run.astimezone(UTC)
    grid = "ifs/0p25" if run >= IFS_PREFIX_FROM else "0p25"
    return f"{run:%Y%m%d}/{run:%H}z/{grid}/{stream}/"


def file_url(run: dt.datetime, step: int, stream: str = "enfo", ext: str = "grib2") -> str:
    run = run.astimezone(UTC)
    return f"{BUCKET}/{run_prefix(run, stream)}{run:%Y%m%d%H}0000-{step}h-{stream}-{_FILE_TYPE[stream]}.{ext}"


def parse_listing(xml: bytes) -> tuple[list[tuple[str, dt.datetime]], str | None]:
    """(key, LastModified) pairs of one ListObjectsV2 page and the continuation token."""
    root = ET.fromstring(xml)
    keys = [
        (c.findtext(f"{_S3_NS}Key", ""), parse_iso(c.findtext(f"{_S3_NS}LastModified", "")))
        for c in root.iter(f"{_S3_NS}Contents")
    ]
    truncated = root.findtext(f"{_S3_NS}IsTruncated") == "true"
    return keys, root.findtext(f"{_S3_NS}NextContinuationToken") if truncated else None


def published_steps(keys: Iterable[tuple[str, dt.datetime]]) -> dict[int, dt.datetime]:
    """Steps that have both the GRIB and the index file -> when the later one was written."""
    seen: dict[int, dict[str, dt.datetime]] = {}
    for key, modified in keys:
        m = _STEP_KEY.search(key)
        if m:
            seen.setdefault(int(m.group(1)), {})[m.group(2)] = modified
    return {step: max(files.values()) for step, files in sorted(seen.items()) if len(files) == 2}


def list_run(client: httpx.Client, run: dt.datetime) -> dict[int, dt.datetime]:
    """Published enfo steps of a run -> publication time (empty when the run is absent)."""
    keys: list[tuple[str, dt.datetime]] = []
    token: str | None = None
    while True:
        params = {"list-type": "2", "prefix": run_prefix(run)}
        if token:
            params["continuation-token"] = token
        page, token = parse_listing(_get(client, f"{BUCKET}/", params=params).content)
        keys.extend(page)
        if not token:
            return published_steps(keys)


def available_steps(client: httpx.Client, run: dt.datetime) -> list[int]:
    """Forecast steps (hours) present in the bucket for that run."""
    return list(list_run(client, run))


def latest_run(
    client: httpx.Client, now: dt.datetime | None = None, *, settle: dt.timedelta = SETTLE, lookback: int = 8
) -> dt.datetime | None:
    """Most recent 00/06/12/18Z run whose enfo files were all in the bucket at ``now``.

    A run's ~50-85 step files are uploaded over 4-7 minutes, so it only counts once
    the newest of them is at least ``settle`` old; a run still being uploaded is
    passed over for the one before. The bucket keeps the original upload times, so a
    past ``now`` answers "which run was available then". Looks back ``lookback`` runs.
    """
    now = (now or utcnow()).astimezone(UTC)
    run = now.replace(hour=now.hour - now.hour % 6, minute=0, second=0, microsecond=0)
    for _ in range(lookback):
        if run < FIRST_RUN:
            break
        published = list_run(client, run)
        if 0 in published and max(published.values()) <= now - settle:
            return run
        run -= dt.timedelta(hours=6)
    return None


# --- index -----------------------------------------------------------------------


def parse_index(text: str) -> list[dict]:
    """The ``.index`` sidecar: one JSON object per line."""
    return [json.loads(line) for line in text.splitlines() if line.strip()]


def select_fields(entries: Iterable[dict], step: int, params: Sequence[str]) -> list[Field]:
    """Surface fields of ``params`` valid at ``step``, one per member, in file order.

    ``type`` cf (ensemble control) and fc (stream oper) are member 0, pf is member
    ``number``; derived products (em, es, ep) are ignored. Entries labelled with
    another step are dropped: the 0 h file holds placeholders for the 3-hour
    extremes under step 3.
    """
    picked: dict[tuple[str, int], Field] = {}
    for e in entries:
        if e.get("param") not in params or e.get("levtype") != "sfc" or e.get("type") not in _MEMBER_TYPES:
            continue
        if str(e.get("step", "")).split("-")[-1] != str(step):
            continue
        member = int(e["number"]) if e["type"] == "pf" else 0
        picked.setdefault((e["param"], member), Field(e["param"], member, int(e["_offset"]), int(e["_length"])))
    return sorted(picked.values(), key=lambda f: f.offset)


def merge_ranges(fields: Iterable[Field], max_gap: int = 0) -> list[tuple[int, int, list[Field]]]:
    """Group fields into byte ranges ``(start, end_exclusive, fields)``.

    Fields whose bytes touch, overlap or lie within ``max_gap`` bytes of each other
    share one range, so they cost one request (the gap is downloaded and discarded).
    """
    ranges: list[tuple[int, int, list[Field]]] = []
    for f in sorted(fields, key=lambda f: f.offset):
        if ranges and f.offset - ranges[-1][1] <= max_gap:
            start, end, group = ranges[-1]
            ranges[-1] = (start, max(end, f.offset + f.length), group + [f])
        else:
            ranges.append((f.offset, f.offset + f.length, [f]))
    return ranges


def fetch_index(client: httpx.Client, run: dt.datetime, step: int, stream: str = "enfo") -> list[dict] | None:
    """Index entries of one step file, or None when it is not in the bucket."""
    r = _get(client, file_url(run, step, stream, "index"), ok=(200, 404))
    return parse_index(r.text) if r.status_code == 200 else None


def fetch_messages(
    client: httpx.Client, url: str, fields: Sequence[Field], max_gap: int = 0
) -> list[tuple[Field, bytes]]:
    """Download the GRIB messages of ``fields`` with one Range request per merged range."""
    out: list[tuple[Field, bytes]] = []
    for start, end, group in merge_ranges(fields, max_gap):
        # S3 serves one range per request; several in one header would return the whole object.
        body = _get(client, url, ok=(206,), headers={"Range": f"bytes={start}-{end - 1}"}).content
        if len(body) != end - start:
            raise ValueError(f"{url}: asked for {end - start} bytes at {start}, got {len(body)}")
        for f in group:
            message = body[f.offset - start : f.offset - start + f.length]
            if message[:4] != b"GRIB" or message[-4:] != b"7777":
                raise ValueError(f"{url}: bytes {f.offset}+{f.length} are not a GRIB message")
            out.append((f, message))
    return out


# --- grid and decoding -----------------------------------------------------------


def grid_index(grid: Grid, lat: float, lon: float) -> int:
    """Position in the value array of the grid point nearest to (lat, lon).

    Longitude may be given as -180..180 or 0..360: it is taken modulo 360 relative
    to the grid's first column, and the column index wraps around the date line.
    Latitude is clamped to the grid, so it works for either row order.
    """
    i = round(((lon - grid.lon0) / grid.dlon) % (360.0 / abs(grid.dlon))) % grid.ni
    j = min(max(round((lat - grid.lat0) / grid.dlat), 0), grid.nj - 1)
    return j * grid.ni + i


def grid_point(grid: Grid, index: int) -> tuple[float, float]:
    """(lat, lon) of a value position, longitude in -180..180."""
    j, i = divmod(index, grid.ni)
    lon = (grid.lon0 + i * grid.dlon + 180.0) % 360.0 - 180.0
    return grid.lat0 + j * grid.dlat, lon


@lru_cache(maxsize=16)
def _indices(grid: Grid, points: tuple[tuple[float, float], ...]) -> tuple[int, ...]:
    return tuple(grid_index(grid, lat, lon) for lat, lon in points)


def _read_grid(handle) -> Grid:
    import eccodes

    get = lambda key: eccodes.codes_get(handle, key)  # noqa: E731
    if get("gridType") != "regular_ll" or get("jPointsAreConsecutive") or get("alternativeRowScanning"):
        raise ValueError(f"unsupported grid: {get('gridType')} scanningMode={get('scanningMode')}")
    grid = Grid(
        ni=get("Ni"),
        nj=get("Nj"),
        lat0=get("latitudeOfFirstGridPointInDegrees"),
        lon0=get("longitudeOfFirstGridPointInDegrees"),
        dlat=get("jDirectionIncrementInDegrees") * (1 if get("jScansPositively") else -1),
        dlon=get("iDirectionIncrementInDegrees") * (-1 if get("iScansNegatively") else 1),
    )
    if abs(grid.ni * abs(grid.dlon) - 360.0) > 1e-6:
        raise ValueError(f"grid does not span the globe in longitude: {grid}")
    return grid


def decode_points(message: bytes, points: tuple[tuple[float, float], ...]) -> tuple[dict, list[float | None]]:
    """Decode one GRIB message; returns its identity and the value (kelvin) nearest each point.

    A missing value is returned as None.
    """
    import eccodes

    handle = eccodes.codes_new_from_message(message)
    try:
        meta = {
            "param": eccodes.codes_get(handle, "shortName"),
            "number": eccodes.codes_get(handle, "number") if eccodes.codes_is_defined(handle, "number") else 0,
            "run": dt.datetime.strptime(
                f"{eccodes.codes_get(handle, 'dataDate')}{eccodes.codes_get(handle, 'dataTime'):04d}", "%Y%m%d%H%M"
            ).replace(tzinfo=UTC),
            "step": eccodes.codes_get(handle, "endStep"),
            "units": eccodes.codes_get(handle, "units"),
        }
        grid = _read_grid(handle)
        missing = eccodes.codes_get(handle, "missingValue")
        values = eccodes.codes_get_values(handle)
    finally:
        eccodes.codes_release(handle)
    if len(values) != grid.ni * grid.nj:
        raise ValueError(f"{meta['param']}: {len(values)} values for a {grid.ni}x{grid.nj} grid")
    picked = [float(values[k]) for k in _indices(grid, points)]
    return meta, [None if v == missing else v for v in picked]


# --- fetching --------------------------------------------------------------------


def _step_fields(
    client: httpx.Client, run: dt.datetime, step: int, params: Sequence[str]
) -> dict[str, list[Field]] | None:
    """Fields to download for one step, by stream; None when the step file is absent."""
    entries = fetch_index(client, run, step)
    if entries is None:
        return None
    by_stream = {"enfo": select_fields(entries, step, params)}
    have = {f.param for f in by_stream["enfo"]}
    lacking_control = sorted(have - {f.param for f in by_stream["enfo"] if f.member == 0})
    if lacking_control:
        oper = fetch_index(client, run, step, "oper")
        by_stream["oper"] = select_fields(oper or [], step, lacking_control)
        if len(by_stream["oper"]) != len(lacking_control):
            log.warning("ecmwf %s +%dh: no control forecast for %s in enfo or oper", iso(run), step, lacking_control)
    return by_stream


def fetch_step(
    client: httpx.Client, run: dt.datetime, step: int, stations: Sequence[Station], variables: Sequence[str] = ("2t",)
) -> list[EnsembleSample] | None:
    """All members of one step at the stations; None when the step is not published."""
    t0 = time.monotonic()
    run = run.astimezone(UTC)
    by_stream = _step_fields(client, run, step, variables)
    if by_stream is None:
        return None
    points = tuple((s.lat, s.lon) for s in stations)
    valid = run + dt.timedelta(hours=step)
    out: list[EnsembleSample] = []
    n_bytes = n_fields = 0
    for stream, fields in by_stream.items():
        for field, message in fetch_messages(client, file_url(run, step, stream), fields):
            meta, kelvin = decode_points(message, points)
            want = (field.param, field.member, run, step, "K")
            got = (meta["param"], meta["number"], meta["run"], meta["step"], meta["units"])
            if got != want:
                raise ValueError(f"ecmwf {stream} {iso(run)} +{step}h: index says {want}, message says {got}")
            n_bytes += len(message)
            n_fields += 1
            for station, k in zip(stations, kelvin):
                if k is None or not _PLAUSIBLE_K[0] < k < _PLAUSIBLE_K[1]:
                    log.warning("ecmwf %s +%dh %s member %d at %s: implausible value %s K, skipped",
                                iso(run), step, field.param, field.member, station.icao, k)
                    continue
                out.append(EnsembleSample(station.icao, MODEL, run, valid, field.member, VARIABLES[field.param],
                                          round(k - _KELVIN, 2)))
    for param in variables:
        n = len({f.member for fields in by_stream.values() for f in fields if f.param == param})
        if n not in (0, N_MEMBERS):
            log.warning("ecmwf %s +%dh %s: %d members instead of %d", iso(run), step, param, n, N_MEMBERS)
    log.info("ecmwf %s +%dh: %d fields, %.1f MB, %.1fs", iso(run), step, n_fields, n_bytes / 1e6, time.monotonic() - t0)
    return out


def fetch_points(
    client: httpx.Client,
    run: dt.datetime,
    steps: Iterable[int],
    stations: Sequence[Station],
    variables: Sequence[str] = ("2t",),
) -> list[EnsembleSample]:
    """One sample per station x member x step x variable for a run.

    ``variables`` are GRIB short names (keys of ``VARIABLES``). Member 0 is the
    control, 1..50 the perturbed members; ``valid`` is run + step (for the 3-hour
    extremes: the end of the interval); values are degrees Celsius at the nearest
    grid point. A step that is not in the bucket, or a variable that does not exist
    at a step, yields no samples; any other failure raises.
    """
    unknown = sorted(set(variables) - set(VARIABLES))
    if unknown:
        raise ValueError(f"unsupported variables {unknown}; known: {sorted(VARIABLES)}")
    run = run.astimezone(UTC)
    out: list[EnsembleSample] = []
    for step in steps:
        samples = fetch_step(client, run, step, stations, variables)
        if samples is None:
            log.warning("ecmwf %s +%dh: step not in bucket", iso(run), step)
            continue
        out.extend(samples)
    return out


class EcmwfOpenSource:
    """``ForecastSource`` over the latest published run.

    A run out to 72 h is ~460 MB of paced requests (roughly a quarter of an hour),
    so a published run is downloaded once: a further ``fetch`` for the same run and
    stations returns [] until the next run appears (``refetch=True`` turns that off).
    """

    name = "ecmwf_open"

    def __init__(
        self,
        client: httpx.Client,
        steps: Sequence[int] | None = None,
        max_step: int = 72,
        *,
        variables: Sequence[str] = ("2t",),
        refetch: bool = False,
    ) -> None:
        self.client = client
        self.steps = steps
        self.max_step = max_step
        self.variables = tuple(variables)
        self.refetch = refetch
        self.last_run: dt.datetime | None = None
        self._fetched: dict[frozenset[str], dt.datetime] = {}  # station set -> run already returned

    def fetch(self, stations: Sequence[Station]) -> list[EnsembleSample]:
        run = latest_run(self.client)
        if run is None:
            log.warning("ecmwf: no published enfo run found")
            return []
        key = frozenset(s.icao for s in stations)
        if self._fetched.get(key) == run and not self.refetch:
            return []
        published = available_steps(self.client, run)
        wanted = self.steps if self.steps is not None else [s for s in published if s <= self.max_step]
        steps = [s for s in wanted if s in published]
        samples = fetch_points(self.client, run, steps, stations, self.variables)
        self.last_run = run
        if steps:
            self._fetched[key] = run
        return samples
