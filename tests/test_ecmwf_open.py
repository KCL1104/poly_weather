"""ECMWF open-data reader: everything here runs offline (httpx.MockTransport, synthetic GRIB)."""

import datetime as dt
import json
import re
import subprocess
import sys

import httpx
import pytest

from polyweather.stations import Station
from polyweather.weather import ecmwf_open as eo
from polyweather.weather.ensemble import ForecastSource

UTC = dt.timezone.utc
RUN = dt.datetime(2026, 11, 24, 0, tzinfo=UTC)
# The real grid: 1440 x 721, rows 90N -> 90S, each row from 180E eastwards.
ECMWF = eo.Grid(ni=1440, nj=721, lat0=90.0, lon0=180.0, dlat=-0.25, dlon=0.25)


@pytest.fixture(autouse=True)
def unpaced(monkeypatch):
    monkeypatch.setattr(eo, "REQUEST_INTERVAL", 0.0)


def station(icao: str, lat: float, lon: float) -> Station:
    return Station(icao, icao.lower(), "UTC", lat, lon, None, "", "", icao.startswith("K"))


def entry(param: str, type_: str, number: int | None, step, offset: int, length: int, **extra) -> dict:
    e = {"domain": "g", "date": "20261124", "time": "0000", "expver": "0001", "class": "od", "type": type_,
         "stream": "enfo", "levtype": "sfc", "step": str(step), "param": param, "_offset": offset, "_length": length}
    if number is not None:
        e["number"] = str(number)
    return e | extra


# --- bucket layout and listing ------------------------------------------------------


def test_run_prefix_follows_the_bucket_layout_changes():
    assert eo.run_prefix(RUN) == "20261124/00z/ifs/0p25/enfo/"
    assert eo.run_prefix(dt.datetime(2024, 2, 28, 6, tzinfo=UTC)) == "20240228/06z/ifs/0p25/enfo/"
    assert eo.run_prefix(dt.datetime(2024, 2, 28, 0, tzinfo=UTC)) == "20240228/00z/0p25/enfo/"
    # A run given in another zone is still filed under its UTC date and hour.
    est = dt.timezone(dt.timedelta(hours=-5))
    assert eo.run_prefix(dt.datetime(2026, 11, 23, 19, tzinfo=est)) == "20261124/00z/ifs/0p25/enfo/"
    assert eo.file_url(RUN, 6) == f"{eo.BUCKET}/20261124/00z/ifs/0p25/enfo/20261124000000-6h-enfo-ef.grib2"
    assert eo.file_url(RUN, 144, "oper", "index") == (
        f"{eo.BUCKET}/20261124/00z/ifs/0p25/oper/20261124000000-144h-oper-fc.index"
    )


def listing(keys: list[tuple[str, str]], token: str | None = None) -> bytes:
    contents = "".join(
        f"<Contents><Key>{k}</Key><LastModified>{m}</LastModified><Size>1</Size></Contents>" for k, m in keys
    )
    more = f"<NextContinuationToken>{token}</NextContinuationToken><IsTruncated>true</IsTruncated>" if token else (
        "<IsTruncated>false</IsTruncated>"
    )
    return (
        '<?xml version="1.0" encoding="UTF-8"?>'
        f'<ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Name>b</Name>{more}{contents}'
        "</ListBucketResult>"
    ).encode()


def run_keys(run: dt.datetime, steps: dict[int, str]) -> list[tuple[str, str]]:
    """Listing keys of a run: {step: LastModified} for both files of each step, plus bystanders."""
    p = eo.run_prefix(run)
    stamp = f"{run:%Y%m%d%H}0000"
    keys = [(f"{p}{stamp}-{s}h-enfo-ef.{ext}", m) for s, m in steps.items() for ext in ("grib2", "index")]
    bystanders = [f"{p}{stamp}-240h-enfo-ep.grib2", f"{p}{stamp}-360h-enfo-tf.bufr", f"{p}README.txt"]
    return keys + [(k, "2026-11-24T08:01:03.000Z") for k in bystanders]


def test_parse_listing_and_published_steps():
    p = "20261124/00z/ifs/0p25/enfo/20261124000000"
    keys, token = eo.parse_listing(
        listing(
            [
                (f"{p}-0h-enfo-ef.grib2", "2026-11-24T07:40:02.000Z"),
                (f"{p}-0h-enfo-ef.index", "2026-11-24T07:40:03.000Z"),
                (f"{p}-102h-enfo-ef.grib2", "2026-11-24T07:42:35.000Z"),
                (f"{p}-102h-enfo-ef.index", "2026-11-24T07:42:34.000Z"),
                (f"{p}-3h-enfo-ef.grib2", "2026-11-24T07:40:03.000Z"),  # index not uploaded yet
                (f"{p}-240h-enfo-ep.grib2", "2026-11-24T08:01:03.000Z"),  # probabilities, not members
                (f"{p}-240h-enfo-ep.index", "2026-11-24T08:01:03.000Z"),
                ("20261124/00z/ifs/0p25/enfo/README.txt", "2026-11-24T07:40:03.000Z"),
            ],
            token="abc/123==",
        )
    )
    assert token == "abc/123=="
    assert len(keys) == 8 and keys[0][1] == dt.datetime(2026, 11, 24, 7, 40, 2, tzinfo=UTC)
    assert eo.published_steps(keys) == {
        0: dt.datetime(2026, 11, 24, 7, 40, 3, tzinfo=UTC),
        102: dt.datetime(2026, 11, 24, 7, 42, 35, tzinfo=UTC),
    }
    assert eo.parse_listing(listing([])) == ([], None)


def bucket_client(runs: dict[dt.datetime, dict[int, str]], page_size: int = 1000) -> tuple[httpx.Client, list[str]]:
    """Client whose transport answers ListObjectsV2 for the given runs; records the prefixes asked."""
    by_prefix = {eo.run_prefix(run): run_keys(run, steps) for run, steps in runs.items()}
    asked: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        assert request.url.path == "/" and request.url.params["list-type"] == "2"
        prefix = request.url.params["prefix"]
        start = int(request.url.params.get("continuation-token", "0"))
        if not start:
            asked.append(prefix)
        keys = by_prefix.get(prefix, [])
        page = keys[start : start + page_size]
        token = str(start + page_size) if start + page_size < len(keys) else None
        return httpx.Response(200, content=listing(page, token))

    return httpx.Client(transport=httpx.MockTransport(handler)), asked


def test_available_steps_sorted_and_paginated():
    client, _ = bucket_client({RUN: {s: "2026-11-24T07:41:00.000Z" for s in (150, 0, 3, 144, 6)}}, page_size=3)
    assert eo.available_steps(client, RUN) == [0, 3, 6, 144, 150]
    assert eo.available_steps(client, RUN - dt.timedelta(hours=6)) == []


def test_latest_run_waits_for_the_upload_to_finish():
    prev = dt.datetime(2026, 11, 23, 18, tzinfo=UTC)
    client, asked = bucket_client(
        {
            RUN: {0: "2026-11-24T07:40:02.000Z", 3: "2026-11-24T07:40:03.000Z", 72: "2026-11-24T07:41:43.000Z"},
            prev: {0: "2026-11-24T01:04:01.000Z", 72: "2026-11-24T01:05:10.000Z"},
        }
    )
    at = lambda h, m, s=0: dt.datetime(2026, 11, 24, h, m, s, tzinfo=UTC)  # noqa: E731
    # 06Z is not there yet at 11:00, 00Z is complete.
    assert eo.latest_run(client, at(11, 0)) == RUN
    assert asked == ["20261124/06z/ifs/0p25/enfo/", "20261124/00z/ifs/0p25/enfo/"]
    # While 00Z is being uploaded (and until the newest file is 2 minutes old) the previous run is the latest.
    assert eo.latest_run(client, at(7, 41)) == prev
    assert eo.latest_run(client, at(7, 43, 42)) == prev
    assert eo.latest_run(client, at(7, 43, 43)) == RUN
    assert eo.latest_run(client, at(7, 41), settle=dt.timedelta(0)) == prev  # step 72 was written at 07:41:43
    # Before anything listed was published, and beyond the lookback, there is no run.
    assert eo.latest_run(client, at(1, 0)) is None
    assert eo.latest_run(client, dt.datetime(2026, 11, 27, 12, tzinfo=UTC)) is None
    assert eo.latest_run(client, dt.datetime(2026, 11, 27, 12, tzinfo=UTC), lookback=20) == RUN


def test_latest_run_does_not_look_before_the_first_0p25_run():
    client, asked = bucket_client({})
    assert eo.latest_run(client, dt.datetime(2024, 1, 31, 13, tzinfo=UTC)) is None
    assert asked == ["20240131/12z/0p25/enfo/", "20240131/06z/0p25/enfo/"]


# --- index selection ----------------------------------------------------------------


def test_parse_index_reads_json_lines():
    text = json.dumps(entry("2t", "pf", 7, 6, 10, 20)) + "\n\n" + json.dumps(entry("2t", "cf", None, 6, 30, 5)) + "\n"
    assert [e["type"] for e in eo.parse_index(text)] == ["pf", "cf"]


def test_select_fields_maps_members_and_drops_everything_else():
    entries = [
        entry("2t", "pf", 2, 6, 900, 100),
        entry("2t", "cf", None, 6, 500, 110),
        entry("2t", "pf", 1, 6, 100, 120),
        entry("2t", "pf", 1, 6, 5000, 120),  # duplicate member: first one wins
        entry("2d", "pf", 1, 6, 300, 100),  # other parameter
        entry("t", "pf", 1, 6, 400, 100, levtype="pl", levelist="850"),
        entry("2t", "pf", 1, 6, 600, 100, levtype="pl", levelist="1000"),  # not a surface field
        entry("2t", "em", None, 6, 700, 100),  # ensemble mean is not a member
        entry("mx2t3", "pf", 1, 6, 1200, 90),
        entry("mx2t3", "pf", 2, "3-6", 1100, 90),  # a step range ends at the step
        entry("mn2t3", "pf", 1, 3, 1300, 227),  # labelled with another step
    ]
    assert eo.select_fields(entries, 6, ("2t",)) == [
        eo.Field("2t", 1, 100, 120),
        eo.Field("2t", 0, 500, 110),
        eo.Field("2t", 2, 900, 100),
    ]
    both = eo.select_fields(entries, 6, ("2t", "mx2t3", "mn2t3"))
    assert [(f.param, f.member) for f in both] == [("2t", 1), ("2t", 0), ("2t", 2), ("mx2t3", 2), ("mx2t3", 1)]
    assert eo.select_fields(entries, 3, ("mn2t3",)) == [eo.Field("mn2t3", 1, 1300, 227)]
    assert eo.select_fields(entries, 9, ("2t",)) == []


def test_select_fields_drops_the_step_zero_placeholders():
    # The 0 h file lists all-zero mx2t3/mn2t3 messages under step 3.
    entries = [
        entry("2t", "pf", 1, 0, 0, 300),
        entry("mx2t3", "pf", 1, 3, 300, 227),
        entry("mn2t3", "pf", 1, 3, 527, 227),
    ]
    assert eo.select_fields(entries, 0, ("2t", "mx2t3", "mn2t3")) == [eo.Field("2t", 1, 0, 300)]


def test_select_fields_takes_the_oper_forecast_as_control():
    oper = [
        entry("2t", "fc", None, 6, 6470446, 312099, stream="oper"),
        entry("msl", "fc", None, 6, 0, 10, stream="oper"),
    ]
    assert eo.select_fields(oper, 6, ("2t",)) == [eo.Field("2t", 0, 6470446, 312099)]


# --- byte ranges --------------------------------------------------------------------


def test_merge_ranges_joins_touching_fields_only():
    a, b = eo.Field("2t", 1, 0, 100), eo.Field("2t", 2, 100, 50)
    c, d = eo.Field("2t", 3, 151, 49), eo.Field("2t", 4, 1000, 10)
    assert eo.merge_ranges([d, c, a, b]) == [(0, 150, [a, b]), (151, 200, [c]), (1000, 1010, [d])]
    assert eo.merge_ranges([a, b, c, d], max_gap=1) == [(0, 200, [a, b, c]), (1000, 1010, [d])]
    assert eo.merge_ranges([a, b, c, d], max_gap=800) == [(0, 1010, [a, b, c, d])]
    assert eo.merge_ranges([]) == []


def test_merge_ranges_handles_overlap_and_containment():
    outer, inner, tail = eo.Field("2t", 1, 0, 100), eo.Field("2t", 2, 10, 20), eo.Field("2t", 3, 90, 30)
    assert eo.merge_ranges([inner, tail, outer]) == [(0, 120, [outer, inner, tail])]


def file_client(files: dict[str, bytes]) -> tuple[httpx.Client, list[tuple[str, str | None]]]:
    """Client serving whole files (200), single byte ranges (206) or 404; records (path, Range)."""
    seen: list[tuple[str, str | None]] = []

    def handler(request: httpx.Request) -> httpx.Response:
        rng = request.headers.get("range")
        seen.append((request.url.path, rng))
        data = files.get(request.url.path)
        if data is None:
            return httpx.Response(404, text="<Error><Code>NoSuchKey</Code></Error>")
        if rng is None:
            return httpx.Response(200, content=data)
        m = re.fullmatch(r"bytes=(\d+)-(\d+)", rng)  # exactly one range, as S3 requires
        assert m, rng
        return httpx.Response(206, content=data[int(m.group(1)) : int(m.group(2)) + 1])

    return httpx.Client(transport=httpx.MockTransport(handler)), seen


def test_fetch_messages_issues_one_request_per_merged_range():
    msg = lambda n: b"GRIB" + bytes([n]) * 20 + b"7777"  # noqa: E731
    blob = msg(1) + msg(2) + b"x" * 50 + msg(3)
    fields = [eo.Field("2t", 3, 106, 28), eo.Field("2t", 1, 0, 28), eo.Field("2t", 2, 28, 28)]
    client, seen = file_client({"/f.grib2": blob})
    got = eo.fetch_messages(client, "https://bucket.test/f.grib2", fields)
    assert seen == [("/f.grib2", "bytes=0-55"), ("/f.grib2", "bytes=106-133")]
    assert [(f.member, m) for f, m in got] == [(1, msg(1)), (2, msg(2)), (3, msg(3))]
    # With a gap allowance the filler is downloaded too, in a single request.
    client, seen = file_client({"/f.grib2": blob})
    got = eo.fetch_messages(client, "https://bucket.test/f.grib2", fields, max_gap=64)
    assert seen == [("/f.grib2", "bytes=0-133")] and [m for _, m in got] == [msg(1), msg(2), msg(3)]


def test_fetch_messages_rejects_bytes_that_are_not_a_message():
    client, _ = file_client({"/f.grib2": b"GRIB" + b"\0" * 20 + b"7777" + b"junk" * 10})
    with pytest.raises(ValueError, match="not a GRIB message"):
        eo.fetch_messages(client, "https://bucket.test/f.grib2", [eo.Field("2t", 1, 4, 28)])
    with pytest.raises(ValueError, match="asked for 100 bytes"):  # range runs past the end of the file
        eo.fetch_messages(client, "https://bucket.test/f.grib2", [eo.Field("2t", 1, 0, 100)])


def test_requests_outlast_a_slowdown_burst(monkeypatch):
    naps: list[float] = []
    monkeypatch.setattr(eo.time, "sleep", naps.append)
    calls = {"n": 0}

    def handler(request: httpx.Request) -> httpx.Response:
        calls["n"] += 1
        if calls["n"] <= 8:  # more refusals than the shared helper would sit through
            return httpx.Response(503, text="<Error><Code>SlowDown</Code></Error>")
        return httpx.Response(206, content=b"GRIB" + b"\0" * 20 + b"7777")

    client = httpx.Client(transport=httpx.MockTransport(handler))
    got = eo.fetch_messages(client, "https://bucket.test/f.grib2", [eo.Field("2t", 1, 0, 28)])
    assert len(got) == 1 and calls["n"] == 9
    assert naps == [1.0, 2.0, 4.0, 8.0, 16.0, 30.0, 30.0, 30.0]

    # Patience is bounded, and other errors are not waited on.
    monkeypatch.setattr(eo, "THROTTLE_PATIENCE", 0.0)
    calls["n"] = 0
    with pytest.raises(httpx.HTTPStatusError, match="503"):
        eo.fetch_messages(client, "https://bucket.test/f.grib2", [eo.Field("2t", 1, 0, 28)])
    assert calls["n"] == 1
    missing, seen = file_client({})
    with pytest.raises(httpx.HTTPStatusError, match="404"):
        eo.fetch_messages(missing, "https://bucket.test/f.grib2", [eo.Field("2t", 1, 0, 28)])
    assert len(seen) == 1


def test_requests_are_spaced(monkeypatch):
    clock = {"t": 1000.0}
    naps: list[float] = []

    def nap(seconds: float) -> None:
        naps.append(round(seconds, 6))
        clock["t"] += seconds

    monkeypatch.setattr(eo.time, "monotonic", lambda: clock["t"])
    monkeypatch.setattr(eo.time, "sleep", nap)
    monkeypatch.setattr(eo, "REQUEST_INTERVAL", 0.5)
    monkeypatch.setattr(eo, "_last_request", 0.0)
    client, seen = file_client({"/a.index": b"{}"})
    for took in (0.2, 0.7, 0.0):  # how long each request "takes"
        eo._get(client, "https://bucket.test/a.index")
        clock["t"] += took
    eo._get(client, "https://bucket.test/a.index")
    assert naps == [0.3, 0.5] and len(seen) == 4  # nothing before the first, nor after a slow request


# --- nearest grid point -------------------------------------------------------------


@pytest.mark.parametrize(
    "lat, lon, index, point",
    [  # index and point as reported by ecCodes' own nearest-point search on a real message
        (40.7794, -73.8803, 284104, (40.75, -74.0)),  # KLGA
        (33.9382, -118.3866, 322806, (34.0, -118.5)),  # KLAX
        (-23.4356, -46.4731, 654294, (-23.5, -46.5)),  # SBGR, southern hemisphere
        (51.4775, -0.4614, 222478, (51.5, -0.5)),  # EGLL, just west of Greenwich
        (35.5523, 139.7797, 315199, (35.5, 139.75)),  # RJTT, eastern hemisphere
    ],
)
def test_grid_index_on_the_ecmwf_grid(lat, lon, index, point):
    assert eo.grid_index(ECMWF, lat, lon) == index
    assert eo.grid_index(ECMWF, lat, lon + 360.0) == index  # 0..360 convention
    assert eo.grid_index(ECMWF, lat, lon - 360.0) == index
    assert eo.grid_point(ECMWF, index) == point


def test_grid_index_wraps_at_the_date_line_and_clamps_at_the_poles():
    row = 360 * 1440  # the equator: (90 - 0) / 0.25 rows down
    assert eo.grid_index(ECMWF, 0.0, -180.0) == row
    assert eo.grid_index(ECMWF, 0.0, 180.0) == row
    assert eo.grid_index(ECMWF, 0.0, 179.9) == row  # nearer to 180 than to 179.75: wraps to column 0
    assert eo.grid_index(ECMWF, 0.0, 179.8) == row + 1439
    assert eo.grid_index(ECMWF, 0.0, -179.9) == row
    assert eo.grid_index(ECMWF, 0.0, 0.0) == row + 720
    assert eo.grid_index(ECMWF, 90.0, -180.0) == 0
    assert eo.grid_index(ECMWF, 95.0, -180.0) == 0
    assert eo.grid_index(ECMWF, -90.0, 179.75) == 721 * 1440 - 1
    assert eo.grid_index(ECMWF, -99.0, 179.75) == 721 * 1440 - 1
    assert eo.grid_point(ECMWF, 0) == (90.0, -180.0)
    assert eo.grid_point(ECMWF, 721 * 1440 - 1) == (-90.0, 179.75)


def test_grid_index_with_other_conventions():
    # Rows south to north, columns from Greenwich eastwards (0..359.75).
    south_up = eo.Grid(ni=1440, nj=721, lat0=-90.0, lon0=0.0, dlat=0.25, dlon=0.25)
    k = eo.grid_index(south_up, 40.7794, -73.8803)
    assert divmod(k, 1440) == (523, 1144)
    assert eo.grid_point(south_up, k) == (40.75, -74.0)
    assert eo.grid_index(south_up, 40.7794, 286.1197) == k
    # Columns running west from 180E.
    westward = eo.Grid(ni=360, nj=181, lat0=90.0, lon0=180.0, dlat=-1.0, dlon=-1.0)
    k = eo.grid_index(westward, -23.4356, -46.4731)
    assert divmod(k, 360) == (113, 226)
    assert eo.grid_point(westward, k) == (-23.0, -46.0)


# --- decoding and the whole path, on synthetic GRIB ---------------------------------


def test_module_imports_without_eccodes():
    code = "import sys; sys.modules['eccodes'] = None; import polyweather.weather.ecmwf_open as m; print(m.MODEL)"
    out = subprocess.run([sys.executable, "-c", code], capture_output=True, text=True, timeout=60)
    assert out.returncode == 0 and out.stdout.strip() == "ecmwf_ifs025", out.stderr


def test_unknown_variable_is_refused_before_any_request():
    client, seen = file_client({})
    with pytest.raises(ValueError, match="unsupported variables"):
        eo.fetch_points(client, RUN, [6], [station("KLGA", 40.7794, -73.8803)], variables=("2t", "10u"))
    assert seen == []


NI, NJ = 36, 19  # 10 degree grid laid out like ECMWF's


def grib(param: str, member: int | None, step: int, base_k: float, run: dt.datetime = RUN) -> bytes:
    """A small regular lat/lon GRIB2 field with value base_k + 0.01 * position; member None = deterministic."""
    eccodes = pytest.importorskip("eccodes")
    h = eccodes.codes_grib_new_from_samples("regular_ll_sfc_grib2")
    try:
        keys = {
            "Ni": NI, "Nj": NJ, "iDirectionIncrementInDegrees": 10.0, "jDirectionIncrementInDegrees": 10.0,
            "latitudeOfFirstGridPointInDegrees": 90.0, "latitudeOfLastGridPointInDegrees": -90.0,
            "longitudeOfFirstGridPointInDegrees": 180.0, "longitudeOfLastGridPointInDegrees": 170.0,
            "dataDate": int(f"{run:%Y%m%d}"), "dataTime": run.hour * 100,
        }
        if member is not None:
            keys |= {"productDefinitionTemplateNumber": 1, "perturbationNumber": member}
        for k, v in keys.items():
            eccodes.codes_set(h, k, v)
        eccodes.codes_set(h, "shortName", param)
        eccodes.codes_set(h, "step", step)
        eccodes.codes_set(h, "bitsPerValue", 16)
        eccodes.codes_set_values(h, [base_k + 0.01 * k for k in range(NI * NJ)])
        return eccodes.codes_get_message(h)
    finally:
        eccodes.codes_release(h)


def step_file(messages: list[tuple[str, str, int | None, int, bytes]], stream: str = "enfo") -> tuple[bytes, bytes]:
    """Concatenate (param, type, number, step, message) into a GRIB file and its index."""
    blob, lines = b"", []
    for param, type_, number, step, message in messages:
        lines.append(json.dumps(entry(param, type_, number, step, len(blob), len(message), stream=stream)))
        blob += message
    return blob, "\n".join(lines).encode()


def path(run: dt.datetime, step: int, stream: str, ext: str) -> str:
    return eo.file_url(run, step, stream, ext).removeprefix(eo.BUCKET)


SMALL = eo.Grid(ni=NI, nj=NJ, lat0=90.0, lon0=180.0, dlat=-10.0, dlon=10.0)
KLGA, SBGR = station("KLGA", 40.7794, -73.8803), station("SBGR", -23.4356, -46.4731)


def expected_c(base_k: float, s: Station) -> float:
    return round(base_k + 0.01 * eo.grid_index(SMALL, s.lat, s.lon) - 273.15, 2)


def test_decode_points_reads_identity_and_nearest_values():
    meta, values = eo.decode_points(grib("2t", 7, 6, 280.0), ((KLGA.lat, KLGA.lon), (SBGR.lat, SBGR.lon)))
    assert meta == {"param": "2t", "number": 7, "run": RUN, "step": 6, "units": "K"}
    # KLGA -> row (90-40)/10 = 5, column (-70+180)/10 = 11; SBGR -> row 11, column 13.
    assert [eo.grid_index(SMALL, s.lat, s.lon) for s in (KLGA, SBGR)] == [5 * NI + 11, 11 * NI + 13]
    assert values == pytest.approx([280.0 + 0.01 * (5 * NI + 11), 280.0 + 0.01 * (11 * NI + 13)], abs=2e-3)
    meta, _ = eo.decode_points(grib("2t", None, 6, 280.0), ((0.0, 0.0),))
    assert meta["number"] == 0  # a deterministic field has no member number


def test_fetch_points_with_control_in_the_ensemble_file():
    blob, index = step_file(
        [("2t", "pf", 2, 6, grib("2t", 2, 6, 282.0)), ("msl", "pf", 1, 6, b"x" * 40),
         ("2t", "cf", None, 6, grib("2t", 0, 6, 280.0)), ("2t", "pf", 1, 6, grib("2t", 1, 6, 281.0))]
    )
    client, seen = file_client({path(RUN, 6, "enfo", "grib2"): blob, path(RUN, 6, "enfo", "index"): index})
    samples = eo.fetch_points(client, RUN, [6, 9], [KLGA, SBGR])
    assert len(samples) == 6 and {s.member for s in samples} == {0, 1, 2}
    for s in samples:
        st = KLGA if s.station == "KLGA" else SBGR
        assert s == eo.EnsembleSample(st.icao, "ecmwf_ifs025", RUN, RUN + dt.timedelta(hours=6), s.member, "t2m",
                                      expected_c(280.0 + s.member, st))
    # Step 9 is not in the bucket: skipped. The oper stream is not touched when the file has its own control.
    assert [p for p, _ in seen if "oper" in p] == []
    assert (path(RUN, 9, "enfo", "index"), None) in seen
    # cf and member 1 are adjacent in the file and share a request.
    assert len([p for p, rng in seen if rng]) == 2


def test_fetch_points_takes_the_control_from_oper_when_enfo_has_none():
    run = dt.datetime(2026, 11, 23, 18, tzinfo=UTC)
    enfo, enfo_index = step_file([("2t", "pf", n, 3, grib("2t", n, 3, 280.0 + n, run)) for n in (1, 2, 3)])
    oper, oper_index = step_file(
        [("msl", "fc", None, 3, b"x" * 64), ("2t", "fc", None, 3, grib("2t", None, 3, 290.0, run))], stream="oper"
    )
    client, seen = file_client(
        {path(run, 3, "enfo", "grib2"): enfo, path(run, 3, "enfo", "index"): enfo_index,
         path(run, 3, "oper", "grib2"): oper, path(run, 3, "oper", "index"): oper_index}
    )
    samples = eo.fetch_points(client, run, [3], [KLGA])
    assert {s.member: s.value_c for s in samples} == {
        0: expected_c(290.0, KLGA), 1: expected_c(281.0, KLGA), 2: expected_c(282.0, KLGA), 3: expected_c(283.0, KLGA)
    }
    assert all(s.valid == dt.datetime(2026, 11, 23, 21, tzinfo=UTC) and s.run == run for s in samples)
    assert (path(run, 3, "oper", "grib2"), "bytes=64-%d" % (len(oper) - 1)) in seen


def test_fetch_points_refuses_a_message_that_contradicts_the_index():
    blob, index = step_file([("2t", "pf", 5, 6, grib("2t", 4, 6, 280.0))])  # index says member 5
    client, _ = file_client({path(RUN, 6, "enfo", "grib2"): blob, path(RUN, 6, "enfo", "index"): index})
    with pytest.raises(ValueError, match="index says"):
        eo.fetch_points(client, RUN, [6], [KLGA])


def test_source_fetches_the_latest_run_once(monkeypatch):
    files, steps = {}, (0, 3, 72, 75)
    for step in steps:
        blob, index = step_file(
            [("2t", "cf", None, step, grib("2t", 0, step, 280.0))] + [("2t", "pf", 1, step, grib("2t", 1, step, 281.0))]
        )
        files |= {path(RUN, step, "enfo", "grib2"): blob, path(RUN, step, "enfo", "index"): index}
    lists = run_keys(RUN, {s: "2026-11-24T07:41:00.000Z" for s in steps})
    file_http, seen = file_client(files)

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/":
            keys = lists if request.url.params["prefix"] == eo.run_prefix(RUN) else []
            return httpx.Response(200, content=listing(keys))
        return file_http.get(str(request.url), headers={k: v for k, v in request.headers.items() if k == "range"})

    client = httpx.Client(transport=httpx.MockTransport(handler))
    monkeypatch.setattr(eo, "utcnow", lambda: dt.datetime(2026, 11, 24, 11, 30, tzinfo=UTC))
    source: ForecastSource = eo.EcmwfOpenSource(client)
    assert source.name == "ecmwf_open"
    samples = source.fetch([KLGA, SBGR])
    assert source.last_run == RUN
    assert sorted({(s.valid - s.run) // dt.timedelta(hours=1) for s in samples}) == [0, 3, 72]  # max_step = 72
    assert len(samples) == 3 * 2 * 2
    # The same run is not downloaded twice; another station set is its own download.
    n = len(seen)
    assert source.fetch([KLGA, SBGR]) == [] and len(seen) == n
    assert len(source.fetch([KLGA])) == 3 * 2
    # Explicit steps are honoured where published.
    picky = eo.EcmwfOpenSource(client, steps=[3, 75, 78], refetch=True)
    assert sorted({s.valid for s in picky.fetch([KLGA])}) == [RUN + dt.timedelta(hours=h) for h in (3, 75)]
    assert len(picky.fetch([KLGA])) == 4
