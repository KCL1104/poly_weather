import datetime as dt
import gzip
import json

import pyarrow.parquet as pq
import pytest

from polyweather import stations
from polyweather.ops import gapcheck
from polyweather.settings import Settings
from polyweather.storage import db
from polyweather.weather import awc, ensemble, openmeteo
from polyweather.weather.ensemble import EnsembleSample
from polyweather.weather.metar_store import MetarRow, store

UTC = dt.timezone.utc

CACHE_CSV = (
    "raw_text,station_id,observation_time,latitude,longitude,temp_c,sky_cover,cloud_base_ft_agl,sky_cover,metar_type,elevation_m\n"
    '"METAR KLGA 240951Z 30017G22KT 10SM FEW100 08/M01 A3018 RMK AO2 SLP221 T00781006 $",KLGA,2026-11-24T09:51:00.000Z,40.7794,-73.8803,7.8,FEW,10000,,METAR,9\n'
    '"SPECI CYCA 241047Z AUTO 28004KT 3SM -SN OVC021 00/00 A2933 RMK ICE",CYCA,2026-11-24T10:47:00.000Z,53.68,-57.04,0,OVC,2100,,SPECI,12\n'
    '"SPECI KORD 241012Z 34006KT 10SM FEW250 M03/M08 A3050 RMK AO2 T10331083",KORD,2026-11-24T10:12:00.000Z,41.96,-87.93,-3.3,FEW,25000,,SPECI,202\n'
)


def test_station_registry_is_consistent():
    reg = stations.registry()
    assert len(reg) == 48 and reg["KLGA"].tz == "America/New_York" and reg["KLGA"].iem_id == "LGA"
    assert reg["EGLC"].iem_id == "EGLC" and not reg["EGLC"].us
    us = stations.in_scope("us")
    assert sorted(s.icao for s in us) == ["KATL", "KAUS", "KDAL", "KDEN", "KHOU", "KLAX", "KLGA", "KMIA", "KORD", "KSEA", "KSFO"]
    assert len(stations.in_scope("all")) == 48
    for s in reg.values():
        assert s.zone is not None and -90 <= s.lat <= 90 and -180 <= s.lon <= 180


def test_awc_cache_parse_keeps_only_wanted_stations():
    rows = awc.parse_cache(gzip.compress(CACHE_CSV.encode()), {"KLGA", "KORD"})
    assert [(r.station, r.metar_type) for r in rows] == [("KLGA", "METAR"), ("KORD", "SPECI")]
    assert rows[0].obs_time == dt.datetime(2026, 11, 24, 9, 51, tzinfo=UTC)
    assert rows[0].raw_text.startswith("METAR KLGA 240951Z")


def test_metar_store_parses_rule_fields_and_fills_receipt_time(conn):
    t = dt.datetime(2026, 11, 24, 9, 51, tzinfo=UTC)
    raw = "METAR KLGA 240951Z 30017G22KT 10SM FEW100 08/M01 A3018 RMK AO2 SLP221 T00781006 $"
    assert store(conn, [MetarRow("KLGA", t, raw, "METAR")], "awc_cache", seen_ms=111) == 1
    # The API pass later sees the same text and adds when AWC received it.
    assert store(conn, [MetarRow("KLGA", t, raw, "METAR", "2026-11-24T09:54:06.293Z")], "awc_api", seen_ms=999) == 0
    row = conn.execute("SELECT * FROM metar_obs").fetchone()
    assert (row["temp_tenths_c"], row["temp_source"], row["source"], row["first_seen_ms"]) == (78, "tgroup", "awc_cache", 111)
    assert row["receipt_time"] == "2026-11-24T09:54:06.293Z"


def _openmeteo_payload(n_members=3, hours=("2026-11-24T00:00", "2026-11-24T01:00")):
    hourly = {"time": list(hours), "temperature_2m": [10.0, 11.0]}
    for m in range(1, n_members):
        hourly[f"temperature_2m_member{m:02d}"] = [10.0 + m, None]
    return {"hourly_units": {"temperature_2m": "°C"}, "hourly": hourly}


def test_openmeteo_response_to_samples():
    sts = [stations.get("KLGA"), stations.get("KORD")]
    samples = openmeteo.parse_response([_openmeteo_payload(), _openmeteo_payload()], sts, "ecmwf_ifs025")
    klga = [s for s in samples if s.station == "KLGA"]
    assert len(klga) == 4  # control has two hours, two members have one (the None is skipped)
    assert {s.member for s in klga} == {0, 1, 2} and all(s.variable == "t2m" and s.run is None for s in klga)
    assert sorted(s.value_c for s in klga if s.valid.hour == 0) == [10.0, 11.0, 12.0]
    f = _openmeteo_payload()
    f["hourly_units"]["temperature_2m"] = "°F"
    f["hourly"]["temperature_2m"] = [50.0, 32.0]
    one = openmeteo.parse_response(f, [sts[0]], "ecmwf_ifs025")
    assert sorted(round(s.value_c, 3) for s in one if s.member == 0) == [0.0, 10.0]
    with pytest.raises(ValueError):
        openmeteo.parse_response([_openmeteo_payload()], sts, "ecmwf_ifs025")


def test_ensemble_store_is_content_addressed(conn, tmp_path):
    run = dt.datetime(2026, 11, 24, 0, tzinfo=UTC)
    rows = [EnsembleSample("KLGA", "ecmwf_ifs025", run, run + dt.timedelta(hours=h), m, "t2m", 5.0 + m + h)
            for h in (3, 6) for m in range(3)]
    written = ensemble.store_samples(conn, tmp_path, "ecmwf_open", rows, fetched_ms=1_795_500_000_000)
    assert list(written) == ["ecmwf_ifs025"]
    table = pq.read_table(tmp_path / written["ecmwf_ifs025"])
    assert table.num_rows == 6 and set(table.column("member").to_pylist()) == {0, 1, 2}
    assert ensemble.store_samples(conn, tmp_path, "ecmwf_open", list(reversed(rows))) == {}  # same content
    changed = rows[:-1] + [EnsembleSample("KLGA", "ecmwf_ifs025", run, rows[-1].valid, 2, "t2m", 99.0)]
    assert len(ensemble.store_samples(conn, tmp_path, "ecmwf_open", changed)) == 1
    meta = conn.execute("SELECT run_init, n_stations FROM forecast_fetches ORDER BY id").fetchall()
    assert [tuple(m) for m in meta] == [("2026-11-24T00:00:00Z", 1)] * 2


def test_gapcheck_reports_each_archive(conn, tmp_path):
    settings = Settings(data_dir=tmp_path, scope="us")
    settings.ensure_dirs()
    day = dt.date(2026, 11, 23)
    checks = {c.name: c for c in gapcheck.run(conn, settings, day)}
    assert set(checks) == {"book", "metar", "nbm", "lamp", "ensemble", "catalog"}
    assert not any(c.ok for c in checks.values())  # an empty archive passes nothing
    assert len(checks["nbm"].detail["missing_cycles"]["nbp"]) == 4
    assert len(checks["lamp"].detail["missing_cycles"]) == 24
    assert gapcheck.clean_streak(conn, day) == 0

    # A book gap of 90 s is reported with its length; a 20 s one is only counted.
    start = int(dt.datetime(2026, 11, 23, 12, tzinfo=UTC).timestamp() * 1000)
    conn.execute("INSERT INTO gaps(source, key, start_ms, end_ms, reason) VALUES('book','conn:1',?,?,'disconnect')", (start, start + 90_000))
    conn.execute("INSERT INTO gaps(source, key, start_ms, end_ms, reason) VALUES('book','conn:2',?,?,'disconnect')", (start, start + 20_000))
    book = gapcheck.check_book(conn, settings, day)
    assert book.detail["gaps"] == 2 and book.detail["gap_seconds"] == 110
    assert [g["seconds"] for g in book.detail["gaps_over_60s"]] == [90]

    # Hours missing at the source are listed per station.
    rows = [MetarRow("KLGA", dt.datetime(2026, 11, 23, h, 51, tzinfo=UTC), f"METAR KLGA 23{h:02d}51Z 10SM 08/M01 RMK AO2 T00781006", "METAR")
            for h in range(24) if h != 7]
    store(conn, rows, "awc_api")
    metar = gapcheck.check_metar(conn, settings, day)
    assert metar.detail["hours_missing_at_source"]["KLGA"] == [7]
    assert "KLGA" not in metar.detail["stations_mostly_missing"] and "KORD" in metar.detail["stations_mostly_missing"]

    # Seven days with every check passing is the gate.
    for i in range(7):
        d = (day - dt.timedelta(days=i)).isoformat()
        for fn in gapcheck.CHECKS:
            name = {"check_jobs": "catalog"}.get(fn.__name__, fn.__name__[6:])
            conn.execute("INSERT OR REPLACE INTO daily_checks(date, name, ok, detail, ts_ms) VALUES(?,?,1,'{}',0)", (d, name))
    assert gapcheck.clean_streak(conn, day) == 7


def test_cli_smoke(tmp_path, monkeypatch, capsys):
    from polyweather import cli

    monkeypatch.setenv("POLYWEATHER_DATA_DIR", str(tmp_path))
    assert cli.main(["status"]) == 0
    out = json.loads(capsys.readouterr().out)
    assert out["scope"] == "us" and out["events_open"] == 0 and out["frozen_stations"] == []
    assert cli.main(["station", "status"]) == 0
    assert cli.main(["station", "thaw", "KLGA"]) == 2  # a note is required
    assert cli.main(["check", "streak", "--date", "2026-11-23"]) == 0
    assert cli.main(["nbm", "backfill"]) == 2  # dates are required
    assert db.connect(tmp_path / "db" / "polyweather.sqlite").execute("SELECT COUNT(*) FROM events").fetchone()[0] == 0


@pytest.mark.parametrize("hours_silent, expect_catch_up", [(0.5, False), (6.0, True)])
def test_metar_poller_fetches_again_what_a_long_outage_left_out(conn, monkeypatch, hours_silent, expect_catch_up):
    from polyweather.util.timeutil import now_ms

    seen = now_ms() - int(hours_silent * 3_600_000)
    store(conn, [MetarRow("KLGA", dt.datetime(2026, 11, 24, 9, 51, tzinfo=UTC),
                          "METAR KLGA 240951Z 30017G22KT 10SM FEW100 08/M01 A3018 RMK AO2 T00781006", "METAR")],
          "awc_api", seen_ms=seen)
    calls = []
    monkeypatch.setattr(awc, "fetch_cache", lambda client, wanted: [])
    monkeypatch.setattr(awc, "fetch_api", lambda client, ids, **kw: [])
    monkeypatch.setattr(awc, "backfill", lambda conn_, client, ids, *, days: calls.append((list(ids), days)) or 0)
    awc.poll_forever(conn, None, ["KLGA", "KORD"], stop_after_s=0)
    if expect_catch_up:
        (ids, days), = calls
        assert ids == ["KLGA", "KORD"] and 7.9 / 24 <= days <= 8.2 / 24  # the outage plus two hours
    else:
        assert calls == []
