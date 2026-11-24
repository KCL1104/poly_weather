import copy

from polyweather.catalog import scanner
from polyweather.ops import settlement_check
from polyweather.weather.metar_store import MetarRow, load_obs, store

import datetime as dt
import re

UTC = dt.timezone.utc


def _next_day(event: dict) -> dict:
    """The same market series one day later, as Gamma would list it."""
    e = copy.deepcopy(event)
    old_url = e["resolutionSource"]
    new_url = old_url.replace("2026-11-10T08", "2026-11-11T08")
    swap = lambda s: s.replace(old_url, new_url).replace("9 Nov '26", "10 Nov '26")  # noqa: E731
    e.update(id="2000001", slug="highest-temperature-in-nyc-on-november-10-2026", eventDate="2026-11-10",
             resolutionSource=new_url, description=swap(e["description"]), closed=False)
    for i, m in enumerate(e["markets"]):
        m.update(id=str(9_000_000 + i), description=e["description"], resolutionSource=new_url, closed=False)
    return e


def test_ingest_stores_event_markets_and_spec(conn, nyc_event):
    r = scanner.ingest(conn, [nyc_event])
    assert (r.events_seen, r.events_new, r.markets_seen, r.unparsed) == (1, 1, 11, 0)
    spec = conn.execute("SELECT status, station, measure, unit, obs_date FROM resolution_specs").fetchone()
    assert tuple(spec) == ("ok", "KLGA", "high", "F", "2026-11-09")
    rows = conn.execute("SELECT group_title, lo, hi, unit, winner, fee_rate, tick_size, bracket_ok FROM markets ORDER BY group_index").fetchall()
    assert tuple(rows[0]) == ("49°F or below", None, 49, "F", "NO", 0.01, 0.001, 1)
    assert tuple(rows[5])[:5] == ("58-59°F", 58, 59, "F", "YES")
    assert tuple(rows[-1])[:4] == ("68°F or higher", 68, None, "F")
    assert all(r["winner"] in ("YES", "NO") for r in rows) and sum(r["winner"] == "YES" for r in rows) == 1
    # Re-scanning unchanged data changes nothing and freezes nothing.
    r2 = scanner.ingest(conn, [nyc_event])
    assert (r2.events_new, r2.changes, r2.frozen) == (0, [], [])
    assert conn.execute("SELECT COUNT(*) FROM change_log").fetchone()[0] == 0


def test_edited_rules_freeze_the_station(conn, nyc_event):
    scanner.ingest(conn, [nyc_event])
    edited = copy.deepcopy(nyc_event)
    edited["description"] = edited["description"].replace("does not count", "counts")
    for m in edited["markets"]:
        m["description"] = edited["description"]
    r = scanner.ingest(conn, [edited])
    assert r.frozen == ["KLGA"] and r.unparsed == 1
    assert [c["field"] for c in r.changes] == ["rules_hash"]
    assert scanner.is_frozen(conn, "KLGA")
    log = conn.execute("SELECT scope, action, trigger FROM pause_log").fetchall()
    assert [tuple(x) for x in log] == [("station:KLGA", "pause", "rules_changed")]
    scanner.thaw_station(conn, "KLGA", "re-read the rules; meaning unchanged")
    assert not scanner.is_frozen(conn, "KLGA")


def test_changed_resolution_source_freezes_the_station(conn, nyc_event):
    scanner.ingest(conn, [nyc_event])
    moved = copy.deepcopy(nyc_event)
    moved["resolutionSource"] = "https://www.wunderground.com/history/daily/us/ny/new-york-city/KLGA"
    r = scanner.ingest(conn, [moved])
    assert "resolution_source" in [c["field"] for c in r.changes]
    assert conn.execute("SELECT status FROM resolution_specs").fetchone()[0] == "unparsed"
    # The new source names no AWC station, yet the freeze still lands on the right one.
    assert r.frozen == ["KLGA"] and scanner.is_frozen(conn, "KLGA")


def test_new_event_with_different_rules_freezes_via_series(conn, nyc_event):
    scanner.ingest(conn, [nyc_event])
    same = _next_day(nyc_event)
    assert scanner.ingest(conn, [same]).frozen == []  # normal next day: nothing happens

    switched = _next_day(nyc_event)
    switched.update(id="2000002", slug="highest-temperature-in-nyc-on-november-11-2026", eventDate="2026-11-11",
                    resolutionSource="https://www.wunderground.com/history/daily/us/ny/new-york-city/KLGA")
    r = scanner.ingest(conn, [switched])
    assert r.frozen == ["KLGA"] and r.unparsed == 1
    assert r.changes[0]["field"] == "spec_signature"


def test_history_import_never_freezes(conn, nyc_event):
    scanner.ingest(conn, [nyc_event], detect_changes=False)
    edited = copy.deepcopy(nyc_event)
    edited["description"] += "\nextra"
    r = scanner.ingest(conn, [edited], detect_changes=False)
    assert r.frozen == [] and r.changes == []


def test_market_with_its_own_rules_makes_the_event_unparsed(conn, nyc_event):
    e = copy.deepcopy(nyc_event)
    e["markets"][3]["description"] += " (special)"
    assert scanner.ingest(conn, [e]).unparsed == 1
    reasons = conn.execute("SELECT reasons FROM resolution_specs").fetchone()[0]
    assert "market_rules_differ_from_event" in reasons


def _load_metars(conn, fixtures, source="awc_api"):
    rows = []
    for line in (fixtures / "awc_klga_2026-11-09.txt").read_text().splitlines():
        m = re.search(r"(\d{2})(\d{2})(\d{2})Z", line)
        d, h, mi = map(int, m.groups())
        rows.append(MetarRow("KLGA", dt.datetime(2026, 11, d, h, mi, tzinfo=UTC), line.strip(), "METAR"))
    return store(conn, rows, source)


def test_settlement_check_matches_and_freezes_on_mismatch(conn, nyc_event, fixtures):
    scanner.ingest(conn, [nyc_event])
    assert _load_metars(conn, fixtures) == 30
    assert _load_metars(conn, fixtures) == 0  # idempotent
    s = settlement_check.run(conn, since="2026-11-09", until="2026-11-09", freeze_on_mismatch=True)
    assert (s.checked, s.matched, s.mismatched, s.rate) == (1, 1, 0, 1.0)
    row = conn.execute("SELECT reconstructed_bracket, actual_bracket, value_unit, n_counted FROM settlement_checks").fetchone()
    assert tuple(row) == ("58-59F", "58-59F", 58, 24)
    assert not scanner.is_frozen(conn, "KLGA")

    # A later, hotter corrected report for 16:51Z would move the rebuilt value to another bracket.
    store(conn, [MetarRow("KLGA", dt.datetime(2026, 11, 9, 16, 51, tzinfo=UTC),
                          "METAR KLGA 091651Z COR 30014KT 10SM 17/01 A2985 RMK AO2 T01720006", "METAR")], "awc_api")
    s = settlement_check.run(conn, since="2026-11-09", until="2026-11-09", freeze_on_mismatch=True)
    assert (s.matched, s.mismatched) == (0, 1)
    assert scanner.is_frozen(conn, "KLGA")
    assert conn.execute("SELECT trigger FROM pause_log").fetchone()[0] == "settlement_mismatch"


def test_awc_text_outranks_iem_text_for_the_same_observation(conn):
    t = dt.datetime(2026, 11, 9, 16, 51, tzinfo=UTC)
    store(conn, [MetarRow("KLGA", t, "KLGA 091651Z 10SM 15/01 RMK AO2 T01500006", "METAR")], "iem", seen_ms=2000)
    store(conn, [MetarRow("KLGA", t, "METAR KLGA 091651Z 10SM 14/01 RMK AO2 T01440006", "METAR")], "awc_api", seen_ms=1000)
    obs = load_obs(conn, "KLGA", t - dt.timedelta(hours=1), t + dt.timedelta(hours=1))
    best = max(obs, key=lambda o: o.seen_order)
    assert best.raw_text.startswith("METAR KLGA") and len(obs) == 2
    assert len(load_obs(conn, "KLGA", t, t + dt.timedelta(minutes=1), sources=("iem",))) == 1
