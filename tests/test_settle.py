import datetime as dt
import re

from polyweather.catalog.brackets import parse_bracket
from polyweather.resolution.settle import Obs, settle
from polyweather.resolution.spec import parse_spec

UTC = dt.timezone.utc


def _spec(ev):
    return parse_spec(rules_text=ev["description"], resolution_source=ev["resolutionSource"],
                      event_date=ev["eventDate"], title=ev["title"], bracket_units={"F"})


def _obs_from_awc(text: str, year: int, month: int) -> list[Obs]:
    out = []
    for i, line in enumerate(text.splitlines()):
        m = re.search(r"(\d{2})(\d{2})(\d{2})Z", line)
        if m:
            d, h, mi = map(int, m.groups())
            out.append(Obs(dt.datetime(year, month, d, h, mi, tzinfo=UTC), line.strip(), seen_order=i))
    return out


def test_real_market_rebuilds_to_the_bracket_that_resolved_yes(nyc_event, fixtures):
    spec = _spec(nyc_event)
    obs = _obs_from_awc((fixtures / "awc_klga_2026-11-09.txt").read_text(), 2026, 11)
    s = settle(spec, obs)
    assert (s.n_counted, s.value_tenths_c, s.value_unit) == (24, 144, 58)
    markets = sorted(nyc_event["markets"], key=lambda m: float(m["groupItemThreshold"]))
    brackets = [parse_bracket(m["groupItemTitle"]) for m in markets]
    rebuilt = next(i for i, b in enumerate(brackets) if b.contains(s.value_unit))
    actual = next(i for i, m in enumerate(markets) if m["outcomePrices"] == '["1", "0"]')
    assert rebuilt == actual and brackets[rebuilt].label == "58-59F"
    assert s.deciding_time == dt.datetime(2026, 11, 9, 16, 51, tzinfo=UTC)


def _mk(spec_day_hour_utc: int, minute: int, text: str, order: int = 0, label=None) -> Obs:
    return Obs(dt.datetime(2026, 11, 9, spec_day_hour_utc, minute, tzinfo=UTC), text, label, order)


def test_speci_reports_do_not_count(nyc_event):
    spec = _spec(nyc_event)
    obs = [
        _mk(15, 51, "METAR KLGA 091551Z 10SM 14/01 A2988 RMK AO2 T01440006"),
        _mk(16, 10, "SPECI KLGA 091610Z 10SM 20/01 A2988 RMK AO2 T02000006"),
        # Labelled by the source rather than in the text (as IEM history is).
        _mk(16, 20, "KLGA 091620Z 10SM 21/01 A2988 RMK AO2 T02100006", label="SPECI"),
        # Off the nominal hour and unlabelled: still routine.
        _mk(16, 30, "KLGA 091630Z 10SM 15/01 A2988 RMK AO2 T01500006"),
    ]
    s = settle(spec, obs)
    assert (s.n_reports, s.n_counted, s.value_tenths_c, s.value_unit) == (4, 2, 150, 59)


def test_only_the_station_local_day_counts(nyc_event):
    spec = _spec(nyc_event)  # New York, EST: the day is 05:00Z Nov 9 to 05:00Z Nov 10
    before = Obs(dt.datetime(2026, 11, 9, 4, 51, tzinfo=UTC), "METAR KLGA 090451Z 30/01 RMK T03000006")
    first = Obs(dt.datetime(2026, 11, 9, 5, 0, tzinfo=UTC), "METAR KLGA 090500Z 10/01 RMK T01000006")
    after = Obs(dt.datetime(2026, 11, 10, 5, 0, tzinfo=UTC), "METAR KLGA 100500Z 31/01 RMK T03100006")
    s = settle(spec, [before, first, after])
    assert (s.n_reports, s.value_tenths_c) == (1, 100)


def test_latest_version_of_an_observation_time_wins(nyc_event):
    spec = _spec(nyc_event)
    original = _mk(15, 51, "METAR KLGA 091551Z 10SM 25/01 A2988 RMK AO2 T02500006", order=1)
    corrected = _mk(15, 51, "METAR KLGA 091551Z COR 10SM 15/01 A2988 RMK AO2 T01500006", order=2)
    assert settle(spec, [original, corrected]).value_tenths_c == 150
    assert settle(spec, [corrected, original]).value_tenths_c == 150


def test_running_extreme_and_empty_day(nyc_event, fixtures):
    spec = _spec(nyc_event)
    obs = _obs_from_awc((fixtures / "awc_klga_2026-11-09.txt").read_text(), 2026, 11)
    # By 10:51 local (15:51Z) the high so far was 13.9 °C, i.e. 57 °F.
    so_far = settle(spec, obs, until=dt.datetime(2026, 11, 9, 15, 51, tzinfo=UTC))
    assert (so_far.value_tenths_c, so_far.value_unit) == (139, 57)
    empty = settle(spec, [])
    assert empty.value_unit is None and empty.n_counted == 0
