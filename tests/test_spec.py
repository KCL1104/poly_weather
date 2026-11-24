import json

from polyweather.resolution.spec import ResolutionSpec, match_template, parse_spec


def _parse(ev, **over):
    kw = dict(rules_text=ev["description"], resolution_source=ev["resolutionSource"],
              event_date=ev["eventDate"], title=ev["title"], bracket_units={"F"})
    kw.update(over)
    return parse_spec(**kw)


def test_real_us_high_market_parses(nyc_event):
    s = _parse(nyc_event)
    assert s.ok and s.reasons == ()
    assert (s.template_id, s.station, s.measure, s.unit) == ("awc_metar_high_f_v1", "KLGA", "high", "F")
    assert (s.obs_date, s.tz) == ("2026-11-09", "America/New_York")
    assert (s.window_start_utc, s.window_end_utc) == ("2026-11-09T02:00:00Z", "2026-11-10T08:00:00Z")
    start, end = s.local_day_bounds_utc()
    assert (start.isoformat(), end.isoformat()) == ("2026-11-09T05:00:00+00:00", "2026-11-10T05:00:00+00:00")
    assert s.rules["excluded_labels"] == ["SPECI"] and s.rules["rounding"] == "half_up"
    assert ResolutionSpec.from_json(s.to_json()) == s


def test_real_celsius_low_market_parses(fixtures):
    ev = json.loads((fixtures / "rules_london_low_2026-11-23.json").read_text("utf-8"))
    s = _parse(ev, bracket_units={"C"})
    assert s.ok and (s.template_id, s.station, s.measure, s.unit) == ("awc_metar_low_c_v1", "EGLC", "low", "C")


def test_any_rewording_is_unparsed(nyc_event):
    text = nyc_event["description"].replace("does not count", "counts")
    assert match_template(text) is None
    s = _parse(nyc_event, rules_text=text)
    assert not s.ok and s.reasons == ("unknown_rules_template",)
    # Dropping the rounding sentence must not slip through either.
    assert not _parse(nyc_event, rules_text=nyc_event["description"].replace("rounded up", "rounded down")).ok


def test_other_sources_are_unparsed(nyc_event):
    s = _parse(nyc_event, resolution_source="https://www.wunderground.com/history/daily/us/ny/new-york-city/KLGA")
    assert not s.ok and s.reasons == ("source_not_awc_metar",)


def test_cross_checks(nyc_event):
    src = nyc_event["resolutionSource"]
    # Source field disagrees with the URL quoted in the rules.
    other = src.replace("ids=KLGA", "ids=KJFK")
    assert "source_url_in_rules_differs_from_resolution_source" in _parse(nyc_event, resolution_source=other).reasons
    assert "obs_date_differs_from_event_date" in _parse(nyc_event, event_date="2026-11-10").reasons
    assert "bracket_unit_differs_from_rules" in _parse(nyc_event, bracket_units={"C"}).reasons
    assert "title_measure_differs_from_rules" in _parse(nyc_event, title="Lowest temperature in NYC on November 9?").reasons


def test_window_must_cover_the_local_day(nyc_event):
    short = nyc_event["resolutionSource"].replace("hours=30", "hours=20")
    ev = dict(nyc_event, resolutionSource=short, description=nyc_event["description"].replace(nyc_event["resolutionSource"], short))
    assert "source_window_does_not_cover_local_day" in _parse(ev).reasons


def test_station_missing_from_registry(nyc_event):
    swap = lambda s: s.replace("KLGA", "KJFK")  # noqa: E731
    ev = dict(nyc_event, resolutionSource=swap(nyc_event["resolutionSource"]), description=swap(nyc_event["description"]))
    assert "station_not_in_registry" in _parse(ev).reasons
