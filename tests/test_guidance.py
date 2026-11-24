import datetime as dt

from polyweather.weather import lamp, nbm
from polyweather.weather.guidance import extract_blocks, have_cycle, store_blocks

UTC = dt.timezone.utc


def test_extract_blocks_picks_whole_station_blocks(fixtures):
    data = (fixtures / "nbp_sample_20261123_13.txt").read_bytes()
    blocks = extract_blocks(data, {"KLGA", "KORD", "KZZZ"})
    assert [b.station for b in blocks] == ["KLGA"]  # KORD and KZZZ are not in this sample
    b = blocks[0]
    assert b.cycle == dt.datetime(2026, 11, 23, 13, tzinfo=UTC)
    assert b.text.splitlines()[0].startswith(" KLGA    NBM  NBP GUIDANCE")
    assert "KLGB" not in b.text and "KLFT" not in b.text
    assert b.text.splitlines()[-1].startswith(" SLPP9")


def test_parse_nbp_13z_starts_with_next_morning_min(fixtures):
    b = extract_blocks((fixtures / "nbp_sample_20261123_13.txt").read_bytes(), {"KLGA"})[0]
    rows = nbm.parse_nbp(b)
    assert len(rows) == 18
    first = rows[0]
    assert (first.valid, first.fhr, first.kind) == (dt.datetime(2026, 11, 24, 12, tzinfo=UTC), 23, "min")
    assert (first.mean, first.sd, first.p10, first.p25, first.p50, first.p75, first.p90) == (47, 3, 43, 45, 47, 49, 50)
    # The first maximum in the 13Z issue is for the following local day.
    first_max = next(r for r in rows if r.kind == "max")
    assert (first_max.valid, first_max.p10, first_max.p50, first_max.p90) == (dt.datetime(2026, 11, 25, 0, tzinfo=UTC), 48, 51, 53)
    assert rows[-1].fhr == 227 and rows[-1].p90 == 63


def test_parse_nbp_01z_and_07z_still_hold_same_day_max(fixtures):
    for cycle_hour, fhr, p50 in ((1, 23, 68), (7, 17, 68)):
        b = extract_blocks((fixtures / f"nbp_klga_20261123_{cycle_hour:02d}.txt").read_bytes(), {"KLGA"})[0]
        rows = nbm.parse_nbp(b)
        assert b.cycle.hour == cycle_hour
        assert (rows[0].valid, rows[0].fhr, rows[0].kind, rows[0].p50) == (dt.datetime(2026, 11, 24, 0, tzinfo=UTC), fhr, "max", p50)


def test_nbp_cycle_schedule_and_urls():
    start = dt.datetime(2026, 11, 23, 0, tzinfo=UTC)
    cycles = nbm.expected_cycles("nbp", start, start + dt.timedelta(hours=23))
    assert [c.hour for c in cycles] == [1, 7, 13, 19]
    assert len(nbm.expected_cycles("nbh", start, start + dt.timedelta(hours=23))) == 24
    assert nbm.cycle_url(nbm.S3, cycles[0], "nbp").endswith("/blend.20261123/01/text/blend_nbptx.t01z")


def test_store_blocks_and_have_cycle(conn, fixtures):
    blocks = extract_blocks((fixtures / "nbp_sample_20261123_13.txt").read_bytes(), {"KLGA"})
    assert store_blocks(conn, "nbp", blocks, "s3", "Mon, 23 Nov 2026 14:09:17 GMT") == 1
    assert store_blocks(conn, "nbp", blocks, "s3", None) == 0  # idempotent
    assert nbm.store_txn(conn, nbm.parse_nbp(blocks[0])) == 18
    cycle = blocks[0].cycle
    assert have_cycle(conn, "nbp", cycle, {"KLGA"})
    assert not have_cycle(conn, "nbp", cycle, {"KLGA", "KORD"})


def test_parse_lamp_short_and_extended(fixtures):
    b = extract_blocks((fixtures / "lav_sample_20261124_0930.txt").read_bytes(), {"KLGA"})[0]
    assert b.cycle == dt.datetime(2026, 11, 24, 9, 30, tzinfo=UTC)
    rows = lamp.parse_lav(b)
    assert len(rows) == 25
    assert (rows[0].valid, rows[0].fhr, rows[0].tmp_f, rows[0].dpt_f) == (dt.datetime(2026, 11, 24, 10, tzinfo=UTC), 1, 47, 31)
    assert (rows[-1].valid, rows[-1].fhr, rows[-1].tmp_f) == (dt.datetime(2026, 11, 25, 10, tzinfo=UTC), 25, 37)
    assert max(r.tmp_f for r in rows) == 49

    bx = extract_blocks((fixtures / "lavx_sample_20261124_0930.txt").read_bytes(), {"KLGA"})[0]
    ext = lamp.parse_lav(bx)
    assert [ext[0].fhr, ext[-1].fhr] == [26, 38]
    assert (ext[0].valid, ext[0].tmp_f) == (dt.datetime(2026, 11, 25, 11, tzinfo=UTC), 37)
    assert lamp.cycle_url(b.cycle, "lav").endswith("/lmp.20261124/lmp.t0930z.lavtxt.ascii")
