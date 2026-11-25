"""Paper engine: books at a moment, fills, the shared ledger rules, H2a, reconciliation.

The market is the real New York event of 2026-11-09 with its real METAR reports: the
day's high so far is 58°F once the 16:51Z report is in, which rules out the five
brackets up to 56-57°F. The order books are made up.
"""

import datetime as dt
import json
import re

import pytest

from polyweather.catalog import scanner
from polyweather.paper import engine, fills, h2a, ledger, report
from polyweather.paper.books import BookHistory, asks_of
from polyweather.recorder.recorder import RAW_SCHEMA, SNAP_SCHEMA
from polyweather.settings import Settings
from polyweather.storage.parquet_sink import RollingParquetSink
from polyweather.weather.metar_store import MetarRow, store

UTC = dt.timezone.utc
T0 = int(dt.datetime(2026, 11, 9, 16, 55, tzinfo=UTC).timestamp() * 1000)  # a snapshot is written here
SEEN = T0 + 40_000  # the 16:51Z report is first seen here


def ms(**delta) -> int:
    return int(dt.timedelta(**delta).total_seconds() * 1000)


@pytest.fixture
def world(tmp_path, conn, nyc_event, fixtures):
    """An open New York market, its reports up to 16:51Z, and a recorded book for every NO token."""
    event = json.loads(json.dumps(nyc_event))
    event["closed"] = False
    event.pop("closedTime", None)
    for m in event["markets"]:
        m["closed"] = False
        m["outcomePrices"] = '["0.5", "0.5"]'
        m.pop("closedTime", None)
    scanner.ingest(conn, [event])
    conn.execute("UPDATE events SET closed=0, closed_time=NULL")
    conn.execute("UPDATE markets SET closed=0, closed_time=NULL, winner=NULL")
    settings = Settings(data_dir=tmp_path, scope="us")
    settings.ensure_dirs()

    rows = []
    for line in (fixtures / "awc_klga_2026-11-09.txt").read_text().splitlines():
        d, h, mi = map(int, re.search(r"(\d{2})(\d{2})(\d{2})Z", line).groups())
        when = dt.datetime(2026, 11, d, h, mi, tzinfo=UTC)
        if dt.datetime(2026, 11, 9, 5, tzinfo=UTC) <= when <= dt.datetime(2026, 11, 9, 16, 51, tzinfo=UTC):
            rows.append((when, line.strip()))
    rows.sort()  # the fixture file lists the newest report first
    earlier = [MetarRow("KLGA", w, t, "METAR") for w, t in rows[:-1]]
    store(conn, earlier, "awc_api", seen_ms=T0 - ms(hours=1))
    store(conn, [MetarRow("KLGA", rows[-1][0], rows[-1][1], "METAR")], "awc_cache", seen_ms=SEEN)

    markets = conn.execute("SELECT * FROM markets ORDER BY group_index").fetchall()
    return {"conn": conn, "settings": settings, "markets": markets, "event_id": markets[0]["event_id"]}


def write_books(settings, books: dict, at_ms: int = T0, frames: list | None = None) -> None:
    """One snapshot at ``at_ms`` with ``books`` = {token: (bids, asks)}, and optional (recv_ms, message) frames."""
    snaps = RollingParquetSink(settings.raw_dir / "snapshots", "snap", SNAP_SCHEMA, "snap_ms", flush_seconds=0)
    for token, (bids, asks) in books.items():
        snaps.append((at_ms, token, "0xm", at_ms - 1000, at_ms - 900, "h", 1, json.dumps(bids), json.dumps(asks)))
    snaps.flush()
    raw = RollingParquetSink(settings.raw_dir / "book", "book", RAW_SCHEMA, "recv_ms")
    raw.append((at_ms + 1, 1, 1, "PONG-like filler"))
    for i, (recv, msg) in enumerate(frames or [], start=2):
        raw.append((recv, 1, i, json.dumps(msg)))
    raw.close()


def change(token, side, price, size, ts):
    return {"event_type": "price_change", "market": "0xm", "timestamp": str(ts),
            "price_changes": [{"asset_id": token, "side": side, "price": str(price), "size": str(size), "hash": f"h{ts}"}]}


def no_books(markets, asks_by_label: dict) -> dict:
    """NO-token books: brackets named in ``asks_by_label`` get those asks, the rest sell NO at 0.999."""
    out = {}
    for m in markets:
        asks = asks_by_label.get(m["group_title"], [[0.999, 5000.0]])
        out[m["no_token"]] = ([[0.90, 100.0]], asks)
    return out


# ---- fills -------------------------------------------------------------------------------------


def test_fee_and_limit_price_follow_the_exchange_formula():
    assert fills.fee_per_share(0.5, 0.01) == pytest.approx(0.0025)
    assert fills.cost_per_share(0.97, 0.01) == pytest.approx(0.97 + 0.01 * 0.97 * 0.03)
    limit = fills.limit_price(1.0, 0.02, 0.01, 0.001)
    assert limit == 0.979                                             # one tick higher would leave less than 2pp
    assert 1.0 - fills.cost_per_share(limit, 0.01) >= 0.02 > 1.0 - fills.cost_per_share(limit + 0.001, 0.01)
    assert fills.limit_price(0.04, 0.05, 0.01) is None                # nothing is cheap enough


def test_take_walks_the_levels_takes_a_quarter_and_stops_at_the_limit():
    asks = [(0.90, 8.0), (0.95, 100.0), (0.99, 1000.0)]
    f = fills.take(asks, limit=0.96, budget_usd=10.0, rate=0.01)
    assert f.levels[0] == (0.90, 2.0)                                 # a quarter of the 8 shares shown
    assert [p for p, _ in f.levels] == [0.90, 0.95]                   # 0.99 is above the limit
    assert f.spent <= 10.0 and f.spent == pytest.approx(10.0, abs=0.02)
    assert f.fee == pytest.approx(sum(s * 0.01 * p * (1 - p) for p, s in f.levels))
    assert f.fillable_usd == pytest.approx(0.90 * 2 + 0.95 * 25)


# ---- books ---------------------------------------------------------------------------------------


def test_book_at_a_moment_is_the_snapshot_plus_the_frames_since(world):
    token = world["markets"][0]["no_token"]
    write_books(world["settings"], {token: ([[0.9, 10.0]], [[0.95, 100.0], [0.97, 50.0]])},
                frames=[(T0 + 5_000, change(token, "SELL", 0.95, 0, T0 + 4_900)), (T0 + 30_000, change(token, "SELL", 0.96, 7, T0 + 29_900))])
    books = BookHistory(world["settings"], world["conn"])
    assert asks_of(books.at(T0 + 1_000, [token])[token]) == [(0.95, 100.0), (0.97, 50.0)]
    assert asks_of(books.at(T0 + 6_000, [token])[token]) == [(0.97, 50.0)]
    assert asks_of(books.at(T0 + 31_000, [token])[token]) == [(0.96, 7.0), (0.97, 50.0)]
    assert books.at(T0 - 1, [token])[token] is None                    # before the recording
    assert books.at(T0 + ms(minutes=10), [token])[token] is None       # long after the last snapshot: recorder not running
    assert books.at(T0 + 1_000, ["unknown"])["unknown"] is None


def test_no_book_is_given_out_across_a_hole_in_the_recording(world):
    token, other = world["markets"][0]["no_token"], world["markets"][1]["no_token"]
    write_books(world["settings"], {token: ([], [[0.95, 100.0]]), other: ([], [[0.95, 100.0]])})
    conn = world["conn"]
    conn.execute("INSERT INTO gaps(source, key, start_ms, end_ms, reason) VALUES('book', ?, ?, ?, 'hash_mismatch')",
                 (f"asset:{token}", T0 + 10_000, T0 + 20_000))
    books = BookHistory(world["settings"], conn)
    assert books.at(T0 + 5_000, [token])[token] is not None
    got = books.at(T0 + 30_000, [token, other])
    assert got[token] is None and got[other] is not None               # only the resynchronised book is affected
    conn.execute("INSERT INTO gaps(source, key, start_ms, end_ms, reason) VALUES('book', 'conn:2', ?, NULL, 'disconnect')", (T0 + 25_000,))
    assert books.at(T0 + 30_000, [other])[other] is None               # an open connection gap affects every book


# ---- H2a ------------------------------------------------------------------------------------------


def test_h2a_buys_no_of_the_cheapest_ruled_out_bracket_and_logs_every_candidate(world):
    conn, settings, markets = world["conn"], world["settings"], world["markets"]
    by_label = {m["group_title"]: m for m in markets}
    stale = [[0.95, 100.0], [0.97, 400.0], [0.99, 1000.0]]
    write_books(settings, no_books(markets, {"56-57°F": stale, "54-55°F": [[0.975, 300.0]]}),
                frames=[(SEEN + 1_000, change(by_label["56-57°F"]["no_token"], "SELL", 0.95, 12, SEEN + 900))])  # most of 0.95 is gone within the 2 s

    s = engine.process(conn, settings, engine.LIVE, T0, SEEN + 10_000)
    assert (s.triggers, s.signals, s.entered) == (1, 5, 1)             # five brackets lie below the 58°F already observed
    rows = {r["market_id"]: r for r in conn.execute("SELECT * FROM paper_signals")}
    entered = rows[by_label["56-57°F"]["market_id"]]
    assert (entered["decision"], entered["side"], entered["model_prob"], entered["best_ask"]) == ("enter", "NO", 1.0, 0.95)
    assert entered["edge"] == pytest.approx(1 - fills.cost_per_share(0.95, 0.01))
    assert rows[by_label["54-55°F"]["market_id"]]["skip_reason"] == "not_best"       # qualifies, but only the best is entered
    assert rows[by_label["52-53°F"]["market_id"]]["skip_reason"] == "below_threshold"  # NO at 0.999 leaves no 2pp
    assert json.loads(entered["detail"])["observed_high"] == 58

    cons, opt = [conn.execute("SELECT * FROM paper_fills WHERE variant=?", (v,)).fetchone() for v in ("conservative", "optimistic")]
    assert cons["fill_ms"] == SEEN + 2_000
    assert json.loads(cons["levels"])[0] == [0.95, 3.0]                # a quarter of the 12 shares left after the delay
    assert json.loads(cons["levels"])[1][0] == 0.97 and cons["notional"] + cons["fee"] == pytest.approx(10.0, abs=0.02)
    assert json.loads(opt["levels"]) == [[0.95, 10.52]]                # no delay, whole level: all of it at the old price
    assert (cons["shares"] * 1.0 - cons["notional"] - cons["fee"]) / (cons["notional"] + cons["fee"]) >= 0.02


def test_h2a_does_not_trade_frozen_stations_or_across_gaps_and_respects_the_caps(world):
    conn, settings, markets = world["conn"], world["settings"], world["markets"]
    write_books(settings, no_books(markets, {"56-57°F": [[0.95, 4000.0]]}))
    scanner.freeze_station(conn, "KLGA", "settlement_mismatch", "test", ts=SEEN - 1)
    engine.process(conn, settings, "frozen-run", T0, SEEN + 10_000)
    assert {r[0] for r in conn.execute("SELECT skip_reason FROM paper_signals WHERE run='frozen-run'")} == {"frozen"}
    scanner.thaw_station(conn, "KLGA", "test over", ts=SEEN - 1)

    conn.execute("INSERT INTO gaps(source, key, start_ms, end_ms, reason) VALUES('book', 'process', ?, ?, 'recorder restart')", (SEEN - 5_000, SEEN + 500))
    engine.process(conn, settings, "gap-run", T0, SEEN + 10_000)
    assert {r[0] for r in conn.execute("SELECT skip_reason FROM paper_signals WHERE run='gap-run'")} == {"data_gap"}
    conn.execute("DELETE FROM gaps")

    # The city-day cap is 60 USD: six entries of 10 USD, then nothing more.
    rules, city_day = h2a.RULES, ledger.open_city_day(conn, "KLGA", dt.datetime(2026, 11, 9, 16, 51, tzinfo=UTC), SEEN)
    books = BookHistory(settings, conn)
    for i in range(8):
        cands, _ = h2a.candidates(conn, city_day, SEEN + i)
        ledger.decide(conn, books, rules, "cap-run", SEEN + i, city_day, cands, "test")
    assert conn.execute("SELECT COUNT(*) FROM paper_fills WHERE run='cap-run' AND variant='conservative'").fetchone()[0] == 6
    assert ledger.spent(conn, "cap-run", "H2a", event_id=city_day.event_id) == pytest.approx(60.0, abs=0.1)
    last = conn.execute("SELECT skip_reason FROM paper_signals WHERE run='cap-run' AND ts_ms=? AND best_ask=0.95", (SEEN + 7,)).fetchone()
    assert last[0] == "over_cap"


def test_thin_books_are_skipped_for_lack_of_liquidity(world):
    conn, settings, markets = world["conn"], world["settings"], world["markets"]
    write_books(settings, no_books(markets, {"56-57°F": [[0.95, 12.0]]}))  # a quarter of 12 shares is under 5 USD
    engine.process(conn, settings, engine.LIVE, T0, SEEN + 10_000)
    row = conn.execute("SELECT skip_reason FROM paper_signals WHERE best_ask=0.95").fetchone()
    assert row[0] == "no_liquidity" and conn.execute("SELECT COUNT(*) FROM paper_fills").fetchone()[0] == 0


def test_measurements_follow_a_bracket_that_was_just_ruled_out(world):
    conn, settings, markets = world["conn"], world["settings"], world["markets"]
    by_label = {m["group_title"]: m for m in markets}
    token = by_label["56-57°F"]["no_token"]
    write_books(settings, no_books(markets, {"56-57°F": [[0.93, 100.0], [0.99, 100.0]]}),
                frames=[(SEEN + 30_000, change(token, "SELL", 0.93, 0, SEEN + 29_000))])
    s = engine.process(conn, settings, engine.LIVE, T0, SEEN + 10_000)
    # 16:51Z took the high from 57 to 58°F: only 56-57°F is newly ruled out, and it is measured at four delays
    assert s.measurements_planned == 4
    assert {r[0] for r in conn.execute("SELECT market_id FROM paper_measurements")} == {by_label["56-57°F"]["market_id"]}
    assert h2a.take_measurements(conn, BookHistory(settings, conn), engine.LIVE, SEEN + 70_000) == 2
    got = {r["offset_s"]: r for r in conn.execute("SELECT * FROM paper_measurements")}
    assert (got[0]["status"], got[0]["best_ask"], got[0]["fillable_usd"]) == ("ok", 0.93, pytest.approx(0.25 * (93 + 99)))
    assert got[0]["fillable_at_threshold_usd"] == pytest.approx(0.25 * 93)     # 0.99 leaves less than the 2pp threshold
    assert got[60]["fillable_at_threshold_usd"] == 0.0
    assert (got[60]["best_ask"], got[60]["edge"]) == (0.99, pytest.approx(1 - fills.cost_per_share(0.99, 0.01)))
    assert got[300]["status"] == "pending"
    h2a.take_measurements(conn, BookHistory(settings, conn), engine.LIVE, SEEN + ms(minutes=20))
    assert {r[0] for r in conn.execute("SELECT status FROM paper_measurements WHERE offset_s >= 300")} == {"gap"}  # recording ended


# ---- settlement, reconciliation, report ---------------------------------------------------------------


def test_positions_settle_by_the_market_result_and_the_report_adds_up(world):
    conn, settings, markets = world["conn"], world["settings"], world["markets"]
    by_label = {m["group_title"]: m for m in markets}
    write_books(settings, no_books(markets, {"56-57°F": [[0.95, 4000.0]]}))
    engine.process(conn, settings, engine.LIVE, T0, SEEN + 10_000)
    assert ledger.settle(conn, SEEN + ms(days=1)) == 0                # not resolved yet
    conn.execute("UPDATE markets SET winner='NO' WHERE market_id=?", (by_label["56-57°F"]["market_id"],))
    assert ledger.settle(conn, SEEN + ms(days=1)) == 2                # conservative and optimistic
    fill = conn.execute("SELECT * FROM paper_fills WHERE variant='conservative'").fetchone()
    assert fill["payout"] == fill["shares"]
    r = report.build(conn)
    c = r["ledgers"]["H2a"]["conservative"]
    assert c["settled_pnl_usd"] == pytest.approx(fill["shares"] - fill["notional"] - fill["fee"], abs=1e-4)
    assert c["return_per_usd"] >= 0.02 and c["open_usd"] == 0 and r["ledgers"]["H2a"]["entered"] == 1
    assert "H2a" in report.render(r)

    conn.execute("UPDATE paper_fills SET payout=NULL, settled_ms=NULL")
    conn.execute("UPDATE markets SET winner='YES' WHERE market_id=?", (by_label["56-57°F"]["market_id"],))  # had the report been wrong
    ledger.settle(conn, SEEN + ms(days=1))
    assert conn.execute("SELECT payout FROM paper_fills WHERE variant='conservative'").fetchone()[0] == 0.0


def test_replaying_a_date_reproduces_the_ledger_and_a_difference_pauses_everything(world):
    conn, settings, markets = world["conn"], world["settings"], world["markets"]
    write_books(settings, no_books(markets, {"56-57°F": [[0.95, 4000.0]]}))
    engine.process(conn, settings, engine.LIVE, T0, SEEN + 10_000)
    conn.execute("INSERT INTO kv(key, value) VALUES('paper_cursor_ms', ?)", (str(SEEN + 10_000),))

    ok = engine.reconcile(conn, settings, "2026-11-09", now=SEEN + ms(days=1))
    assert ok["ok"] and ok["ledgers"]["H2a"]["live_fills"] == ok["ledgers"]["H2a"]["replay_fills"] == 1
    assert ok["ledgers"]["H2a"]["difference_usd"] == 0
    assert ledger.paused_reason(conn, SEEN + ms(days=2), "KLGA", "H2a") is None

    conn.execute("UPDATE paper_fills SET notional = notional + 3 WHERE run='live' AND variant='conservative'")  # the ledger drifted
    bad = engine.reconcile(conn, settings, "2026-11-09", now=SEEN + ms(days=1))
    assert not bad["ok"] and bad["ledgers"]["H2a"]["difference_usd"] == pytest.approx(3.0)
    assert "reconciliation" in ledger.paused_reason(conn, SEEN + ms(days=2), "KLGA", "H2a")
    assert ledger.paused_reason(conn, SEEN, "KLGA", "H2a") is None    # ... but not for decisions made before the pause


def test_a_ledger_that_lost_a_quarter_of_its_money_is_paused(world):
    conn = world["conn"]
    conn.execute("""INSERT INTO paper_signals(run, ledger, ts_ms, event_id, market_id, station, obs_date, side, decision, model_version)
                    VALUES('live', 'H2a', 1, 'e', 'm', 'KLGA', '2026-11-09', 'NO', 'enter', 'h2a-1')""")
    conn.execute("""INSERT INTO paper_fills(signal_id, run, ledger, variant, fill_ms, event_id, market_id, station, obs_date, side, shares,
                    avg_price, notional, fee, payout) VALUES(1, 'live', 'H2a', 'conservative', 1, 'e', 'm', 'KLGA', '2026-11-09', 'NO', 600, 0.9, 540, 0, 0)""")
    assert engine.check_drawdown(conn, 10) == ["ledger H2a paused: drawdown"]
    assert engine.check_drawdown(conn, 20) == []                       # only once
    assert "drawdown" in ledger.paused_reason(conn, 30, "KLGA", "H2a")


# ---- H2b -------------------------------------------------------------------------------------------

from polyweather.paper import h2b  # noqa: E402
from polyweather.weather import lamp  # noqa: E402

CYCLE = dt.datetime(2026, 11, 9, 12, 30, tzinfo=UTC)          # 07:30 in New York
LAMP_SEEN = int(dt.datetime(2026, 11, 9, 12, 40, tzinfo=UTC).timestamp() * 1000)


def _iso(t):
    return t.strftime("%Y-%m-%dT%H:%M:%SZ")


def add_lamp(conn, cycle, hourly_f, seen_ms, live=False):
    """One LAMP cycle for KLGA: ``hourly_f`` are the temperatures for the hours after the cycle."""
    for k, tmp in enumerate(hourly_f, start=1):
        valid = cycle.replace(minute=0) + dt.timedelta(hours=k)
        conn.execute("INSERT INTO lamp_hourly(station, cycle, valid_utc, fhr, tmp_f, dpt_f, seen_ms) VALUES('KLGA', ?, ?, ?, ?, 30, ?)",
                     (_iso(cycle), _iso(valid), k, tmp, seen_ms))
    if live:
        conn.execute("INSERT INTO guidance_blocks(product, station, cycle, raw_block, source, fetched_ms) VALUES('lav', 'KLGA', ?, 'x', 'nomads', ?)",
                     (_iso(cycle), seen_ms))


def add_history(conn, days, peak_tenths_c, seen_ms):
    """Past days on which the 12:30Z cycle forecast a 60°F high for the rest of the day and the reports
    reached ``peak_tenths_c(k)`` tenths of a degree Celsius (150 is 59.0°F, 161 is 60.98°F)."""
    for k in range(1, days + 1):
        cycle = CYCLE - dt.timedelta(days=k)
        add_lamp(conn, cycle, [50, 55, 60, 58, 54] + [50] * 12, seen_ms)
        peak_tenths = peak_tenths_c(k)
        reports = []
        for h in range(13, 30):                                # 13:51Z .. 05:51Z next day, hourly
            when = cycle.replace(hour=0, minute=51) + dt.timedelta(hours=h)
            tenths = peak_tenths if h == 15 else peak_tenths - 30
            reports.append(MetarRow("KLGA", when, f"METAR KLGA {when:%d%H%M}Z 10SM {tenths // 10:02d}/01 RMK AO2 T0{tenths:03d}0011", "METAR"))
        store(conn, reports, "awc_api", seen_ms=seen_ms)


def test_bracket_probabilities_respect_the_observed_high_and_the_rounding_rule(world):
    city_day = ledger.open_city_day(world["conn"], "KLGA", dt.datetime(2026, 11, 9, 13, tzinfo=UTC), LAMP_SEEN)
    labels = {m["market_id"]: m["group_title"] for m, _ in city_day.markets}
    p = {labels[k]: v for k, v in h2b.bracket_probabilities(city_day, None, 57.5, 1.0).items()}
    assert sum(p.values()) == pytest.approx(1.0)
    assert p["56-57°F"] == pytest.approx(0.5 - 0.02275, abs=1e-4)    # exact values 55.5 up to 57.5: two sd below the centre to the centre
    assert p["58-59°F"] == pytest.approx(0.5 - 0.02275, abs=1e-4)
    # with 57.9°F already observed (58 after rounding), nothing below 58 can win and 58-59 takes all that mass
    q = {labels[k]: v for k, v in h2b.bracket_probabilities(city_day, 57.92, 57.5, 1.0).items()}
    assert q["56-57°F"] == 0 and q["54-55°F"] == 0 and sum(q.values()) == pytest.approx(1.0)
    assert q["58-59°F"] == pytest.approx(0.97725, abs=1e-4)


def test_past_errors_use_only_what_the_archive_held_at_the_time(world):
    conn = world["conn"]
    add_history(conn, 16, lambda k: 150 if k % 2 else 161, seen_ms=LAMP_SEEN - 1000)
    errors = h2b.past_errors(conn, "KLGA", CYCLE, LAMP_SEEN)
    assert len(errors) == 16 and sorted(set(round(e, 2) for e in errors)) == [-1.0, 0.98]   # reached minus the 60°F forecast
    assert h2b.past_errors(conn, "KLGA", CYCLE, LAMP_SEEN - 2000) == []           # fetched later: not known then
    assert h2b.past_errors(conn, "KLGA", CYCLE - dt.timedelta(hours=1), LAMP_SEEN) == []  # another cycle hour has its own history


def test_h2b_enters_the_best_priced_bracket_with_kelly_capped_at_twenty_dollars(world):
    conn, settings, markets = world["conn"], world["settings"], world["markets"]
    by_label = {m["group_title"]: m for m in markets}
    conn.execute("DELETE FROM metar_obs WHERE obs_time > '2026-11-09T12:00:00Z'")   # the decision is made at 12:40Z
    add_history(conn, 16, lambda k: 150 if k % 2 else 161, seen_ms=LAMP_SEEN - 60_000)   # errors -1.0 and +0.98: mean -0.01, sd 0.99
    add_lamp(conn, CYCLE, [52, 55, 57, 58, 56] + [50] * 12, LAMP_SEEN, live=True)        # 58°F still to come

    books = {}
    for m in markets:
        books[m["yes_token"]] = ([[0.01, 100.0]], [[0.99, 5000.0]])
        books[m["no_token"]] = ([[0.01, 100.0]], [[0.99, 5000.0]])
    books[by_label["58-59°F"]["yes_token"]] = ([[0.25, 100.0]], [[0.30, 4000.0]])        # the model says 0.63
    books[by_label["56-57°F"]["yes_token"]] = ([[0.15, 100.0]], [[0.20, 4000.0]])        # the model says 0.30: a smaller edge
    books[by_label["64-65°F"]["no_token"]] = ([[0.95, 100.0]], [[0.98, 4000.0]])         # almost certain, above the 97 cent cap
    lamp_snapshot = LAMP_SEEN - 20_000
    write_books(settings, books, at_ms=lamp_snapshot)

    s = engine.process(conn, settings, engine.LIVE, lamp_snapshot, LAMP_SEEN + 10_000)
    assert s.entered == 1 and s.decisions_not_taken == {}
    entered = conn.execute("SELECT * FROM paper_signals WHERE ledger='H2b' AND decision='enter'").fetchone()
    assert (entered["market_id"], entered["side"]) == (by_label["58-59°F"]["market_id"], "YES")
    detail = json.loads(entered["detail"])
    assert (detail["forecast_ahead_f"], detail["error_mean_f"], detail["error_sd_f"], detail["history_days"]) == (58.0, -0.01, 0.99, 16)
    import math
    phi = lambda x: 0.5 * (1 + math.erf((x - (58 - 0.01)) / 0.99 / math.sqrt(2)))  # noqa: E731
    assert entered["model_prob"] == pytest.approx(phi(59.5) - phi(57.5), abs=1e-6)           # exact values 57.5 up to 59.5
    assert 0.62 < entered["model_prob"] < 0.63
    assert entered["edge"] == pytest.approx(entered["model_prob"] - fills.cost_per_share(0.30, 0.01))
    fill = conn.execute("SELECT * FROM paper_fills WHERE ledger='H2b' AND variant='conservative'").fetchone()
    assert fill["notional"] + fill["fee"] == pytest.approx(20.0, abs=0.05)              # quarter Kelly asks for far more
    reasons = {(r["market_id"], r["side"]): r["skip_reason"] for r in conn.execute("SELECT * FROM paper_signals WHERE ledger='H2b' AND decision='skip'")}
    assert reasons[(by_label["56-57°F"]["market_id"], "YES")] == "not_best"
    assert reasons[(by_label["64-65°F"]["market_id"], "NO")] == "below_threshold"       # tail protection: no NO above 97 cents
    assert (by_label["54-55°F"]["market_id"], "YES") in reasons                         # still possible at 12:40Z: 55°F observed so far

    # the next cycle may not add YES on the neighbouring bracket
    conn.execute("DELETE FROM lamp_hourly WHERE cycle=?", (_iso(CYCLE + dt.timedelta(hours=1)),))
    cands, _ = h2b.candidates(conn, ledger.open_city_day(conn, "KLGA", dt.datetime(2026, 11, 9, 13, tzinfo=UTC), LAMP_SEEN), LAMP_SEEN + 5, CYCLE, engine.LIVE)
    yes = {c.market["group_title"] for c in cands if c.side == "YES"}
    assert "58-59°F" in yes and "56-57°F" not in yes and "60-61°F" not in yes


def test_h2b_waits_for_enough_history_and_stops_at_eight_in_the_evening(world):
    conn, settings = world["conn"], world["settings"]
    add_history(conn, 10, lambda k: 156, seen_ms=LAMP_SEEN - 60_000)
    add_lamp(conn, CYCLE, [52, 55, 57, 58, 56] + [50] * 12, LAMP_SEEN, live=True)
    write_books(settings, {}, at_ms=LAMP_SEEN - 20_000)
    s = engine.process(conn, settings, engine.LIVE, LAMP_SEEN - 20_000, LAMP_SEEN + 10_000)
    assert s.decisions_not_taken == {"H2b: not enough past days for this cycle hour": 1}
    assert conn.execute("SELECT COUNT(*) FROM paper_signals WHERE ledger='H2b'").fetchone()[0] == 0

    evening = dt.datetime(2026, 11, 10, 1, 30, tzinfo=UTC)                           # 20:30 in New York
    seen = int(evening.timestamp() * 1000) + 600_000
    city_day = ledger.open_city_day(conn, "KLGA", evening, seen)
    add_lamp(conn, evening, [50, 49, 48], seen, live=True)
    assert h2b.candidates(conn, city_day, seen, evening, engine.LIVE)[1]["skipped"] == "after 20:00 local time"


def test_iem_lamp_archive_parses_into_hourly_rows(fixtures):
    rows = lamp.parse_iem_csv((fixtures / "iem_lav_klga_20261124.csv").read_text(encoding="utf-8"))
    first = rows[0]
    assert (first.station, first.cycle, first.valid, first.fhr, first.tmp_f) == (
        "KLGA", dt.datetime(2026, 11, 24, 10, 30, tzinfo=UTC), dt.datetime(2026, 11, 24, 11, 0, tzinfo=UTC), 1, 45)
    assert len({r.cycle for r in rows}) == 4 and max(r.fhr for r in rows) == 38


# ---- the live run ------------------------------------------------------------------------------------


def _recorder_state(settings, alive_ms, flushed_ms):
    (settings.state_dir / "recorder.json").write_text(json.dumps({"last_alive_ms": alive_ms, "flushed_through_ms": flushed_ms}), encoding="utf-8")


def test_live_run_stays_behind_the_recording_and_never_handles_a_trigger_twice(world):
    conn, settings, markets = world["conn"], world["settings"], world["markets"]
    write_books(settings, no_books(markets, {"56-57°F": [[0.95, 4000.0]]}))

    # the recorder has written up to one minute past the report: too close, the engine waits
    _recorder_state(settings, SEEN + 61_000, SEEN + 60_000)
    first = engine.run_live(conn, settings, now=SEEN + 61_000)
    assert first.triggers == 0 and conn.execute("SELECT COUNT(*) FROM paper_signals").fetchone()[0] == 0

    _recorder_state(settings, SEEN + 301_000, SEEN + 300_000)
    second = engine.run_live(conn, settings, now=SEEN + 301_000)
    assert (second.triggers, second.entered) == (1, 1)
    assert second.to_ms == SEEN + 300_000 - engine.LAG_MS
    assert second.measurements_planned == 4 and second.measurements_taken == 2      # +0 s and +60 s are behind the horizon already

    third = engine.run_live(conn, settings, now=SEEN + 302_000)
    assert third.triggers == 0 and conn.execute("SELECT COUNT(*) FROM paper_fills WHERE variant='conservative'").fetchone()[0] == 1


def test_a_stopped_recorder_holds_the_engine_until_it_is_back(world):
    conn, settings, markets = world["conn"], world["settings"], world["markets"]
    write_books(settings, no_books(markets, {"56-57°F": [[0.95, 4000.0]]}))
    # The recorder wrote its last data 100 s after the report and has been silent since. The hole it is
    # leaving is not logged yet, so the engine may not treat its last books as current ones.
    _recorder_state(settings, SEEN + 101_000, SEEN + 100_000)
    waiting = engine.run_live(conn, settings, now=SEEN + 3_600_000)
    assert waiting.triggers == 0 and conn.execute("SELECT COUNT(*) FROM paper_signals").fetchone()[0] == 0

    # It starts again, logs the hole, and records on: the report from before the stop is handled now.
    conn.execute("INSERT INTO gaps(source, key, start_ms, end_ms, reason) VALUES('book', 'process', ?, ?, 'recorder restart')", (SEEN + 100_000, SEEN + 3_600_000))
    _recorder_state(settings, SEEN + 3_900_000, SEEN + 3_899_000)
    s = engine.run_live(conn, settings, now=SEEN + 3_900_000)
    assert (s.triggers, s.entered) == (1, 1)


def test_a_run_killed_half_way_continues_without_doing_anything_twice(world, monkeypatch):
    conn, settings, markets = world["conn"], world["settings"], world["markets"]
    write_books(settings, no_books(markets, {"56-57°F": [[0.95, 4000.0]]}))
    cycle = dt.datetime(2026, 11, 9, 16, 30, tzinfo=UTC)
    add_lamp(conn, cycle, [60] * 12, SEEN + 30_000, live=True)       # a second trigger half a minute after the report
    _recorder_state(settings, SEEN + 601_000, SEEN + 600_000)

    def killed(*args, **kwargs):
        raise KeyboardInterrupt

    with monkeypatch.context() as m:
        m.setattr(engine, "_handle_lamp", killed)
        with pytest.raises(KeyboardInterrupt):
            engine.run_live(conn, settings, now=SEEN + 601_000)
    assert conn.execute("SELECT COUNT(*) FROM paper_fills WHERE variant='conservative'").fetchone()[0] == 1   # the report was done
    assert engine._cursor(conn) == SEEN                                                                     # and the cursor says so

    again = engine.run_live(conn, settings, now=SEEN + 601_000)
    assert again.triggers == 1                                          # only the LAMP cycle; the report is not handled again
    assert conn.execute("SELECT COUNT(*) FROM paper_fills WHERE variant='conservative' AND ledger='H2a'").fetchone()[0] == 1
    assert engine._cursor(conn) == SEEN + 600_000 - engine.LAG_MS


# ---- whole sets of brackets ------------------------------------------------------------------------------

from polyweather.paper import sets  # noqa: E402


def test_a_whole_set_cheaper_than_its_payout_is_found_and_counted(world):
    conn, settings, markets = world["conn"], world["settings"], world["markets"]
    n = len(markets)
    cheap = {m["yes_token"]: ([], [[0.08, 50.0 + i]]) for i, m in enumerate(markets)}          # 11 legs at 0.08: 0.88 plus fees
    fair = {m["no_token"]: ([], [[0.93, 500.0]]) for m in markets}                               # 11 x 0.93 = 10.23 > 10
    write_books(settings, {**cheap, **fair})
    books = BookHistory(settings, conn)
    assert sets.scan(conn, books, T0 + 1) == {"sets_checked": 2, "sets_profitable": 1}
    row = conn.execute("SELECT * FROM paper_set_checks").fetchone()
    cost = n * fills.cost_per_share(0.08, 0.01)
    assert (row["side"], row["legs"]) == ("YES", n) and row["cost"] == pytest.approx(cost) and row["edge"] == pytest.approx(1 - cost)
    assert row["sets_at_best"] == 50.0                                  # the thinnest leg limits how many sets can be bought
    assert sets.scan(conn, books, T0 + 1) == {"sets_checked": 0, "sets_profitable": 0}   # each snapshot is looked at once
    s = sets.summary(conn)
    assert s["YES"]["with_a_sure_profit"] == 1 and s["NO"]["with_a_sure_profit"] == 0
    assert s["NO"]["closest_edge"] == pytest.approx((n - 1) - n * fills.cost_per_share(0.93, 0.01), abs=1e-4)
    assert "set of YES of every bracket" in report.render(report.build(conn))


def test_a_set_with_a_leg_nobody_sells_is_not_counted(world):
    conn, settings, markets = world["conn"], world["settings"], world["markets"]
    books_ = {m["yes_token"]: ([], [[0.05, 50.0]]) for m in markets}
    books_[markets[0]["yes_token"]] = ([[0.01, 5.0]], [])               # no seller on one leg
    write_books(settings, books_)
    assert sets.scan(conn, BookHistory(settings, conn), T0 + 1) == {"sets_checked": 0, "sets_profitable": 0}


def test_no_shares_of_some_brackets_are_enough_for_a_sure_profit(world):
    conn, settings, markets = world["conn"], world["settings"], world["markets"]
    three = {m["no_token"]: ([], [[0.60, 40.0]]) for m in markets[:3]}   # three NO at 0.60: at least two of them pay 1
    write_books(settings, three)
    assert sets.scan(conn, BookHistory(settings, conn), T0 + 1) == {"sets_checked": 1, "sets_profitable": 1}
    row = conn.execute("SELECT * FROM paper_set_checks").fetchone()
    cost = 3 * fills.cost_per_share(0.60, 0.01)
    assert (row["side"], row["legs"]) == ("NO", 3) and row["edge"] == pytest.approx(2 - cost)


# ---- the archive's time stamps are what "as of" rests on -------------------------------------------------


class _Bulletins:
    """Stands in for NOMADS: serves the fixture for the hourly bulletin, nothing for the extended one."""

    def __init__(self, body: bytes):
        self.body = body

    def get(self, url, follow_redirects=False):
        ok = url.endswith("lavtxt.ascii")
        return type("R", (), {"status_code": 200 if ok else 404, "content": self.body if ok else b"", "headers": {}})()


def test_a_cycle_archived_by_the_fetch_job_is_usable_at_its_own_trigger_time(conn, fixtures):
    cycle = dt.datetime(2026, 11, 24, 9, 30, tzinfo=UTC)
    assert lamp.archive_cycle(conn, _Bulletins((fixtures / "lav_sample_20261124_0930.txt").read_bytes()), cycle, {"KLGA"}) == 1
    (ts, station, found), = engine.lamp_triggers(conn, 0, 2**62)
    assert (station, found) == ("KLGA", cycle)
    assert {r[0] for r in conn.execute("SELECT seen_ms FROM lamp_hourly")} == {ts}     # same stamp as the bulletin, not later
    day_end = dt.datetime(2026, 11, 25, 5, tzinfo=UTC)
    assert h2b.forecast_ahead(conn, "KLGA", cycle, cycle, day_end, seen_until_ms=ts) is not None
    assert h2b.forecast_ahead(conn, "KLGA", cycle, cycle, day_end, seen_until_ms=ts - 1) is None


def test_two_runs_at_once_do_not_book_anything_twice(world):
    conn, settings, markets = world["conn"], world["settings"], world["markets"]
    write_books(settings, no_books(markets, {"56-57°F": [[0.95, 4000.0]]}))
    # Both runs read the cursor before either had written: both believe the report is theirs to handle.
    first = engine.process(conn, settings, engine.LIVE, T0, SEEN + 10_000, move_cursor=True)
    second = engine.process(conn, settings, engine.LIVE, T0, SEEN + 10_000, move_cursor=True)
    assert (first.entered, second.entered, second.triggers) == (1, 0, 0)
    assert conn.execute("SELECT COUNT(*) FROM paper_fills WHERE variant='conservative'").fetchone()[0] == 1


def test_a_replay_starts_where_the_ledger_of_record_began(world):
    conn, settings, markets = world["conn"], world["settings"], world["markets"]
    write_books(settings, no_books(markets, {"56-57°F": [[0.95, 4000.0]]}))
    _recorder_state(settings, SEEN + 601_000, SEEN + 600_000)
    engine.run_live(conn, settings, now=SEEN + 601_000)
    assert int(conn.execute("SELECT value FROM kv WHERE key='paper_start_ms'").fetchone()[0]) == T0 - 1
    engine.reconcile(conn, settings, "2026-11-09", now=SEEN + ms(days=1))

    def signals(run):
        return [tuple(r) for r in conn.execute(
            "SELECT ledger, ts_ms, market_id, side, decision, skip_reason FROM paper_signals WHERE run=? ORDER BY 1, 2, 3, 4", (run,))]

    # the morning's reports were fetched an hour before the recording began: neither run decides on them
    assert signals("replay:2026-11-09") == signals(engine.LIVE) and len(signals(engine.LIVE)) == 5


def test_measurements_of_a_report_seen_late_are_reported_apart(world):
    conn, settings, markets = world["conn"], world["settings"], world["markets"]
    by_label = {m["group_title"]: m for m in markets}
    write_books(settings, no_books(markets, {"56-57°F": [[0.93, 100.0]]}))
    engine.process(conn, settings, engine.LIVE, T0, SEEN + 10_000)          # the 16:51Z report, seen a few minutes later
    h2a.take_measurements(conn, BookHistory(settings, conn), engine.LIVE, SEEN + 10_000)
    timely = report.build(conn)["h2a_ruled_out_brackets"]
    assert timely["during_the_day"]["+0s"]["measured"] == 1 and timely["report_seen_late"]["+0s"]["brackets"] == 0

    conn.execute("UPDATE paper_measurements SET seen_ms = seen_ms + ?", (ms(hours=1),))   # the same report, fetched after an outage
    late = report.build(conn)["h2a_ruled_out_brackets"]
    assert late["during_the_day"]["+0s"]["brackets"] == 0 and late["report_seen_late"]["+0s"]["measured"] == 1
    assert "seen more than 15 minutes late" in report.render(report.build(conn))


def test_brackets_ruled_out_by_the_first_report_of_the_day_are_marked(world):
    conn, settings, markets = world["conn"], world["settings"], world["markets"]
    write_books(settings, no_books(markets, {"56-57°F": [[0.93, 100.0]]}))
    conn.execute("DELETE FROM metar_obs WHERE first_seen_ms < ?", (SEEN,))      # nothing was seen of this day before 16:51Z
    s = engine.process(conn, settings, engine.LIVE, T0, SEEN + 10_000)
    assert s.measurements_planned == 5 * 4                                      # all five brackets below 58°F fall at once
    assert {r[0] for r in conn.execute("SELECT first_of_day FROM paper_measurements")} == {1}
    h2a.take_measurements(conn, BookHistory(settings, conn), engine.LIVE, SEEN + 10_000)
    groups = report.build(conn)["h2a_ruled_out_brackets"]
    assert groups["by_the_first_report_of_the_day"]["+0s"]["measured"] == 5 and groups["during_the_day"]["+0s"]["brackets"] == 0
