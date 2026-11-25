import asyncio
import json

import pyarrow.parquet as pq
import pytest

from polyweather.marketdata.replay import ReplaySource
from polyweather.recorder.book import BookState, price_key
from polyweather.recorder.recorder import Recorder, load_universe
from polyweather.settings import Settings
from polyweather.storage.parquet_sink import RollingParquetSink, read_partitions
from polyweather.catalog import scanner


def book(asset, bids, asks, h="h0", ts="1000"):
    lv = lambda rows: [{"price": p, "size": s} for p, s in rows]  # noqa: E731
    return {"event_type": "book", "market": "0xm", "asset_id": asset, "timestamp": ts, "hash": h, "bids": lv(bids), "asks": lv(asks)}


def change(asset, price, size, side, h):
    return {"event_type": "price_change", "market": "0xm", "timestamp": "2000",
            "price_changes": [{"asset_id": asset, "price": price, "size": size, "side": side, "hash": h}]}


def test_book_state_applies_snapshots_and_level_updates():
    st = BookState()
    st.apply(book("A", [("0.40", "100"), ("0.41", "50")], [("0.45", "30"), ("0.46", "10")]), 1)
    st.apply(change("A", "0.42", "25", "BUY", "h1"), 2)   # new best bid
    st.apply(change("A", "0.45", "0", "SELL", "h2"), 3)   # best ask removed
    st.apply(change("A", "0.41", "75.5", "BUY", "h3"), 4)  # level resized
    st.apply(change("B", "0.10", "5", "BUY", "hx"), 5)    # no snapshot for B: ignored
    b = st.books["A"]
    assert (b.best_bid, b.best_ask, b.last_hash, b.n_updates) == (0.42, 0.46, "h3", 4)
    assert b.levels() == ([(0.42, 25.0), (0.41, 75.5), (0.40, 100.0)], [(0.46, 10.0)])
    assert list(b.hashes) == ["h0", "h1", "h2", "h3"] and "B" not in st.books
    assert price_key("0.001") == 10 and price_key(0.999) == 9990


def test_parquet_sink_rolls_files_by_utc_date(tmp_path):
    from polyweather.recorder.recorder import RAW_SCHEMA

    sink = RollingParquetSink(tmp_path, "book", RAW_SCHEMA, "recv_ms", flush_rows=3)
    day1 = 1_795_478_400_000  # 2026-11-24T00:00:00Z
    for i in range(4):
        sink.append((day1 - 2000 + i * 1000, 1, i, f"m{i}"))  # two rows before midnight, two after
    sink.close()
    files = read_partitions(tmp_path, "2026-11-23", "2026-11-24")
    assert [f.parent.name for f in files] == ["date=2026-11-23", "date=2026-11-24"]
    assert [pq.read_table(f).num_rows for f in files] == [2, 2]
    assert sink.rows_written == 4 and not list(tmp_path.rglob("*.tmp"))
    assert read_partitions(tmp_path, "2026-11-24", "2026-11-24") == files[1:]


class FakeConnection:
    """Stands in for MarketConnection: the test drives frames and state changes by hand."""

    instances: list["FakeConnection"] = []

    def __init__(self, conn_id, assets, on_frame, on_state):
        self.conn_id, self.assets, self.on_frame, self.on_state = conn_id, set(assets), on_frame, on_state
        self.resynced: list[str] = []
        FakeConnection.instances.append(self)

    def add(self, assets):
        self.assets.update(assets)

    def remove(self, assets):
        self.assets.difference_update(assets)

    def resync(self, assets):
        self.resynced += list(assets)

    def stop(self):
        pass

    async def run(self):
        await asyncio.sleep(3600)


@pytest.fixture
def recorder(tmp_path, conn, nyc_event):
    FakeConnection.instances.clear()
    open_event = json.loads(json.dumps(nyc_event))
    open_event["closed"] = False
    for m in open_event["markets"]:
        m["closed"] = False
    scanner.ingest(conn, [open_event])
    settings = Settings(data_dir=tmp_path, scope="us")
    settings.ensure_dirs()
    return Recorder(settings, conn, connection_factory=FakeConnection)


async def test_recorder_lands_frames_marks_gaps_and_replays(recorder, conn, tmp_path):
    assert len(load_universe(conn, "us")) == 22  # 11 brackets, YES and NO
    assert load_universe(conn, "all").keys() == load_universe(conn, "us").keys()
    recorder._sync_universe()
    fc = FakeConnection.instances[0]
    asset = sorted(fc.assets)[0]

    t = 1_795_500_000_000
    fc.on_state(1, "connected", t, "22 assets")
    fc.on_frame(t + 10, 1, json.dumps([book(asset, [("0.40", "100")], [("0.45", "30")])]))
    fc.on_frame(t + 20, 1, "PONG")  # heartbeats are not stored
    fc.on_frame(t + 30, 1, json.dumps(change(asset, "0.41", "7", "BUY", "h1")))
    assert recorder.state.books[asset].best_bid == 0.41

    # Disconnect: a gap opens at the last frame time and the stale book is dropped.
    fc.on_state(1, "disconnected", t + 30, "ConnectionClosed")
    assert asset not in recorder.state.books
    gap = conn.execute("SELECT * FROM gaps WHERE source='book'").fetchone()
    assert (gap["start_ms"], gap["end_ms"], gap["key"]) == (t + 30, None, "conn:1")

    # Reconnect: the first frame closes the gap.
    fc.on_frame(t + 95_000, 1, json.dumps([book(asset, [("0.39", "5")], [("0.44", "9")], h="h9")]))
    gap = conn.execute("SELECT * FROM gaps WHERE source='book'").fetchone()
    assert gap["end_ms"] == t + 95_000
    recorder._write_snapshots()
    recorder.raw.close()

    replay = ReplaySource(tmp_path / "raw", conn, t, t + 100_000)
    frames = list(replay.frames())
    assert [f[0] - t for f in frames] == [10, 30, 95_000]
    assert replay.book_snapshot(asset, at_ms=t + 40)["bids"][0] == (0.41, 7.0)
    assert replay.book_snapshot(asset)["hash"] == "h9"
    assert replay.gaps() == [(t + 30, t + 95_000, "conn:1", "disconnect: ConnectionClosed")]
    assert len(list(replay.messages([asset]))) == 3 and list(replay.messages(["nope"])) == []
    assert len(replay.list_markets()) == 11


async def test_recorder_resyncs_a_book_that_differs_from_a_newer_snapshot(recorder, conn, monkeypatch):
    from polyweather.polymarket import clob
    from polyweather.recorder import recorder as rec_mod

    recorder._sync_universe()
    fc = FakeConnection.instances[0]
    asset = sorted(fc.assets)[0]
    fc.on_frame(1_795_500_000_000, 1, json.dumps([book(asset, [("0.40", "100")], [("0.45", "30")], h="seen", ts="1000")]))

    async def rest_books(client, ids):
        # A bid the feed never delivered, in a snapshot newer than the feed's last change.
        return [{"asset_id": a, "hash": rest_hash[0], "timestamp": "5000",
                 "bids": [{"price": "0.43", "size": "9"}], "asks": []} for a in ids]

    monkeypatch.setattr(clob, "get_books", rest_books)
    monkeypatch.setattr(rec_mod, "CHECK_GRACE_S", 0.0)

    rest_hash = ["seen"]
    await recorder._check_books(None)
    assert (recorder.stats.checks_ok, recorder.stats.checks_mismatch) == (1, 0)

    rest_hash[0] = "never-in-feed"
    await recorder._check_books(None)  # goes pending
    assert recorder.stats.checks_mismatch == 0 and asset in recorder._pending_checks
    await recorder._check_books(None)  # second look differs too: one more chance for the feed to catch up
    assert recorder.stats.checks_mismatch == 0 and asset in recorder._pending_checks
    await recorder._check_books(None)  # still different: the feed missed something
    assert recorder.stats.checks_mismatch == 1 and recorder.stats.checks_stale_snapshot == 0
    assert fc.resynced == [asset] and asset not in recorder.state.books
    row = conn.execute("SELECT key, reason FROM gaps WHERE reason='hash_mismatch'").fetchone()
    assert row["key"] == f"asset:{asset}"


async def test_a_stale_exchange_snapshot_does_not_replace_the_feed_built_book(recorder, conn, monkeypatch):
    """What the exchange did on 2026-11-24: two cancellations in the same instant, and a
    snapshot stuck between them with a timestamp just before theirs."""
    from polyweather.polymarket import clob
    from polyweather.recorder import recorder as rec_mod

    recorder._sync_universe()
    fc = FakeConnection.instances[0]
    asset = sorted(fc.assets)[0]
    fc.on_frame(1, 1, json.dumps([book(asset, [("0.05", "372"), ("0.07", "372")], [], h="before", ts="1795533020000")]))
    batch = {"event_type": "price_change", "market": "0xm", "timestamp": "1795533025005", "price_changes": [
        {"asset_id": asset, "price": "0.05", "size": "186", "side": "BUY", "hash": "after-both"},
        {"asset_id": asset, "price": "0.07", "size": "186", "side": "BUY", "hash": "after-both"}]}
    fc.on_frame(2, 1, json.dumps(batch))

    async def rest_books(client, ids):
        return [{"asset_id": a, "hash": "between-the-two", "timestamp": "1795533025000",
                 "bids": [{"price": "0.05", "size": "186"}, {"price": "0.07", "size": "372"}], "asks": []} for a in ids]

    monkeypatch.setattr(clob, "get_books", rest_books)
    monkeypatch.setattr(rec_mod, "CHECK_GRACE_S", 0.0)
    for _ in range(4):
        await recorder._check_books(None)

    assert recorder.stats.checks_stale_snapshot >= 1 and recorder.stats.checks_mismatch == 0
    assert fc.resynced == [] and recorder.state.books[asset].bids == {price_key("0.05"): 186.0, price_key("0.07"): 186.0}
    assert conn.execute("SELECT COUNT(*) FROM gaps").fetchone()[0] == 0


async def test_a_failed_second_read_gives_no_verdict(recorder, conn, monkeypatch):
    from polyweather.polymarket import clob
    from polyweather.recorder import recorder as rec_mod

    recorder._sync_universe()
    fc = FakeConnection.instances[0]
    asset = sorted(fc.assets)[0]
    fc.on_frame(1, 1, json.dumps([book(asset, [("0.40", "100")], [("0.45", "30")], h="seen")]))
    calls = []

    async def rest_books(client, ids):
        calls.append(list(ids))
        if len(calls) == 2:  # the re-check of the pending book
            raise RuntimeError("exchange unavailable")
        return [{"asset_id": a, "hash": "never-in-feed", "timestamp": "5000", "bids": [], "asks": []} for a in ids]

    monkeypatch.setattr(clob, "get_books", rest_books)
    monkeypatch.setattr(rec_mod, "CHECK_GRACE_S", 0.0)
    await recorder._check_books(None)  # pending
    await recorder._check_books(None)  # the second read fails
    assert recorder.stats.checks_mismatch == 0 and fc.resynced == [] and asset in recorder.state.books
    assert conn.execute("SELECT COUNT(*) FROM gaps").fetchone()[0] == 0


async def test_pending_check_clears_when_the_hash_arrives(recorder, monkeypatch):
    from polyweather.polymarket import clob

    recorder._sync_universe()
    fc = FakeConnection.instances[0]
    asset = sorted(fc.assets)[0]
    fc.on_frame(1, 1, json.dumps([book(asset, [("0.40", "100")], [("0.45", "30")], h="old")]))

    async def rest_books(client, ids):
        return [{"asset_id": a, "hash": "newer"} for a in ids]

    monkeypatch.setattr(clob, "get_books", rest_books)
    await recorder._check_books(None)
    assert asset in recorder._pending_checks
    fc.on_frame(2, 1, json.dumps(change(asset, "0.41", "7", "BUY", "newer")))  # the feed catches up
    await recorder._check_books(None)
    assert recorder.stats.checks_mismatch == 0 and recorder.stats.checks_ok >= 1


async def test_unknown_hash_with_identical_levels_is_not_a_mismatch(recorder, conn, monkeypatch):
    from polyweather.polymarket import clob
    from polyweather.recorder import recorder as rec_mod

    recorder._sync_universe()
    fc = FakeConnection.instances[0]
    asset = sorted(fc.assets)[0]
    fc.on_frame(1_795_500_000_000, 1, json.dumps([book(asset, [("0.40", "100")], [("0.45", "30")], h="seen")]))

    async def rest_books(client, ids):
        # Same price levels, but a hash that is not in the recorder's history.
        return [{"asset_id": a, "hash": "outside-history", "bids": [{"price": "0.4", "size": "100.0"}],
                 "asks": [{"price": "0.45", "size": "30"}]} for a in ids]

    monkeypatch.setattr(clob, "get_books", rest_books)
    monkeypatch.setattr(rec_mod, "CHECK_GRACE_S", 0.0)
    await recorder._check_books(None)   # pending
    await recorder._check_books(None)   # grace over: second read matches level by level
    assert (recorder.stats.checks_mismatch, recorder.stats.checks_recovered) == (0, 1)
    assert fc.resynced == [] and asset in recorder.state.books
    assert conn.execute("SELECT COUNT(*) FROM gaps").fetchone()[0] == 0


def test_a_heartbeat_file_that_cannot_be_written_does_not_stop_the_recorder(recorder, monkeypatch):
    from polyweather.recorder import recorder as rec_mod

    def locked(a, b):
        raise PermissionError(13, "The process cannot access the file because it is being used by another process")

    monkeypatch.setattr(rec_mod.os, "replace", locked)
    recorder._heartbeat()
    recorder._heartbeat()  # no exception, and only one warning for a run of failures
    assert recorder._heartbeat_failed is True
    monkeypatch.undo()
    recorder._heartbeat()
    assert recorder._heartbeat_failed is False and recorder._state_path.exists()


def _restart_gap_start(conn):
    return conn.execute("SELECT start_ms FROM gaps WHERE reason='recorder restart' ORDER BY id DESC LIMIT 1").fetchone()[0]


async def test_restart_gap_starts_at_the_last_frame_on_disk_not_the_last_sign_of_life(tmp_path, conn, recorder):
    """A run killed outright loses the frames it still held in memory."""
    recorder._sync_universe()
    fc = FakeConnection.instances[0]
    asset = sorted(fc.assets)[0]
    fc.on_frame(1_795_500_000_000, 1, json.dumps([book(asset, [("0.40", "100")], [], h="a")]))
    recorder.raw.flush()                                    # this frame is on disk
    fc.on_frame(1_795_500_020_000, 1, json.dumps(change(asset, "0.41", "7", "BUY", "b")))  # this one is not
    recorder._heartbeat()                                   # ... and then the power goes

    successor = Recorder(recorder.settings, conn, connection_factory=FakeConnection)
    successor._restart_gap()
    assert _restart_gap_start(conn) == 1_795_500_000_000


def test_restart_gap_of_a_run_that_wrote_nothing_starts_when_that_run_started(conn, recorder):
    recorder._started_ms = 1_795_500_100_000
    recorder._heartbeat()                                   # alive, but killed before its first flush
    successor = Recorder(recorder.settings, conn, connection_factory=FakeConnection)
    successor._restart_gap()
    assert _restart_gap_start(conn) == 1_795_500_100_000


async def test_restart_gap_after_a_clean_stop_starts_at_the_last_frame(conn, recorder):
    recorder._sync_universe()
    fc = FakeConnection.instances[0]
    asset = sorted(fc.assets)[0]
    fc.on_frame(1_795_500_200_000, 1, json.dumps([book(asset, [("0.40", "100")], [], h="a")]))
    recorder.raw.close()                                    # what a clean shutdown does
    recorder._heartbeat()
    successor = Recorder(recorder.settings, conn, connection_factory=FakeConnection)
    successor._restart_gap()
    assert _restart_gap_start(conn) == 1_795_500_200_000


def test_sink_removes_old_half_written_files_but_not_fresh_ones(tmp_path):
    import os, time
    from polyweather.recorder.recorder import RAW_SCHEMA

    day = tmp_path / "date=2026-11-25"
    day.mkdir()
    old, fresh, other = day / "book-1-99-000003.tmp", day / "book-2-99-000004.tmp", day / "snap-1-99-000001.tmp"
    for f in (old, fresh, other):
        f.write_bytes(b"partial")
    long_ago = time.time() - 3600
    os.utime(old, (long_ago, long_ago)); os.utime(other, (long_ago, long_ago))
    RollingParquetSink(tmp_path, "book", RAW_SCHEMA, "recv_ms")
    assert not old.exists() and fresh.exists() and other.exists()  # only this sink's own, only old ones


def test_a_gap_left_open_by_a_killed_run_is_closed_when_the_next_one_starts(conn, recorder):
    """Killed while a connection was down: nobody was left to close that gap."""
    recorder._heartbeat()
    conn.execute("INSERT INTO gaps(source, key, start_ms, reason) VALUES('book', 'conn:2', 1795500000000, 'disconnect')")
    conn.execute("INSERT INTO gaps(source, key, start_ms, reason) VALUES('metar', 'awc', 1795500000000, 'not ours to close')")
    successor = Recorder(recorder.settings, conn, connection_factory=FakeConnection)
    successor._restart_gap()
    assert conn.execute("SELECT COUNT(*) FROM gaps WHERE source='book' AND end_ms IS NULL").fetchone()[0] == 0
    closed = conn.execute("SELECT end_ms FROM gaps WHERE key='conn:2'").fetchone()[0]
    restart = conn.execute("SELECT end_ms FROM gaps WHERE reason='recorder restart'").fetchone()[0]
    assert abs(closed - restart) < 1000                                 # it ends where the restart gap ends
    assert conn.execute("SELECT end_ms FROM gaps WHERE source='metar'").fetchone()[0] is None
