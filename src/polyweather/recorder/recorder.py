"""Order-book recorder: the one piece of this system that cannot be backfilled.

What it writes
  raw/book/date=.../book-*.parquet      every feed frame, verbatim, with local receive
                                        time in milliseconds (the replay source)
  raw/snapshots/date=.../snap-*.parquet every 60 s, each rebuilt book in full
  gaps (SQLite)                         intervals in which the feed cannot be trusted:
                                        disconnects, process restarts, hash mismatches

Consistency check: every 60 s a sample of books is fetched over REST. Each feed
update carries the hash of the book it produces, so the REST hash must appear in that
outcome's recent feed history. If it still has not appeared after a grace period the
local book has diverged; the outcome is re-subscribed for a fresh snapshot and the
interval since its last good check is logged as a gap.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import random
import sqlite3
from dataclasses import dataclass, field

import pyarrow as pa

from .. import stations as station_registry
from ..polymarket import clob
from ..polymarket.ws import MarketConnection
from ..settings import Settings
from ..storage.parquet_sink import RollingParquetSink
from ..util.http import make_async_client
from ..util.timeutil import now_ms
from .book import BookState, as_int, price_key

log = logging.getLogger(__name__)

RAW_SCHEMA = pa.schema(
    [("recv_ms", pa.int64()), ("conn", pa.int16()), ("seq", pa.int64()), ("msg", pa.string())]
)
SNAP_SCHEMA = pa.schema(
    [
        ("snap_ms", pa.int64()),
        ("asset_id", pa.string()),
        ("market", pa.string()),
        ("exch_ts_ms", pa.int64()),
        ("recv_ms", pa.int64()),
        ("hash", pa.string()),
        ("n_updates", pa.int64()),
        ("bids", pa.string()),  # JSON [[price, size], ...] best first
        ("asks", pa.string()),
    ]
)

ASSETS_PER_CONNECTION = 400
SNAPSHOT_EVERY_S = 60.0
CHECK_EVERY_S = 60.0
CHECK_SAMPLE = 40
CHECK_GRACE_S = 15.0
CHECK_RETRIES = 1  # further looks at a book that differs from a newer snapshot, before resyncing
UNIVERSE_EVERY_S = 60.0
GAP_TRIGGER_MS = 60_000  # a book gap longer than this is a pause trigger in the plan


def load_universe(conn: sqlite3.Connection, scope: str) -> dict[str, str]:
    """{asset id: market id} for every open weather market in scope."""
    rows = conn.execute(
        """SELECT m.market_id, m.yes_token, m.no_token, s.station
           FROM markets m JOIN events e ON e.event_id = m.event_id
           LEFT JOIN resolution_specs s ON s.event_id = e.event_id
           WHERE e.closed = 0 AND m.closed = 0 AND m.yes_token IS NOT NULL"""
    ).fetchall()
    wanted = {s.icao for s in station_registry.in_scope(scope)}
    out: dict[str, str] = {}
    for r in rows:
        if scope != "all" and r["station"] not in wanted:
            continue
        out[r["yes_token"]] = r["market_id"]
        out[r["no_token"]] = r["market_id"]
    return out


@dataclass
class Stats:
    frames: int = 0
    messages: int = 0
    by_type: dict[str, int] = field(default_factory=dict)
    checks_ok: int = 0
    checks_recovered: int = 0  # hash not seen in the feed, but a second read matched level by level
    checks_stale_snapshot: int = 0  # the exchange's snapshot was older than the feed: local book kept
    checks_mismatch: int = 0
    disconnects: int = 0


def _same_levels(book, rest: dict) -> bool:
    bids = {price_key(l["price"]): float(l["size"]) for l in rest.get("bids") or []}
    asks = {price_key(l["price"]): float(l["size"]) for l in rest.get("asks") or []}
    return bids == book.bids and asks == book.asks


class Recorder:
    def __init__(self, settings: Settings, conn: sqlite3.Connection, *, connection_factory=MarketConnection) -> None:
        self.settings = settings
        self.db = conn
        self.state = BookState()
        self.stats = Stats()
        self.raw = RollingParquetSink(settings.raw_dir / "book", "book", RAW_SCHEMA, "recv_ms")
        self.snaps = RollingParquetSink(settings.raw_dir / "snapshots", "snap", SNAP_SCHEMA, "snap_ms", flush_seconds=0)
        self._factory = connection_factory
        self._conns: dict[int, MarketConnection] = {}
        self._conn_tasks: dict[int, asyncio.Task] = {}
        self._asset_conn: dict[str, int] = {}
        self._seq = 0
        self._heartbeat_failed = False
        self._open_gap: dict[int, int] = {}  # conn id -> gaps row id
        self._last_ok_check: dict[str, int] = {}
        self._pending_checks: dict[str, tuple[str, int, int]] = {}  # asset -> (rest hash, deadline ms, looks so far)
        self._state_path = settings.state_dir / "recorder.json"
        self._stopping = False

    # ---- feed handlers (called from the connection tasks) -----------------------

    def on_frame(self, recv_ms: int, conn_id: int, text: str) -> None:
        self.stats.frames += 1
        if text == "PONG":
            return
        self._seq += 1
        self.raw.append((recv_ms, conn_id, self._seq, text))
        try:
            decoded = json.loads(text)
        except ValueError:
            return
        for msg in decoded if isinstance(decoded, list) else [decoded]:
            if not isinstance(msg, dict):
                continue
            et = self.state.apply(msg, recv_ms)
            self.stats.messages += 1
            self.stats.by_type[et or "?"] = self.stats.by_type.get(et or "?", 0) + 1
        gap_id = self._open_gap.pop(conn_id, None)
        if gap_id is not None:
            self.db.execute("UPDATE gaps SET end_ms=? WHERE id=?", (recv_ms, gap_id))

    def on_state(self, conn_id: int, state: str, ts_ms: int, detail: str) -> None:
        if state == "disconnected":
            self.stats.disconnects += 1
            log.warning("book connection %d lost: %s", conn_id, detail)
            if conn_id not in self._open_gap:
                cur = self.db.execute(
                    "INSERT INTO gaps(source, key, start_ms, reason) VALUES('book', ?, ?, ?)",
                    (f"conn:{conn_id}", ts_ms, f"disconnect: {detail}"),
                )
                self._open_gap[conn_id] = cur.lastrowid
            # Books of this connection are stale until its next snapshots arrive.
            for asset, cid in self._asset_conn.items():
                if cid == conn_id:
                    self.state.drop(asset)
        else:
            log.info("book connection %d %s (%s)", conn_id, state, detail)

    # ---- universe ----------------------------------------------------------------

    def _sync_universe(self) -> None:
        wanted = set(load_universe(self.db, self.settings.scope))
        current = set(self._asset_conn)
        for asset in current - wanted:
            cid = self._asset_conn.pop(asset)
            self._conns[cid].remove([asset])
            self.state.drop(asset)
        new = sorted(wanted - current)
        while new:
            cid = next((c for c, conn in self._conns.items() if len(conn.assets) < ASSETS_PER_CONNECTION), None)
            if cid is None:
                cid = len(self._conns) + 1
                self._conns[cid] = self._factory(cid, [], self.on_frame, self.on_state)
                self._conn_tasks[cid] = asyncio.create_task(self._conns[cid].run(), name=f"book-conn-{cid}")
            room = ASSETS_PER_CONNECTION - len(self._conns[cid].assets)
            batch, new = new[:room], new[room:]
            self._conns[cid].add(batch)
            for a in batch:
                self._asset_conn[a] = cid
        if wanted != current:
            log.info("universe: %d outcomes on %d connections", len(wanted), len(self._conns))

    # ---- periodic work -----------------------------------------------------------

    def _write_snapshots(self) -> None:
        snap_ms = now_ms()
        for book in self.state.books.values():
            bids, asks = book.levels()
            self.snaps.append(
                (snap_ms, book.asset_id, book.market, book.exch_ts_ms, book.recv_ms, book.last_hash,
                 book.n_updates, json.dumps(bids), json.dumps(asks))
            )
        self.snaps.flush()

    async def _check_books(self, client) -> None:
        """Compare a sample of books with the exchange's REST snapshots.

        A snapshot whose hash the feed has carried is a match. One it has not carried
        is looked at again after a grace period, because the snapshot may simply be
        ahead of the feed. What happens then is decided in ``_confirm_mismatches``.
        """
        now = now_ms()
        # First settle the checks left pending by the previous round.
        expired: list[tuple[str, str, int]] = []
        for asset, (rest_hash, deadline, looks) in list(self._pending_checks.items()):
            book = self.state.books.get(asset)
            if book is None:
                del self._pending_checks[asset]
            elif rest_hash in book.hashes:
                del self._pending_checks[asset]
                self._last_ok_check[asset] = now
                self.stats.checks_ok += 1
            elif now >= deadline:
                del self._pending_checks[asset]
                expired.append((asset, rest_hash, looks))
        if expired:
            await self._confirm_mismatches(client, expired, now)
        assets = [a for a in self.state.books if a not in self._pending_checks]
        if not assets:
            return
        sample = random.sample(assets, min(CHECK_SAMPLE, len(assets)))
        try:
            rest = await clob.get_books(client, sample)
        except Exception as e:  # noqa: BLE001 - a failed check is not a feed failure
            log.warning("REST book check failed: %s", e)
            return
        for b in rest:
            asset, rest_hash = b.get("asset_id"), b.get("hash")
            book = self.state.books.get(asset)
            if book is None or not rest_hash:
                continue
            if rest_hash in book.hashes:
                self._last_ok_check[asset] = now
                self.stats.checks_ok += 1
            else:
                self._pending_checks[asset] = (rest_hash, now + int(CHECK_GRACE_S * 1000), 0)

    async def _confirm_mismatches(self, client, expired: list[tuple[str, str, int]], now: int) -> None:
        """Decide about snapshots whose hash never showed up in the feed.

        Read those books again and compare price levels directly. Three outcomes:

        * the levels match (or the new hash is known): nothing was wrong;
        * the levels differ and the snapshot is not newer than the last change applied
          from the feed: the snapshot is the stale side. The exchange's snapshots lag
          one change behind when two changes land in the same instant (seen on
          2026-11-24: the snapshot showed the state between two simultaneous
          cancellations, with a timestamp before theirs, until the next change). The
          feed-built book is kept; replacing it would install the stale state;
        * the levels differ and the snapshot is newer: the feed missed something. After
          one more look, to rule out a snapshot that is merely ahead, the book is
          resynced and the interval logged as a gap.
        """
        try:
            fresh = {b.get("asset_id"): b for b in await clob.get_books(client, [a for a, _, _ in expired])}
        except Exception as e:  # noqa: BLE001
            log.warning("REST book re-check failed: %s", e)
            return  # no second reading, no verdict
        for asset, first_hash, looks in expired:
            book = self.state.books.get(asset)
            rest = fresh.get(asset)
            if book is None or rest is None:
                continue
            if rest.get("hash") in book.hashes or _same_levels(book, rest):
                self._last_ok_check[asset] = now
                self.stats.checks_ok += 1
                self.stats.checks_recovered += 1
                continue
            rest_ts = as_int(rest.get("timestamp"))
            if rest_ts is not None and book.exch_ts_ms is not None and rest_ts <= book.exch_ts_ms:
                self.stats.checks_stale_snapshot += 1
                log.info("stale exchange snapshot for %s (snapshot %d, feed %d): keeping the feed-built book",
                         asset[:16], rest_ts, book.exch_ts_ms)
                continue
            if looks < CHECK_RETRIES and rest.get("hash"):
                self._pending_checks[asset] = (rest["hash"], now + int(CHECK_GRACE_S * 1000), looks + 1)
                continue
            self._mismatch(asset, rest.get("hash") or first_hash, now)

    def _mismatch(self, asset: str, rest_hash: str, now: int) -> None:
        self.stats.checks_mismatch += 1
        start = self._last_ok_check.get(asset) or now - int(CHECK_EVERY_S * 1000)
        log.warning("book mismatch for %s: newer REST snapshot %s differs from the feed-built book; resyncing", asset[:16], rest_hash)
        self.db.execute(
            "INSERT INTO gaps(source, key, start_ms, end_ms, reason) VALUES('book', ?, ?, ?, 'hash_mismatch')",
            (f"asset:{asset}", start, now),
        )
        cid = self._asset_conn.get(asset)
        if cid is not None:
            self.state.drop(asset)
            self._conns[cid].resync([asset])

    def _heartbeat(self) -> None:
        """Note that the recorder is alive. Losing one note is harmless, so a file that
        is briefly locked (Windows: a reader or a virus scanner) or a full disk must
        not take the recorder down."""
        try:
            self._state_path.parent.mkdir(parents=True, exist_ok=True)
            tmp = self._state_path.with_suffix(".tmp")
            tmp.write_text(json.dumps({"last_alive_ms": now_ms(), "frames": self.stats.frames}), encoding="utf-8")
            os.replace(tmp, self._state_path)
        except OSError as e:
            if not self._heartbeat_failed:
                log.warning("could not write the heartbeat file: %s", e)
            self._heartbeat_failed = True
        else:
            self._heartbeat_failed = False

    def _restart_gap(self) -> None:
        """Log the outage between the previous run's last heartbeat and now."""
        try:
            last = json.loads(self._state_path.read_text(encoding="utf-8"))["last_alive_ms"]
        except (OSError, ValueError, KeyError):
            return
        self.db.execute(
            "INSERT INTO gaps(source, key, start_ms, end_ms, reason) VALUES('book', 'process', ?, ?, 'recorder restart')",
            (last, now_ms()),
        )

    # ---- main loop -----------------------------------------------------------------

    async def run(self, duration_s: float | None = None) -> Stats:
        self._restart_gap()
        loop = asyncio.get_running_loop()
        started = loop.time()
        next_snap = started + SNAPSHOT_EVERY_S
        next_check = started + CHECK_EVERY_S
        next_universe = started
        next_log = started + 300
        async with make_async_client(self.settings.user_agent) as client:
            try:
                while not self._stopping:
                    now = loop.time()
                    if now >= next_universe:
                        self._sync_universe()
                        next_universe = now + UNIVERSE_EVERY_S
                    if now >= next_snap:
                        self._write_snapshots()
                        next_snap = now + SNAPSHOT_EVERY_S
                    if now >= next_check:
                        await self._check_books(client)
                        next_check = now + CHECK_EVERY_S
                    if now >= next_log:
                        log.info(
                            "recorder: %d frames, %d books, raw %.1f MB, checks ok/recovered/stale snapshot/mismatch %d/%d/%d/%d, disconnects %d",
                            self.stats.frames, len(self.state.books), self.raw.bytes_written / 1e6,
                            self.stats.checks_ok, self.stats.checks_recovered, self.stats.checks_stale_snapshot,
                            self.stats.checks_mismatch, self.stats.disconnects,
                        )
                        next_log = now + 300
                    self.raw.maybe_flush(now_ms())
                    self._heartbeat()
                    for cid, task in self._conn_tasks.items():
                        if task.done() and not self._stopping:
                            raise RuntimeError(f"connection task {cid} ended") from task.exception()
                    if duration_s is not None and now - started >= duration_s:
                        break
                    await asyncio.sleep(1.0)
            finally:
                await self._shutdown()
        return self.stats

    def stop(self) -> None:
        self._stopping = True

    async def _shutdown(self) -> None:
        self._stopping = True
        for c in self._conns.values():
            c.stop()
        for t in self._conn_tasks.values():
            t.cancel()
        await asyncio.gather(*self._conn_tasks.values(), return_exceptions=True)
        self.raw.close()
        self._heartbeat()
