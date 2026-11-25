"""The order book of any outcome at any recorded moment.

The recorder writes every feed frame and, once a minute, a full snapshot of every
book. A book at time ``t`` is the last snapshot before ``t`` plus the frames received
since then, the same rebuild the recorder itself does (checked: replaying the frames
reproduces every snapshot).

No book is given out across a hole in the recording. If the recorder was down, a
connection was lost, or one book was resynchronised between the snapshot and ``t``,
the answer is ``None``: the paper engine may not trade on a book it cannot vouch for.
"""

from __future__ import annotations

import bisect
import json
import sqlite3
from collections import OrderedDict
from pathlib import Path

import pyarrow.parquet as pq

from ..recorder.book import PRICE_SCALE, Book, as_int, price_key
from ..settings import Settings

MAX_SNAPSHOT_AGE_MS = 150_000  # snapshots come every 60 s; older than this means the recorder was not running
_FILE_CACHE = 6


def _stamp(path: Path) -> int:
    """First timestamp of an archive file, from its name: <prefix>-<first ms>-<pid>-<n>.parquet."""
    return int(path.name.split("-")[1])


class BookHistory:
    def __init__(self, settings: Settings, conn: sqlite3.Connection) -> None:
        self.settings = settings
        self.conn = conn
        self._snaps: list[tuple[int, Path]] = []
        self._raw: list[tuple[int, Path]] = []
        self._frames: OrderedDict[Path, list[tuple[int, int, str]]] = OrderedDict()
        self._snap_rows: OrderedDict[Path, dict[str, dict]] = OrderedDict()
        self.refresh()

    def refresh(self) -> None:
        """Look at the archive again (new files appear while the recorder runs)."""
        self._snaps = sorted((_stamp(p), p) for p in (self.settings.raw_dir / "snapshots").rglob("snap-*.parquet"))
        self._raw = sorted((_stamp(p), p) for p in (self.settings.raw_dir / "book").rglob("book-*.parquet"))

    # ---- pieces ------------------------------------------------------------------------

    def _snapshot_before(self, t_ms: int) -> tuple[int, Path] | None:
        i = bisect.bisect_right(self._snaps, (t_ms, Path("￿")))
        return self._snaps[i - 1] if i else None

    def _snapshot_rows(self, path: Path) -> dict[str, dict]:
        rows = self._snap_rows.get(path)
        if rows is None:
            rows = {r["asset_id"]: r for r in pq.read_table(path).to_pylist()}
            self._snap_rows[path] = rows
            while len(self._snap_rows) > 2:
                self._snap_rows.popitem(last=False)
        else:
            self._snap_rows.move_to_end(path)
        return rows

    def _file_frames(self, path: Path) -> list[tuple[int, int, str]]:
        frames = self._frames.get(path)
        if frames is None:
            t = pq.read_table(path, columns=["recv_ms", "seq", "msg"])
            frames = sorted(zip(t.column("recv_ms").to_pylist(), t.column("seq").to_pylist(), t.column("msg").to_pylist()))
            self._frames[path] = frames
            while len(self._frames) > _FILE_CACHE:
                self._frames.popitem(last=False)
        else:
            self._frames.move_to_end(path)
        return frames

    def _frames_between(self, after_ms: int, until_ms: int) -> list[tuple[int, int, str]]:
        """Frames with after_ms < recv_ms <= until_ms, in receive order."""
        out: list[tuple[int, int, str]] = []
        for i, (first, path) in enumerate(self._raw):
            if first > until_ms:
                break
            nxt = self._raw[i + 1][0] if i + 1 < len(self._raw) else None
            # a file holds frames from its first stamp to (at most) the next file's first stamp;
            # files of different recorder runs can overlap in time only across a restart gap
            if nxt is not None and nxt <= after_ms and (nxt - first) < 600_000:
                continue
            frames = self._file_frames(path)
            lo = bisect.bisect_right(frames, (after_ms, float("inf"), ""))
            hi = bisect.bisect_right(frames, (until_ms, float("inf"), ""))
            out += frames[lo:hi]
        out.sort()
        return out

    def _gap_between(self, start_ms: int, end_ms: int, asset: str) -> str | None:
        """Reason why the recording of ``asset`` is not continuous over [start, end], if it is not."""
        row = self.conn.execute(
            """SELECT key, reason FROM gaps WHERE source='book' AND start_ms <= ? AND (end_ms IS NULL OR end_ms >= ?)
               AND (key = 'process' OR key LIKE 'conn:%' OR key = ?) ORDER BY start_ms LIMIT 1""",
            (end_ms, start_ms, f"asset:{asset}"),
        ).fetchone()
        return f"{row[0]}: {row[1]}" if row else None

    # ---- the question ------------------------------------------------------------------

    def at(self, t_ms: int, assets: list[str] | tuple[str, ...]) -> dict[str, Book | None]:
        """Books of ``assets`` as they stood at ``t_ms``; ``None`` where no trustworthy book exists."""
        out: dict[str, Book | None] = {a: None for a in assets}
        self.why: dict[str, str] = {}
        snap = self._snapshot_before(t_ms)
        if snap is None or t_ms - snap[0] > MAX_SNAPSHOT_AGE_MS:
            for a in assets:
                self.why[a] = "no snapshot shortly before this time: the recorder was not running"
            return out
        snap_ms, snap_path = snap
        rows = self._snapshot_rows(snap_path)
        books: dict[str, Book] = {}
        for a in assets:
            gap = self._gap_between(snap_ms, t_ms, a)
            if gap:
                self.why[a] = f"recording gap ({gap})"
                continue
            r = rows.get(a)
            if r is None:
                self.why[a] = "outcome not in the snapshot (not subscribed yet)"
                continue
            b = Book(a, r["market"])
            b.bids = {price_key(p): float(s) for p, s in json.loads(r["bids"])}
            b.asks = {price_key(p): float(s) for p, s in json.loads(r["asks"])}
            b.last_hash, b.exch_ts_ms, b.recv_ms, b.n_updates = r["hash"], r["exch_ts_ms"], r["recv_ms"], r["n_updates"]
            books[a] = b
        if not books:
            return out
        wanted = tuple(books) if len(books) <= 48 else None  # few books: skip frames that do not mention them before parsing
        for recv_ms, _seq, text in self._frames_between(snap_ms, t_ms):
            if '"book"' not in text and '"price_change"' not in text:
                continue
            if wanted is not None and not any(a in text for a in wanted):
                continue
            try:
                decoded = json.loads(text)
            except ValueError:
                continue
            for msg in decoded if isinstance(decoded, list) else [decoded]:
                if not isinstance(msg, dict):
                    continue
                kind = msg.get("event_type")
                if kind == "book":
                    b = books.get(msg.get("asset_id"))
                    if b is not None:
                        b.apply_snapshot(msg, recv_ms)
                elif kind == "price_change":
                    ts = as_int(msg.get("timestamp"))
                    for change in msg.get("price_changes") or []:
                        b = books.get(change.get("asset_id"))
                        if b is not None:
                            b.apply_change(change, ts, recv_ms)
        out.update(books)
        return out


def asks_of(book: Book) -> list[tuple[float, float]]:
    """Resting sell orders, cheapest first, as (price, size)."""
    return [(k / PRICE_SCALE, v) for k, v in sorted(book.asks.items())]


def bids_of(book: Book) -> list[tuple[float, float]]:
    """Resting buy orders, highest first, as (price, size)."""
    return [(k / PRICE_SCALE, v) for k, v in sorted(book.bids.items(), reverse=True)]
