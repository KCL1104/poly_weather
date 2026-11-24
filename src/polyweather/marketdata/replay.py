"""Replay source: plays back what the recorder landed, in original receive order."""

from __future__ import annotations

import json
import sqlite3
from pathlib import Path
from typing import Iterator, Sequence

import pyarrow.parquet as pq

from ..recorder.book import BookState
from ..storage.parquet_sink import read_partitions
from ..util.timeutil import ms_to_iso
from .base import FeedMessage, MarketInfo


def load_markets(conn: sqlite3.Connection) -> list[MarketInfo]:
    rows = conn.execute(
        """SELECT market_id, event_id, yes_token, no_token, group_title, lo, hi, unit, fee_rate, tick_size,
                  min_order_size FROM markets WHERE yes_token IS NOT NULL"""
    ).fetchall()
    return [MarketInfo(*tuple(r)) for r in rows]


class ReplaySource:
    def __init__(self, raw_dir: Path, conn: sqlite3.Connection, start_ms: int, end_ms: int) -> None:
        self.raw_dir = raw_dir
        self.conn = conn
        self.start_ms = start_ms
        self.end_ms = end_ms

    def list_markets(self) -> Sequence[MarketInfo]:
        return load_markets(self.conn)

    def frames(self) -> Iterator[tuple[int, int, str]]:
        """(recv_ms, conn, raw text) for every recorded frame in the window, in order."""
        files = read_partitions(self.raw_dir / "book", ms_to_iso(self.start_ms)[:10], ms_to_iso(self.end_ms)[:10])
        rows: list[tuple[int, int, int, str]] = []
        for f in files:
            t = pq.read_table(f, filters=[("recv_ms", ">=", self.start_ms), ("recv_ms", "<", self.end_ms)])
            rows += zip(
                t.column("recv_ms").to_pylist(), t.column("seq").to_pylist(),
                t.column("conn").to_pylist(), t.column("msg").to_pylist(),
            )
        rows.sort(key=lambda r: (r[0], r[1]))
        for recv_ms, _seq, conn_id, msg in rows:
            yield recv_ms, conn_id, msg

    def messages(self, asset_ids: Sequence[str] | None = None) -> Iterator[FeedMessage]:
        wanted = set(asset_ids) if asset_ids is not None else None
        for recv_ms, _conn, text in self.frames():
            try:
                decoded = json.loads(text)
            except ValueError:
                continue
            for msg in decoded if isinstance(decoded, list) else [decoded]:
                if not isinstance(msg, dict):
                    continue
                if wanted is not None and not _touches(msg, wanted):
                    continue
                yield FeedMessage(recv_ms, msg)

    def book_snapshot(self, asset_id: str, at_ms: int | None = None) -> dict | None:
        """Rebuild one outcome's book by replaying up to ``at_ms`` (window end if None)."""
        limit = self.end_ms if at_ms is None else at_ms
        state = BookState()
        for fm in self.messages([asset_id]):
            if fm.recv_ms > limit:
                break
            state.apply(fm.msg, fm.recv_ms)
        book = state.books.get(asset_id)
        if book is None:
            return None
        bids, asks = book.levels()
        return {"asset_id": asset_id, "bids": bids, "asks": asks, "hash": book.last_hash, "recv_ms": book.recv_ms}

    def gaps(self) -> list[tuple[int, int | None, str, str]]:
        """Recorded feed gaps overlapping the window: (start_ms, end_ms, key, reason)."""
        rows = self.conn.execute(
            "SELECT start_ms, end_ms, key, reason FROM gaps WHERE source='book' AND start_ms < ? "
            "AND (end_ms IS NULL OR end_ms > ?) ORDER BY start_ms",
            (self.end_ms, self.start_ms),
        ).fetchall()
        return [tuple(r) for r in rows]


def _touches(msg: dict, wanted: set[str]) -> bool:
    if msg.get("asset_id") in wanted:
        return True
    for c in msg.get("price_changes") or []:
        if c.get("asset_id") in wanted:
            return True
    return any(a in wanted for a in msg.get("assets_ids") or [])
