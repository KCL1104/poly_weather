"""Order-book state rebuilt from the market WebSocket feed.

``book`` events replace a book; ``price_change`` events set the resting size at one
price level (0 removes it). Each change carries the hash of the book it produces, the
same hash the REST snapshot reports, which is what the recorder's consistency check
compares. Prices are held as integer ten-thousandths so levels compare exactly.
"""

from __future__ import annotations

from collections import OrderedDict
from dataclasses import dataclass, field

PRICE_SCALE = 10_000
HASH_HISTORY = 5_000


def price_key(price: str | float) -> int:
    return round(float(price) * PRICE_SCALE)


@dataclass
class Book:
    asset_id: str
    market: str | None = None
    bids: dict[int, float] = field(default_factory=dict)
    asks: dict[int, float] = field(default_factory=dict)
    last_hash: str | None = None
    exch_ts_ms: int | None = None
    recv_ms: int | None = None
    n_updates: int = 0
    hashes: OrderedDict = field(default_factory=OrderedDict)  # hash -> recv_ms, bounded

    def _note_hash(self, h: str | None, recv_ms: int) -> None:
        if not h:
            return
        self.last_hash = h
        self.hashes[h] = recv_ms
        self.hashes.move_to_end(h)
        while len(self.hashes) > HASH_HISTORY:
            self.hashes.popitem(last=False)

    def apply_snapshot(self, msg: dict, recv_ms: int) -> None:
        self.market = msg.get("market") or self.market
        self.bids = {price_key(l["price"]): float(l["size"]) for l in msg.get("bids") or []}
        self.asks = {price_key(l["price"]): float(l["size"]) for l in msg.get("asks") or []}
        self.exch_ts_ms = _int(msg.get("timestamp"))
        self.recv_ms = recv_ms
        self.n_updates += 1
        self._note_hash(msg.get("hash"), recv_ms)

    def apply_change(self, change: dict, exch_ts_ms: int | None, recv_ms: int) -> None:
        side = self.bids if change.get("side") == "BUY" else self.asks
        key = price_key(change["price"])
        size = float(change["size"])
        if size <= 0:
            side.pop(key, None)
        else:
            side[key] = size
        self.exch_ts_ms = exch_ts_ms
        self.recv_ms = recv_ms
        self.n_updates += 1
        self._note_hash(change.get("hash"), recv_ms)

    @property
    def best_bid(self) -> float | None:
        return max(self.bids) / PRICE_SCALE if self.bids else None

    @property
    def best_ask(self) -> float | None:
        return min(self.asks) / PRICE_SCALE if self.asks else None

    def levels(self) -> tuple[list[tuple[float, float]], list[tuple[float, float]]]:
        """(bids best-first, asks best-first) as (price, size) pairs."""
        bids = [(k / PRICE_SCALE, v) for k, v in sorted(self.bids.items(), reverse=True)]
        asks = [(k / PRICE_SCALE, v) for k, v in sorted(self.asks.items())]
        return bids, asks


def _int(x) -> int | None:
    try:
        return int(x)
    except (TypeError, ValueError):
        return None


class BookState:
    """All books of one recorder, updated message by message."""

    def __init__(self) -> None:
        self.books: dict[str, Book] = {}

    def apply(self, msg: dict, recv_ms: int) -> str | None:
        """Apply one decoded feed message; returns its event type."""
        et = msg.get("event_type")
        if et == "book":
            asset = msg.get("asset_id")
            if asset:
                self.books.setdefault(asset, Book(asset)).apply_snapshot(msg, recv_ms)
        elif et == "price_change":
            ts = _int(msg.get("timestamp"))
            for change in msg.get("price_changes") or []:
                book = self.books.get(change.get("asset_id"))
                if book is not None:  # no snapshot yet: the change cannot be placed
                    book.apply_change(change, ts, recv_ms)
        return et

    def drop(self, asset_id: str) -> None:
        self.books.pop(asset_id, None)
