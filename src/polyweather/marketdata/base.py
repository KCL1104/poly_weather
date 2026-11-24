"""The one interface strategies use to see the market.

Live and replay sources yield the same ``FeedMessage`` stream, so a strategy cannot
tell which it is running against. The interface is read-only by construction: there
is no method that could send an order.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import AsyncIterator, Iterator, Protocol, Sequence


@dataclass(frozen=True)
class MarketInfo:
    market_id: str
    event_id: str
    yes_token: str
    no_token: str
    group_title: str
    lo: int | None
    hi: int | None
    unit: str | None
    fee_rate: float | None
    tick_size: float | None
    min_order_size: float | None


@dataclass(frozen=True)
class FeedMessage:
    recv_ms: int  # local receive time of the frame that carried this message
    msg: dict  # one decoded feed message (book, price_change, last_trade_price, ...)


class MarketDataSource(Protocol):
    def list_markets(self) -> Sequence[MarketInfo]:
        """Markets known to the source."""

    def messages(self, asset_ids: Sequence[str] | None = None) -> Iterator[FeedMessage] | AsyncIterator[FeedMessage]:
        """Order-book feed for the given outcomes (all when None), in receive order."""

    def book_snapshot(self, asset_id: str, at_ms: int | None = None) -> dict | None:
        """Latest full book for an outcome, as of ``at_ms`` when given."""
