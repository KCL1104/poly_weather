"""Market-channel WebSocket connection with heartbeat and automatic reconnect.

One ``MarketConnection`` holds up to a few hundred outcome ids. It sends the text
frame ``PING`` every 10 seconds as the feed requires, treats prolonged silence as a
dead connection, and reconnects with backoff. Every text frame is handed to
``on_frame`` with the local receive time; connection changes go to ``on_state`` so
the recorder can mark gaps.
"""

from __future__ import annotations

import asyncio
import json
import logging
import random
import time
from typing import Callable, Iterable

import websockets

from ..util.timeutil import now_ms

log = logging.getLogger(__name__)

URL = "wss://ws-subscriptions-clob.polymarket.com/ws/market"
PING_EVERY_S = 10.0
SILENCE_TIMEOUT_S = 35.0

FrameHandler = Callable[[int, int, str], None]  # (recv_ms, conn_id, text)
StateHandler = Callable[[int, str, int, str], None]  # (conn_id, state, ts_ms, detail)


class MarketConnection:
    def __init__(
        self,
        conn_id: int,
        assets: Iterable[str],
        on_frame: FrameHandler,
        on_state: StateHandler,
        *,
        url: str = URL,
        connect: Callable[[], object] | None = None,
    ) -> None:
        self.conn_id = conn_id
        self.assets: set[str] = set(assets)
        self.on_frame = on_frame
        self.on_state = on_state
        self.url = url
        self._connect = connect or (lambda: websockets.connect(self.url, max_size=None, ping_interval=None, open_timeout=20))
        self._ws = None
        self._pending: asyncio.Queue[tuple[str, list[str]]] = asyncio.Queue()
        self.last_frame_ms: int | None = None
        self._stop = False

    def add(self, assets: Iterable[str]) -> None:
        new = [a for a in assets if a not in self.assets]
        if new:
            self.assets.update(new)
            self._pending.put_nowait(("subscribe", new))

    def remove(self, assets: Iterable[str]) -> None:
        old = [a for a in assets if a in self.assets]
        if old:
            self.assets.difference_update(old)
            self._pending.put_nowait(("unsubscribe", old))

    def resync(self, assets: Iterable[str]) -> None:
        """Ask for fresh snapshots of assets already subscribed."""
        again = [a for a in assets if a in self.assets]
        if again:
            self._pending.put_nowait(("unsubscribe", again))
            self._pending.put_nowait(("subscribe", again))

    def stop(self) -> None:
        self._stop = True

    async def run(self) -> None:
        attempt = 0
        while not self._stop:
            if not self.assets:
                await asyncio.sleep(1.0)
                continue
            try:
                async with self._connect() as ws:
                    self._ws = ws
                    # Operations queued while disconnected are covered by the full subscribe.
                    while not self._pending.empty():
                        self._pending.get_nowait()
                    await ws.send(
                        json.dumps({"assets_ids": sorted(self.assets), "type": "market", "custom_feature_enabled": True})
                    )
                    self.on_state(self.conn_id, "connected", now_ms(), f"{len(self.assets)} assets")
                    attempt = 0
                    await self._session(ws)
            except asyncio.CancelledError:
                raise
            except Exception as e:  # noqa: BLE001 - any failure means reconnect
                detail = f"{type(e).__name__}: {e}"[:300]
            else:
                detail = "closed"
            finally:
                self._ws = None
            if self._stop:
                break
            self.on_state(self.conn_id, "disconnected", self.last_frame_ms or now_ms(), detail)
            attempt += 1
            await asyncio.sleep(min(30.0, 0.5 * 2 ** min(attempt, 6)) * (0.5 + random.random()))

    async def _session(self, ws) -> None:
        last_ping = time.monotonic()
        last_any = time.monotonic()
        while not self._stop:
            now = time.monotonic()
            if now - last_ping >= PING_EVERY_S:
                await ws.send("PING")
                last_ping = now
            while not self._pending.empty():
                op, ids = self._pending.get_nowait()
                await ws.send(json.dumps({"assets_ids": ids, "operation": op}))
            if now - last_any > SILENCE_TIMEOUT_S:
                raise TimeoutError(f"no frames for {SILENCE_TIMEOUT_S:.0f}s")
            try:
                frame = await asyncio.wait_for(ws.recv(), timeout=1.0)
            except asyncio.TimeoutError:
                continue
            last_any = time.monotonic()
            if isinstance(frame, bytes):
                frame = frame.decode("utf-8", "replace")
            recv = now_ms()
            self.last_frame_ms = recv
            self.on_frame(recv, self.conn_id, frame)
