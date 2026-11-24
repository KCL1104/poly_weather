import asyncio
import json

from polyweather.polymarket import ws as ws_mod
from polyweather.polymarket.ws import MarketConnection


class FakeSocket:
    """A scripted websocket: yields queued frames, then either idles or fails."""

    def __init__(self, frames, fail_after=True):
        self.frames = list(frames)
        self.sent: list[str] = []
        self.fail_after = fail_after

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        return False

    async def send(self, text):
        self.sent.append(text)

    async def recv(self):
        if self.frames:
            return self.frames.pop(0)
        if self.fail_after:
            raise ConnectionError("socket closed")
        await asyncio.sleep(3600)


async def test_subscribes_delivers_frames_and_reconnects(monkeypatch):
    monkeypatch.setattr(ws_mod.random, "random", lambda: 0.0)
    real_sleep = asyncio.sleep
    monkeypatch.setattr(ws_mod.asyncio, "sleep", lambda s: real_sleep(0))

    sockets = [FakeSocket(['[{"event_type":"book"}]', "PONG"]), FakeSocket(['{"event_type":"price_change"}'])]
    opened = []

    def connect():
        s = sockets[len(opened)]
        opened.append(s)
        return s

    frames, states = [], []
    conn = MarketConnection(7, ["a2", "a1"], lambda recv, cid, text: frames.append((cid, text)),
                            lambda cid, state, ts, detail: states.append((cid, state, detail)), connect=connect)

    def stop_after_second_session(cid, state, ts, detail):
        states.append((cid, state, detail))
        if [s for _, s, _ in states].count("disconnected") == 2:
            conn.stop()

    conn.on_state = stop_after_second_session
    await asyncio.wait_for(conn.run(), timeout=5)

    assert [t for _, t in frames] == ['[{"event_type":"book"}]', "PONG", '{"event_type":"price_change"}']
    assert [s for _, s, _ in states] == ["connected", "disconnected", "connected", "disconnected"]
    assert "ConnectionError" in states[1][2]
    for s in opened:
        first = json.loads(s.sent[0])
        assert first == {"assets_ids": ["a1", "a2"], "type": "market", "custom_feature_enabled": True}


async def test_heartbeat_and_incremental_subscriptions(monkeypatch):
    monkeypatch.setattr(ws_mod, "PING_EVERY_S", 0.0)
    sock = FakeSocket(["x", "y", "z"], fail_after=True)
    conn = MarketConnection(1, ["a1"], lambda *a: None, lambda *a: None, connect=lambda: sock)
    conn.add(["a1", "a2"])   # a1 already present: only a2 is new
    conn.remove(["a9"])      # unknown: nothing to do
    task = asyncio.create_task(conn.run())
    await asyncio.sleep(0.05)
    conn.add(["a3"])
    conn.resync(["a3"])
    await asyncio.sleep(0.05)
    conn.stop()
    task.cancel()
    await asyncio.gather(task, return_exceptions=True)
    assert json.loads(sock.sent[0])["assets_ids"] == ["a1", "a2"]  # queued add folded into the first subscribe
    assert "PING" in sock.sent
    assert conn.assets == {"a1", "a2", "a3"}


async def test_silence_is_treated_as_a_dead_connection(monkeypatch):
    monkeypatch.setattr(ws_mod, "SILENCE_TIMEOUT_S", 0.0)
    monkeypatch.setattr(ws_mod.random, "random", lambda: 0.0)
    states = []
    sock = FakeSocket([], fail_after=False)
    conn = MarketConnection(1, ["a1"], lambda *a: None, lambda cid, st, ts, d: (states.append((st, d)), conn.stop() if st == "disconnected" else None), connect=lambda: sock)
    await asyncio.wait_for(conn.run(), timeout=5)
    assert states[0][0] == "connected" and states[1][0] == "disconnected" and "no frames" in states[1][1]
