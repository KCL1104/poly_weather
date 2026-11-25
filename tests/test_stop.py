import asyncio
import signal
import subprocess
import sys
import textwrap

import pytest

from polyweather.util import stop


def test_stop_signals_cover_ctrl_c_and_terminate():
    assert signal.SIGINT in stop.stop_signals() and signal.SIGTERM in stop.stop_signals()


class _LoopWithoutSignals(asyncio.SelectorEventLoop):
    """Stands in for the Windows event loop, which cannot register signal handlers."""

    def add_signal_handler(self, sig, callback, *args):
        raise NotImplementedError


@pytest.mark.parametrize("loop_factory", [asyncio.SelectorEventLoop, _LoopWithoutSignals])
def test_on_stop_reaches_the_callback_inside_the_loop(loop_factory):
    previous = {sig: signal.getsignal(sig) for sig in stop.stop_signals()}
    loop = loop_factory()
    try:
        async def main():
            stopped = asyncio.Event()
            stop.on_stop(stopped.set, asyncio.get_running_loop())
            signal.raise_signal(signal.SIGTERM)  # os.kill would end the process outright on Windows
            await asyncio.wait_for(stopped.wait(), timeout=5)
            return True

        assert loop.run_until_complete(main())
    finally:
        loop.close()
        for sig, handler in previous.items():
            signal.signal(sig, handler)


def test_ask_to_stop_lets_a_child_finish_cleanly(tmp_path):
    marker = tmp_path / "flushed"
    child = tmp_path / "child.py"
    child.write_text(textwrap.dedent(f"""
        import pathlib, time
        from polyweather.util.stop import on_stop
        stopping = []
        on_stop(lambda: stopping.append(1))
        print("ready", flush=True)
        while not stopping:
            time.sleep(0.05)
        pathlib.Path({str(marker)!r}).write_text("yes")
    """), encoding="utf-8")
    p = subprocess.Popen([sys.executable, str(child)], stdout=subprocess.PIPE, **stop.spawn_options())
    assert p.stdout.readline().strip() == b"ready"
    stop.ask_to_stop(p)
    assert p.wait(timeout=10) == 0 and marker.read_text() == "yes"


def test_windows_children_get_their_own_group_and_a_ctrl_break():
    assert stop.spawn_options(windows=True)["creationflags"] == 0x00000200  # CREATE_NEW_PROCESS_GROUP
    assert stop.spawn_options(windows=False) == {}

    class Child:
        def __init__(self, console=True):
            self.console, self.sent, self.terminated = console, [], False

        def send_signal(self, sig):
            if not self.console:
                raise OSError("no console")
            self.sent.append(sig)

        def terminate(self):
            self.terminated = True

    with_console, without = Child(), Child(console=False)
    stop.ask_to_stop(with_console, windows=True)
    stop.ask_to_stop(without, windows=True)
    assert with_console.sent == [getattr(signal, "CTRL_BREAK_EVENT", 1)] and not with_console.terminated
    assert without.terminated  # nothing polite is possible without a console
