"""Stop requests for long-running commands, on POSIX and on Windows.

POSIX sends SIGTERM (service managers, ``docker stop``) or SIGINT (Ctrl+C). Windows
has neither for another process: the only polite request one process can send another
is Ctrl+Break, and only to a process started in its own process group.
"""

from __future__ import annotations

import asyncio
import os
import signal
import subprocess
from typing import Callable

WINDOWS = os.name == "nt"


def stop_signals() -> list[int]:
    """SIGINT and SIGTERM, plus SIGBREAK where it exists (Windows)."""
    return [getattr(signal, name) for name in ("SIGINT", "SIGTERM", "SIGBREAK") if hasattr(signal, name)]


def on_stop(callback: Callable[[], None], loop: asyncio.AbstractEventLoop | None = None) -> None:
    """Call ``callback()`` when the process is asked to stop.

    With an event loop the callback runs inside the loop. Windows event loops cannot
    register signal handlers, so there a plain handler hands the call to the loop.
    """
    for sig in stop_signals():
        if loop is None:
            signal.signal(sig, lambda *_: callback())
            continue
        try:
            loop.add_signal_handler(sig, callback)
        except (NotImplementedError, RuntimeError, ValueError):
            signal.signal(sig, lambda *_, _loop=loop: _loop.call_soon_threadsafe(callback))


def spawn_options(windows: bool = WINDOWS) -> dict:
    """Extra ``Popen`` arguments so that ``ask_to_stop`` can reach the child."""
    if windows:
        return {"creationflags": getattr(subprocess, "CREATE_NEW_PROCESS_GROUP", 0x00000200)}
    return {}


def ask_to_stop(process: subprocess.Popen, windows: bool = WINDOWS) -> None:
    """Ask a child started with ``spawn_options`` to finish what it is doing and exit."""
    if not windows:
        process.send_signal(signal.SIGTERM)
        return
    try:
        process.send_signal(getattr(signal, "CTRL_BREAK_EVENT", 1))
    except (OSError, ValueError):
        # No console to deliver Ctrl+Break through (started without a window): the
        # only thing left is to end the process, as a power cut would.
        process.terminate()
