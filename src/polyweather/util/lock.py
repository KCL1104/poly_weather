"""One process of a kind per data directory.

Two recorders on one data directory write every frame twice and fight over the state
file; two paper runs at once could book the same fill twice. Starting the system a
second time by accident (a second double-click, a container next to a native run)
must therefore fail loudly instead of quietly damaging the archive.

The lock is an operating-system lock on a file next to the database. It disappears
with the process that holds it, however that process ends, so a crash or a power cut
leaves nothing to clean up.
"""

from __future__ import annotations

import errno
import logging
import os
from pathlib import Path
from typing import IO

log = logging.getLogger(__name__)
_held: dict[str, IO] = {}  # kept open for the life of the process


class Busy(Exception):
    """Another process holds this lock."""


def _lock(handle: IO) -> None:
    if os.name == "nt":
        import msvcrt

        handle.seek(0)
        msvcrt.locking(handle.fileno(), msvcrt.LK_NBLCK, 1)
    else:
        import fcntl

        fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)


def hold(directory: Path, name: str) -> bool:
    """Take the lock ``name`` for this data directory until the process ends.

    Raises ``Busy`` if another process has it. Returns False (and carries on unprotected)
    on a filesystem that cannot lock at all, which some shared folders cannot.
    """
    path = Path(directory) / f"{name}.lock"
    if str(path) in _held:
        return True
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        handle = open(path, "a+")
    except OSError as e:
        log.warning("no lock file %s (%s): running without the single-instance check", path, e)
        return False
    try:
        _lock(handle)
    except OSError as e:
        handle.close()
        if e.errno in (errno.EACCES, errno.EAGAIN, errno.EWOULDBLOCK, errno.EDEADLK, errno.EDEADLOCK) or os.name == "nt":
            raise Busy(str(path)) from None
        log.warning("cannot lock %s (%s): running without the single-instance check", path, e)
        return False
    _held[str(path)] = handle
    return True


def release(directory: Path, name: str) -> None:
    """Give the lock back early (closing the file releases it). Used by tests and by short commands."""
    handle = _held.pop(str(Path(directory) / f"{name}.lock"), None)
    if handle is not None:
        handle.close()
