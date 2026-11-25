"""File moves that survive Windows.

On Windows a file cannot be renamed or replaced while another program has it open,
and virus scanners open every freshly written file for a moment. The move then fails
with PermissionError although nothing is wrong. Trying again shortly works.
"""

from __future__ import annotations

import os
import time
from pathlib import Path


def move_into_place(src: Path | str, dst: Path | str, *, attempts: int = 10, wait_s: float = 0.2) -> None:
    """``os.replace(src, dst)``, tried again when the file is briefly held by someone else."""
    for attempt in range(attempts):
        try:
            os.replace(src, dst)
            return
        except PermissionError:
            if attempt == attempts - 1:
                raise
            time.sleep(wait_s)
