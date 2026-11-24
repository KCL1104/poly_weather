"""Runtime settings, read from environment variables.

POLYWEATHER_DATA_DIR   where everything is stored (default: the attached Railway volume
                       when there is one, else ./data): db/ the SQLite database, raw/ the
                       Parquet archive, state/ small run-time files, backup/ daily
                       database copies
POLYWEATHER_SCOPE      "us" (default) or "all": which stations the recorder and
                       archivers cover
POLYWEATHER_USER_AGENT User-Agent sent to public APIs
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class Settings:
    data_dir: Path
    scope: str = "us"
    user_agent: str = "polyweather-research/0.1"

    @property
    def db_path(self) -> Path:
        return self.data_dir / "db" / "polyweather.sqlite"

    @property
    def raw_dir(self) -> Path:
        return self.data_dir / "raw"

    @property
    def state_dir(self) -> Path:
        return self.data_dir / "state"

    @property
    def backup_dir(self) -> Path:
        return self.data_dir / "backup"

    def ensure_dirs(self) -> None:
        for p in (self.db_path.parent, self.raw_dir, self.state_dir):
            p.mkdir(parents=True, exist_ok=True)


def load_settings() -> Settings:
    scope = os.environ.get("POLYWEATHER_SCOPE", "us").lower()
    if scope not in ("us", "all"):
        raise ValueError(f"POLYWEATHER_SCOPE must be 'us' or 'all', got {scope!r}")
    return Settings(
        data_dir=Path(
            os.environ.get("POLYWEATHER_DATA_DIR") or os.environ.get("RAILWAY_VOLUME_MOUNT_PATH") or "data"
        ).resolve(),
        scope=scope,
        user_agent=os.environ.get("POLYWEATHER_USER_AGENT", "polyweather-research/0.1"),
    )
