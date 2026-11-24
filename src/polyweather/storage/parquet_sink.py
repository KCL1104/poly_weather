"""Append-only Parquet landing for high-volume streams.

Rows are buffered in memory and written as small immutable files partitioned by UTC
date: ``<root>/date=YYYY-MM-DD/<prefix>-<first_ts_ms>-<pid>-<n>.parquet``. A file is
written under a temporary name and renamed, so a reader never sees a partial file. A
crash loses at most the unflushed buffer, and the recorder logs that as a gap.
"""

from __future__ import annotations

import datetime as dt
import os
from pathlib import Path
from typing import Sequence

import pyarrow as pa
import pyarrow.parquet as pq

from ..util.timeutil import UTC


class RollingParquetSink:
    def __init__(
        self,
        root: Path,
        prefix: str,
        schema: pa.Schema,
        ts_column: str,
        *,
        flush_rows: int = 50_000,
        flush_seconds: float = 30.0,
        compression_level: int = 9,
    ) -> None:
        self.root = root
        self.prefix = prefix
        self.schema = schema
        self.ts_index = schema.get_field_index(ts_column)
        self.flush_rows = flush_rows
        self.flush_seconds = flush_seconds
        self.compression_level = compression_level
        self._cols: list[list] = [[] for _ in schema]
        self._first_ts: int | None = None
        self._last_flush_ts: int | None = None
        self._n_files = 0
        self.rows_written = 0
        self.bytes_written = 0

    @staticmethod
    def _date(ts_ms: int) -> str:
        return dt.datetime.fromtimestamp(ts_ms / 1000, UTC).strftime("%Y-%m-%d")

    def __len__(self) -> int:
        return len(self._cols[0])

    def append(self, row: Sequence) -> None:
        ts = row[self.ts_index]
        if self._first_ts is not None and self._date(ts) != self._date(self._first_ts):
            self.flush()
        if self._first_ts is None:
            self._first_ts = ts
            if self._last_flush_ts is None:
                self._last_flush_ts = ts
        for col, value in zip(self._cols, row):
            col.append(value)
        if len(self) >= self.flush_rows:
            self.flush()

    def maybe_flush(self, now_ms: int) -> None:
        if len(self) and self._last_flush_ts is not None and now_ms - self._last_flush_ts >= self.flush_seconds * 1000:
            self.flush()
        elif not len(self):
            self._last_flush_ts = now_ms

    def flush(self) -> Path | None:
        if not len(self) or self._first_ts is None:
            return None
        directory = self.root / f"date={self._date(self._first_ts)}"
        directory.mkdir(parents=True, exist_ok=True)
        self._n_files += 1
        path = directory / f"{self.prefix}-{self._first_ts}-{os.getpid()}-{self._n_files:06d}.parquet"
        table = pa.Table.from_arrays([pa.array(c, type=f.type) for c, f in zip(self._cols, self.schema)], schema=self.schema)
        tmp = path.with_suffix(".tmp")
        pq.write_table(table, tmp, compression="zstd", compression_level=self.compression_level)
        tmp.replace(path)
        self.rows_written += len(self)
        self.bytes_written += path.stat().st_size
        self._last_flush_ts = self._cols[self.ts_index][-1]
        self._cols = [[] for _ in self.schema]
        self._first_ts = None
        return path

    def close(self) -> None:
        self.flush()


def read_partitions(root: Path, start_date: str, end_date: str) -> list[Path]:
    """Files of the date partitions in [start_date, end_date], in name order."""
    out: list[Path] = []
    if not root.exists():
        return out
    for d in sorted(p for p in root.iterdir() if p.is_dir() and p.name.startswith("date=")):
        day = d.name[5:]
        if start_date <= day <= end_date:
            out += sorted(d.glob("*.parquet"))
    return out
