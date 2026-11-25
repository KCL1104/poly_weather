"""Ensemble forecast archive behind a replaceable source interface.

Two implementations exist: Open-Meteo's Ensemble API (small requests, non-commercial
free tier, members retrievable for only ~3 days) and a direct reader of ECMWF's open
data (CC-BY-4.0, any use, but whole global fields must be downloaded). Everything
downstream sees only ``EnsembleSample`` rows, so swapping the source changes nothing
else.
"""

from __future__ import annotations

import datetime as dt
import hashlib
import sqlite3
from dataclasses import dataclass
from pathlib import Path
from typing import Protocol, Sequence

import pyarrow as pa
import pyarrow.parquet as pq

from ..stations import Station
from ..util.files import move_into_place
from ..util.timeutil import UTC, now_ms


@dataclass(frozen=True)
class EnsembleSample:
    station: str
    model: str  # e.g. 'ecmwf_ifs025'
    run: dt.datetime | None  # model initialisation time (UTC) when the source states it
    valid: dt.datetime  # UTC
    member: int  # 0 = control run
    variable: str  # 't2m' (instantaneous) or 'mx2t3' / 'mn2t3' (3-hour extremes)
    value_c: float  # degrees Celsius


class ForecastSource(Protocol):
    """Anything that can return ensemble samples for a set of stations."""

    name: str

    def fetch(self, stations: Sequence[Station]) -> list[EnsembleSample]: ...


_SCHEMA = pa.schema(
    [
        ("station", pa.string()),
        ("model", pa.string()),
        ("run", pa.timestamp("s", tz="UTC")),
        ("valid", pa.timestamp("s", tz="UTC")),
        ("member", pa.int16()),
        ("variable", pa.string()),
        ("value_c", pa.float32()),
        ("fetched_ms", pa.int64()),
    ]
)


def store_samples(
    conn: sqlite3.Connection,
    data_dir: Path,
    source: str,
    samples: Sequence[EnsembleSample],
    fetched_ms: int | None = None,
) -> dict[str, str]:
    """Write samples to Parquet, one file per model, skipping content already stored.

    Returns {model: relative path} for the files written.
    """
    fetched_ms = fetched_ms or now_ms()
    written: dict[str, str] = {}
    by_model: dict[str, list[EnsembleSample]] = {}
    for s in samples:
        by_model.setdefault(s.model, []).append(s)
    for model, rows in by_model.items():
        rows.sort(key=lambda r: (r.station, r.variable, r.valid, r.member))
        h = hashlib.sha256()
        for r in rows:
            h.update(f"{r.station}|{r.variable}|{r.valid.timestamp():.0f}|{r.member}|{r.value_c:.2f}\n".encode())
        digest = h.hexdigest()
        if conn.execute(
            "SELECT 1 FROM forecast_fetches WHERE source=? AND model=? AND content_hash=?", (source, model, digest)
        ).fetchone():
            continue
        runs = {r.run for r in rows if r.run is not None}
        run_init = max(runs) if runs else None
        day = dt.datetime.fromtimestamp(fetched_ms / 1000, UTC).strftime("%Y-%m-%d")
        rel = Path("raw") / "ens" / f"source={source}" / f"date={day}" / f"{model}-{fetched_ms}.parquet"
        path = data_dir / rel
        path.parent.mkdir(parents=True, exist_ok=True)
        table = pa.Table.from_pydict(
            {
                "station": [r.station for r in rows],
                "model": [r.model for r in rows],
                "run": [r.run for r in rows],
                "valid": [r.valid for r in rows],
                "member": [r.member for r in rows],
                "variable": [r.variable for r in rows],
                "value_c": [r.value_c for r in rows],
                "fetched_ms": [fetched_ms] * len(rows),
            },
            schema=_SCHEMA,
        )
        tmp = path.with_suffix(".tmp")
        pq.write_table(table, tmp, compression="zstd")
        move_into_place(tmp, path)
        conn.execute(
            """INSERT INTO forecast_fetches(source, model, run_init, fetched_ms, content_hash, path, n_stations)
               VALUES(?,?,?,?,?,?,?)""",
            (source, model, run_init.strftime("%Y-%m-%dT%H:%M:%SZ") if run_init else None, fetched_ms, digest,
             str(rel), len({r.station for r in rows})),
        )
        written[model] = str(rel)
    return written
