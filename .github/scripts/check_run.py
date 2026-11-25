"""Check what a short live run left behind. Usage: check_run.py <data dir> [run log].

Fails (exit 1) unless the run imported the catalog and METAR reports, recorded order
book messages into readable Parquet files, left no half-written file, and, when a
log is given, the recorder printed its closing summary (it only does when it was
asked to stop and had the time to write out what it held).
"""

from __future__ import annotations

import json
import sqlite3
import sys
from pathlib import Path

import pyarrow.parquet as pq


def main() -> int:
    data = Path(sys.argv[1] if len(sys.argv) > 1 and not sys.argv[1].startswith("-") else "data")
    log = Path(sys.argv[2]) if len(sys.argv) > 2 else None
    problems: list[str] = []
    facts: dict = {}

    conn = sqlite3.connect(data / "db" / "polyweather.sqlite")
    q = lambda sql: conn.execute(sql).fetchone()[0]  # noqa: E731
    facts["events"] = q("SELECT COUNT(*) FROM events")
    facts["specs_ok"] = q("SELECT COUNT(*) FROM resolution_specs WHERE status='ok'")
    facts["metar_reports"] = q("SELECT COUNT(*) FROM metar_obs")
    # The gap check reports "incomplete" for the day before the first start: expected.
    facts["failed_jobs"] = [r[0] for r in conn.execute("SELECT job FROM job_runs WHERE ok=0 AND job != 'check-gaps'")]
    facts["gaps"] = [tuple(r) for r in conn.execute("SELECT source, reason, COUNT(*) FROM gaps GROUP BY 1, 2")]
    facts["integrity"] = q("PRAGMA integrity_check")
    for name, minimum in (("events", 50), ("specs_ok", 50), ("metar_reports", 500)):
        if facts[name] < minimum:
            problems.append(f"{name} = {facts[name]}, expected at least {minimum}")
    if facts["integrity"] != "ok":
        problems.append(f"database integrity check: {facts['integrity']}")
    if facts["failed_jobs"]:
        problems.append(f"failed jobs: {facts['failed_jobs']}")

    for kind, minimum in (("book", 5000), ("snapshots", 1000)):
        rows = 0
        files = sorted((data / "raw" / kind).rglob("*.parquet"))
        for f in files:
            rows += pq.read_metadata(f).num_rows
            pq.read_table(f)  # every file must be readable end to end
        facts[f"{kind}_files"], facts[f"{kind}_rows"] = len(files), rows
        if rows < minimum:
            problems.append(f"{kind}: {rows} rows in {len(files)} files, expected at least {minimum}")
    leftovers = [str(p) for p in data.rglob("*.tmp")]
    if leftovers:
        problems.append(f"half-written files left behind: {leftovers[:5]}")

    if log is not None:
        summary = None
        for line in log.read_text(encoding="utf-8", errors="replace").splitlines():
            line = line.strip()
            if line.startswith('{"frames"'):
                summary = json.loads(line)
        facts["recorder_summary"] = summary
        if summary is None:
            problems.append("the recorder did not print its closing summary: it was not stopped cleanly")
        elif summary["frames"] < 5000:
            problems.append(f"recorder saw only {summary['frames']} frames")

    print(json.dumps(facts, indent=2, default=str))
    for p in problems:
        print("PROBLEM:", p)
    return 1 if problems else 0


if __name__ == "__main__":
    raise SystemExit(main())
