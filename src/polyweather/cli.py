"""Command-line entry point: ``polyweather <command>``.

Every command only reads public data and writes under the data directory. Long-running
commands (``record``, ``metar poll``) are meant to run as services; the rest are short
jobs run from timers. Each timer job logs its outcome to ``job_runs``.
"""

from __future__ import annotations

import argparse
import asyncio
import datetime as dt
import json
import logging
import signal
import sys

from . import stations as station_registry
from .settings import Settings, load_settings
from .storage import db
from .util.http import make_client
from .util.timeutil import UTC, iso, ms_to_iso, utcnow

log = logging.getLogger("polyweather")


def _setup_logging(verbose: bool) -> None:
    logging.basicConfig(
        level=logging.DEBUG if verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
        stream=sys.stderr,
    )
    logging.getLogger("httpx").setLevel(logging.WARNING)
    logging.getLogger("httpcore").setLevel(logging.WARNING)
    logging.getLogger("websockets").setLevel(logging.WARNING)


def _scope_ids(settings: Settings, us_only: bool = False) -> list[str]:
    return [s.icao for s in station_registry.in_scope(settings.scope) if s.us or not us_only]


def _job(conn, name: str, fn) -> int:
    """Run a timer job, record the outcome, return a process exit code."""
    try:
        detail = fn()
    except Exception as e:  # noqa: BLE001 - the failure itself is the result to record
        log.exception("%s failed", name)
        db.log_job(conn, name, False, f"{type(e).__name__}: {e}")
        return 1
    db.log_job(conn, name, True, json.dumps(detail, default=str) if detail is not None else "")
    if detail is not None:
        print(json.dumps(detail, default=str, ensure_ascii=False))
    return 0


# ---- commands ---------------------------------------------------------------------


def cmd_catalog_scan(args, settings, conn) -> int:
    from .catalog import scanner

    def run():
        with make_client(settings.user_agent) as client:
            since = (utcnow() - dt.timedelta(days=args.closed_days)).strftime("%Y-%m-%dT%H:%M:%SZ")
            r = scanner.scan(conn, client, closed_since=since, detect_changes=not args.history)
        return {"events": r.events_seen, "new": r.events_new, "markets": r.markets_seen, "unparsed": r.unparsed,
                "changes": len(r.changes), "frozen": sorted(set(r.frozen))}

    return _job(conn, "catalog", run)


def cmd_record(args, settings, conn) -> int:
    from .recorder.recorder import Recorder

    rec = Recorder(settings, conn)

    async def main():
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            loop.add_signal_handler(sig, rec.stop)
        return await rec.run(args.duration)

    stats = asyncio.run(main())
    print(json.dumps({"frames": stats.frames, "messages": stats.messages, "by_type": stats.by_type,
                      "checks_ok": stats.checks_ok, "checks_mismatch": stats.checks_mismatch,
                      "disconnects": stats.disconnects, "raw_bytes": rec.raw.bytes_written,
                      "snapshot_bytes": rec.snaps.bytes_written}))
    return 0


def cmd_metar(args, settings, conn) -> int:
    from .weather import awc, iem

    ids = _scope_ids(settings)
    with make_client(settings.user_agent) as client:
        if args.action == "poll":
            awc.poll_forever(conn, client, ids, stop_after_s=args.duration)
            return 0
        if args.action == "backfill-awc":
            return _job(conn, "metar-backfill-awc", lambda: {"new": awc.backfill(conn, client, ids, days=args.days)})
        start, end = dt.date.fromisoformat(args.start), dt.date.fromisoformat(args.end)
        sts = station_registry.in_scope(settings.scope)
        return _job(conn, "metar-backfill-iem", lambda: {"new": iem.backfill(conn, client, sts, start, end)})


def cmd_nbm(args, settings, conn) -> int:
    from .weather import nbm

    wanted = set(_scope_ids(settings, us_only=True))
    with make_client(settings.user_agent, timeout=120) as client:
        if args.action == "archive":
            return _job(conn, "nbm", lambda: nbm.archive_recent(conn, client, wanted))
        start, end = dt.date.fromisoformat(args.start), dt.date.fromisoformat(args.end)
        cycles = tuple(int(c) for c in args.cycles.split(","))
        return _job(conn, "nbm-backfill", lambda: {"new": nbm.backfill(conn, client, wanted, start, end, cycles=cycles)})


def cmd_lamp(args, settings, conn) -> int:
    from .weather import lamp

    wanted = set(_scope_ids(settings, us_only=True))
    with make_client(settings.user_agent, timeout=120) as client:
        return _job(conn, "lamp", lambda: {"new": lamp.archive_recent(conn, client, wanted)})


def cmd_ensemble(args, settings, conn) -> int:
    from .weather import ensemble

    sts = [s for s in station_registry.in_scope(settings.scope) if s.us or settings.scope == "all"]

    def run():
        with make_client(settings.user_agent, timeout=180) as client:
            if args.source == "openmeteo":
                from .weather.openmeteo import OpenMeteoSource

                source = OpenMeteoSource(client, models=tuple(args.models.split(",")))
            else:
                from .weather.ecmwf_open import EcmwfOpenSource

                source = EcmwfOpenSource(client, max_step=args.max_step)
            samples = source.fetch(sts)
            written = ensemble.store_samples(conn, settings.data_dir, source.name, samples)
        return {"source": source.name, "samples": len(samples), "new_files": written}

    return _job(conn, f"ensemble-{args.source}", run)


def cmd_prices(args, settings, conn) -> int:
    from .polymarket import prices

    def run():
        cutoff = (utcnow() - dt.timedelta(days=args.closed_days)).strftime("%Y-%m-%d")
        wanted = set(_scope_ids(settings))
        rows = conn.execute(
            """SELECT e.event_id, s.station FROM events e LEFT JOIN resolution_specs s ON s.event_id = e.event_id
               WHERE e.closed = 0 OR e.event_date >= ?""",
            (cutoff,),
        ).fetchall()
        ids = [r["event_id"] for r in rows if settings.scope == "all" or r["station"] in wanted]
        with make_client(settings.user_agent) as client:
            n, path = prices.archive_events(conn, client, settings.data_dir, ids, bucket_seconds=args.bucket, label=f"{args.bucket}s")
        return {"events": len(ids), "tokens": n, "file": str(path) if path else None}

    return _job(conn, "prices", run)


def cmd_check(args, settings, conn) -> int:
    from .ops import gapcheck, settlement_check

    if args.action == "settlement":
        def run():
            since = args.since or (utcnow() - dt.timedelta(days=3)).strftime("%Y-%m-%d")
            s = settlement_check.run(conn, since=since, until=args.until, freeze_on_mismatch=not args.no_freeze,
                                     stations=_scope_ids(settings))
            return {"since": since, "checked": s.checked, "matched": s.matched, "mismatched": s.mismatched,
                    "no_data": s.no_data, "no_winner": s.no_winner, "match_rate": s.rate}

        return _job(conn, "check-settlement", run)
    if args.action == "gaps":
        day = dt.date.fromisoformat(args.date) if args.date else (utcnow() - dt.timedelta(days=1)).date()
        checks = gapcheck.run(conn, settings, day)
        out = {"date": day.isoformat(), "ok": all(c.ok for c in checks), "checks": {c.name: {"ok": c.ok, **c.detail} for c in checks}}
        db.log_job(conn, "check-gaps", out["ok"], json.dumps(out))
        print(json.dumps(out, indent=2, ensure_ascii=False))
        return 0 if out["ok"] else 2
    until = dt.date.fromisoformat(args.date) if args.date else (utcnow() - dt.timedelta(days=1)).date()
    n = gapcheck.clean_streak(conn, until)
    print(json.dumps({"until": until.isoformat(), "clean_days": n, "gate_7_days": n >= 7}))
    return 0


def cmd_station(args, settings, conn) -> int:
    from .catalog import scanner

    if args.action == "thaw":
        last = conn.execute(
            "SELECT is_match, checked_ms FROM settlement_checks WHERE station=? AND is_match IS NOT NULL ORDER BY obs_date DESC LIMIT 1",
            (args.icao,),
        ).fetchone()
        since = conn.execute("SELECT since_ms FROM station_status WHERE station=?", (args.icao,)).fetchone()
        passed = bool(last and last["is_match"] and since and last["checked_ms"] >= since["since_ms"])
        if not passed and not args.force:
            print("refused: no matching settlement check since the freeze (re-confirm the rules, run "
                  "`polyweather check settlement`, or pass --force with a note)", file=sys.stderr)
            return 2
        scanner.thaw_station(conn, args.icao, args.note)
        print(f"{args.icao} thawed")
        return 0
    rows = conn.execute("SELECT * FROM station_status ORDER BY station").fetchall()
    for r in rows:
        print(f"{r['station']}  {r['state']:7}  since {ms_to_iso(r['since_ms'])}  {r['reason'] or ''}")
    if not rows:
        print("no station has ever been frozen")
    return 0


def cmd_status(args, settings, conn) -> int:
    q = lambda sql, *a: conn.execute(sql, a).fetchone()[0]  # noqa: E731
    out = {
        "data_dir": str(settings.data_dir),
        "scope": settings.scope,
        "events_open": q("SELECT COUNT(*) FROM events WHERE closed=0"),
        "events_closed": q("SELECT COUNT(*) FROM events WHERE closed=1"),
        "specs_ok": q("SELECT COUNT(*) FROM resolution_specs WHERE status='ok'"),
        "specs_unparsed": q("SELECT COUNT(*) FROM resolution_specs WHERE status!='ok'"),
        "metar_reports": q("SELECT COUNT(*) FROM metar_obs"),
        "nbm_cycles": q("SELECT COUNT(DISTINCT cycle) FROM guidance_blocks WHERE product='nbp'"),
        "lamp_cycles": q("SELECT COUNT(DISTINCT cycle) FROM guidance_blocks WHERE product='lav'"),
        "ensemble_files": q("SELECT COUNT(*) FROM forecast_fetches"),
        "open_book_gaps": q("SELECT COUNT(*) FROM gaps WHERE source='book' AND end_ms IS NULL"),
        "frozen_stations": [r[0] for r in conn.execute("SELECT station FROM station_status WHERE state='frozen'")],
        "settlement": dict(conn.execute("SELECT COALESCE(SUM(is_match=1),0) AS matched, COALESCE(SUM(is_match=0),0) AS mismatched FROM settlement_checks").fetchone()),
        "last_jobs": {r["job"]: {"at": ms_to_iso(r["ts_ms"]), "ok": bool(r["ok"])} for r in conn.execute(
            "SELECT job, ts_ms, ok FROM job_runs WHERE id IN (SELECT MAX(id) FROM job_runs GROUP BY job)")},
    }
    print(json.dumps(out, indent=2, ensure_ascii=False))
    return 0


# ---- parser -------------------------------------------------------------------------


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(prog="polyweather", description=__doc__.splitlines()[0])
    p.add_argument("-v", "--verbose", action="store_true")
    sub = p.add_subparsers(dest="command", required=True)

    c = sub.add_parser("catalog", help="scan Gamma for weather events and parse their rules")
    c.add_argument("action", choices=["scan"])
    c.add_argument("--closed-days", type=float, default=3, help="also refresh events closed in the last N days")
    c.add_argument("--history", action="store_true", help="import mode: do not freeze stations on past rule changes")
    c.set_defaults(fn=cmd_catalog_scan)

    c = sub.add_parser("record", help="record the order-book feed (long-running)")
    c.add_argument("--duration", type=float, default=None, help="stop after N seconds (for testing)")
    c.set_defaults(fn=cmd_record)

    c = sub.add_parser("metar", help="archive METAR reports")
    c.add_argument("action", choices=["poll", "backfill-awc", "backfill-iem"])
    c.add_argument("--duration", type=float, default=None)
    c.add_argument("--days", type=float, default=31)
    c.add_argument("--start")
    c.add_argument("--end")
    c.set_defaults(fn=cmd_metar)

    c = sub.add_parser("nbm", help="archive NBM station bulletins")
    c.add_argument("action", choices=["archive", "backfill"])
    c.add_argument("--start")
    c.add_argument("--end")
    c.add_argument("--cycles", default="1,7,13,19")
    c.set_defaults(fn=cmd_nbm)

    c = sub.add_parser("lamp", help="archive LAMP station bulletins")
    c.add_argument("action", choices=["archive"])
    c.set_defaults(fn=cmd_lamp)

    c = sub.add_parser("ensemble", help="archive ensemble forecasts")
    c.add_argument("action", choices=["archive"])
    c.add_argument("--source", choices=["openmeteo", "ecmwf_open"], default="openmeteo")
    c.add_argument("--models", default="ecmwf_ifs025")
    c.add_argument("--max-step", type=int, default=72)
    c.set_defaults(fn=cmd_ensemble)

    c = sub.add_parser("prices", help="archive exchange price history (backstop for recorder gaps)")
    c.add_argument("action", choices=["archive"])
    c.add_argument("--closed-days", type=float, default=2)
    c.add_argument("--bucket", type=int, default=60)
    c.set_defaults(fn=cmd_prices)

    c = sub.add_parser("check", help="daily checks")
    c.add_argument("action", choices=["settlement", "gaps", "streak"])
    c.add_argument("--since")
    c.add_argument("--until")
    c.add_argument("--date")
    c.add_argument("--no-freeze", action="store_true", help="report mismatches without freezing stations")
    c.set_defaults(fn=cmd_check)

    c = sub.add_parser("station", help="show or lift station freezes")
    c.add_argument("action", choices=["status", "thaw"])
    c.add_argument("icao", nargs="?")
    c.add_argument("--note", default="")
    c.add_argument("--force", action="store_true")
    c.set_defaults(fn=cmd_station)

    c = sub.add_parser("status", help="one-screen summary of the archive")
    c.set_defaults(fn=cmd_status)
    return p


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    _setup_logging(args.verbose)
    settings = load_settings()
    settings.ensure_dirs()
    if args.command == "station" and args.action == "thaw" and (not args.icao or not args.note):
        print("station thaw needs an ICAO code and --note explaining why", file=sys.stderr)
        return 2
    if args.command in ("metar", "nbm") and args.action in ("backfill-iem", "backfill") and not (args.start and args.end):
        print("--start and --end (YYYY-MM-DD) are required", file=sys.stderr)
        return 2
    conn = db.connect(settings.db_path)
    try:
        return args.fn(args, settings, conn)
    finally:
        conn.close()


if __name__ == "__main__":
    raise SystemExit(main())
