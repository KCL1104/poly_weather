"""What the paper ledgers have done so far, in one place.

For each ledger: signals and why most were not entered, money put in, fees, what has
settled and how, return per USD invested, and the same without the three most
profitable city-days (success criterion 4). For H2a also the measurement the spec
asks for: cost and buyable amount of ruled-out brackets at fixed delays.
"""

from __future__ import annotations

import json
import sqlite3
import statistics

from . import engine


def _median(values: list[float]) -> float | None:
    return round(statistics.median(values), 4) if values else None


def build(conn: sqlite3.Connection, run: str = engine.LIVE) -> dict:
    out: dict = {"run": run, "ledgers": {}}
    row = conn.execute("SELECT value FROM kv WHERE key='paper_cursor_ms'").fetchone()
    out["processed_through"] = None if row is None else __import__("polyweather.paper.ledger", fromlist=["utc_iso"]).utc_iso(int(row[0]))
    for name, rules in engine.LEDGERS.items():
        sig = conn.execute(
            "SELECT decision, COALESCE(skip_reason, ''), COUNT(*) FROM paper_signals WHERE run=? AND ledger=? GROUP BY 1, 2", (run, name)
        ).fetchall()
        skips = {r[1]: r[2] for r in sig if r[0] == "skip"}
        entry: dict = {
            "model_version": rules.model_version,
            "signals": sum(r[2] for r in sig),
            "entered": sum(r[2] for r in sig if r[0] == "enter"),
            "skipped": dict(sorted(skips.items(), key=lambda kv: -kv[1])),
            "dates": [r[0] for r in conn.execute("SELECT DISTINCT obs_date FROM paper_signals WHERE run=? AND ledger=? ORDER BY 1", (run, name))],
            "paused": __import__("polyweather.paper.ledger", fromlist=["paused_reason"]).paused_reason(conn, 2**62, "-", name),
        }
        for variant in ("conservative", "optimistic"):
            fills_ = conn.execute(
                "SELECT event_id, notional, fee, payout FROM paper_fills WHERE run=? AND ledger=? AND variant=?", (run, name, variant)
            ).fetchall()
            invested = sum(f["notional"] + f["fee"] for f in fills_)
            settled = [f for f in fills_ if f["payout"] is not None]
            settled_in = sum(f["notional"] + f["fee"] for f in settled)
            pnl = sum(f["payout"] - f["notional"] - f["fee"] for f in settled)
            by_city_day: dict[str, float] = {}
            for f in settled:
                by_city_day[f["event_id"]] = by_city_day.get(f["event_id"], 0.0) + f["payout"] - f["notional"] - f["fee"]
            top3 = sum(sorted(by_city_day.values(), reverse=True)[:3])
            entry[variant] = {
                "fills": len(fills_),
                "invested_usd": round(invested, 2),
                "fees_usd": round(sum(f["fee"] for f in fills_), 4),
                "open_usd": round(invested - settled_in, 2),
                "settled_fills": len(settled),
                "settled_pnl_usd": round(pnl, 4),
                "return_per_usd": round(pnl / settled_in, 4) if settled_in else None,
                "pnl_without_top3_city_days_usd": round(pnl - top3, 4) if settled else None,
                "city_days_settled": len(by_city_day),
                "equity_usd": round(rules.bankroll_usd + pnl, 2),
            }
        out["ledgers"][name] = entry

    # A day's first report rules out every bracket far below the night's temperature at once; those
    # were never in play. The hypothesis is about brackets that fall during the day, so they are shown apart.
    measures = {}
    for label, clause in (("during_the_day", "p.trigger > d.first"), ("by_the_first_report_of_the_day", "p.trigger = d.first")):
        group = {}
        for offset in conn.execute("SELECT DISTINCT offset_s FROM paper_measurements WHERE run=? ORDER BY 1", (run,)).fetchall():
            rows = conn.execute(
                f"""SELECT p.status, p.best_ask, p.fillable_usd, p.edge FROM paper_measurements p
                    JOIN (SELECT event_id, MIN(trigger) AS first FROM paper_measurements WHERE run=? GROUP BY event_id) d ON d.event_id = p.event_id
                    WHERE p.run=? AND p.ledger='H2a' AND p.offset_s=? AND {clause}""", (run, run, offset[0])
            ).fetchall()
            ok = [r for r in rows if r["status"] == "ok"]
            with_ask = [r for r in ok if r["best_ask"] is not None]
            group[f"+{offset[0]}s"] = {
                "brackets": len(rows), "measured": len(ok), "pending": sum(1 for r in rows if r["status"] == "pending"),
                "in_recording_gap": sum(1 for r in rows if r["status"] == "gap"),
                "nobody_selling_no": len(ok) - len(with_ask),
                "median_best_ask": _median([r["best_ask"] for r in with_ask]),
                "median_edge_after_fee": _median([r["edge"] for r in with_ask]),
                "median_buyable_usd": _median([r["fillable_usd"] or 0.0 for r in ok]),
                "share_with_edge_2pp": round(sum(1 for r in with_ask if r["edge"] >= 0.02) / len(ok), 4) if ok else None,
            }
        measures[label] = group
    out["h2a_ruled_out_brackets"] = measures
    out["reconciliations"] = {
        r[0].split(":", 1)[1]: json.loads(r[1])["ok"] for r in conn.execute("SELECT key, value FROM kv WHERE key LIKE 'paper_reconciled:%' ORDER BY key")
    }
    return out


def render(report: dict, as_json: bool = False) -> str:
    if as_json:
        return json.dumps(report, indent=2, ensure_ascii=False)
    lines = [f"paper ledgers ({report['run']}), processed through {report['processed_through']}"]
    for name, e in report["ledgers"].items():
        c = e["conservative"]
        lines.append(f"\n{name} ({e['model_version']}){'  PAUSED: ' + e['paused'] if e['paused'] else ''}")
        lines.append(f"  signals {e['signals']}, entered {e['entered']}, not entered: {e['skipped'] or 'none'}")
        lines.append(f"  invested {c['invested_usd']} USD in {c['fills']} fills (fees {c['fees_usd']}), still open {c['open_usd']}")
        lines.append(f"  settled: {c['settled_fills']} fills, profit {c['settled_pnl_usd']} USD, per USD invested {c['return_per_usd']}, "
                     f"without the 3 best city-days {c['pnl_without_top3_city_days_usd']}")
        o = e["optimistic"]
        lines.append(f"  optimistic fills (no delay, whole visible size): invested {o['invested_usd']}, settled profit {o['settled_pnl_usd']}")
    for label, title in (("during_the_day", "ruled out during the day"), ("by_the_first_report_of_the_day", "ruled out by the day's first report")):
        group = report["h2a_ruled_out_brackets"].get(label) or {}
        if not any(m["brackets"] for m in group.values()):
            continue
        lines.append(f"\nH2a measurement, brackets {title}: NO side by delay after the report was seen")
        for k, m in group.items():
            lines.append(f"  {k:>6}: {m['measured']} measured ({m['pending']} pending, {m['in_recording_gap']} in a gap, {m['nobody_selling_no']} with no seller); "
                         f"median ask {m['median_best_ask']}, median edge after fee {m['median_edge_after_fee']}, "
                         f"median buyable {m['median_buyable_usd']} USD, share with 2pp edge {m['share_with_edge_2pp']}")
    if report["reconciliations"]:
        lines.append("\nreconciliation by observation date: " + ", ".join(f"{d} {'ok' if ok else 'DIFFERENT'}" for d, ok in report["reconciliations"].items()))
    return "\n".join(lines)
