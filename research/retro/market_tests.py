"""Part 2 of the retrospective study: does M0 know anything the market price does not?

Sample: every closed US daily-high event since the switch to METAR settlement
(observation dates 2026-10-24 onward). Market prices are the exchange's 5-minute
series, which has no depth: this can test information content (success criterion 2)
and reproduce a calibration curve (H3), but it cannot measure tradable profit.

Usage: python market_tests.py <retro.sqlite> <prices_5m.parquet> [out_dir]
"""

from __future__ import annotations

import datetime as dt
import email.utils
import sqlite3
import sys
from pathlib import Path
from zoneinfo import ZoneInfo

import numpy as np
import pandas as pd
from scipy import optimize, stats

from calibration import add_walk_forward, build_pairs
from common import UTC, until_iso
from polyweather import stations
from polyweather.catalog.brackets import Bracket
from polyweather.models.m0 import PercentileDistribution, bracket_probabilities

FIRST_DATE = "2026-10-24"
DECISIONS = {"D-1 08:00": (-1, 8), "D-1 18:00": (-1, 18), "D 08:00": (0, 8)}
H3_TIMES = {"D 12:00": (0, 12), "D 15:00": (0, 15), "D 18:00": (0, 18)}
MAX_PRICE_AGE_S = 3600
P_FLOOR = 0.001
FEE_RATE = 0.01
RNG = np.random.default_rng(20261124)


def decision_utc(station: str, day: dt.date, offset_days: int, hour: int) -> dt.datetime:
    zone = ZoneInfo(stations.get(station).tz)
    return dt.datetime.combine(day + dt.timedelta(days=offset_days), dt.time(hour), zone).astimezone(UTC)


def load_events(conn) -> pd.DataFrame:
    return pd.read_sql_query(
        """SELECT e.event_id, s.station, s.obs_date AS date, m.market_id, m.group_index, m.group_title, m.lo, m.hi,
                  (m.winner = 'YES') AS win
           FROM events e JOIN resolution_specs s ON s.event_id = e.event_id JOIN markets m ON m.event_id = e.event_id
           WHERE e.closed = 1 AND s.status = 'ok' AND s.unit = 'F' AND s.measure = 'high'
                 AND s.obs_date >= ? AND s.obs_date <= ?
           ORDER BY s.obs_date, s.station, m.group_index""",
        conn, params=(FIRST_DATE, until_iso()),
    )


class Prices:
    def __init__(self, path: str) -> None:
        df = pd.read_parquet(path).sort_values(["market_id", "ts"])
        self.series = {mid: (g["ts"].to_numpy(), g["price"].to_numpy()) for mid, g in df.groupby("market_id")}

    def at(self, market_id: str, when: dt.datetime) -> float:
        s = self.series.get(market_id)
        if s is None:
            return np.nan
        t = int(when.timestamp())
        i = np.searchsorted(s[0], t, side="right") - 1
        if i < 0 or t - s[0][i] > MAX_PRICE_AGE_S:
            return np.nan
        return float(s[1][i])


def nbp_lookup(conn) -> pd.DataFrame:
    """Calibrated NBP rows with the time each became available."""
    wf = add_walk_forward(build_pairs(conn))
    pub = pd.read_sql_query("SELECT station, cycle, published FROM guidance_blocks WHERE product='nbp'", conn)
    pub["cycle"] = pd.to_datetime(pub["cycle"], utc=True)
    wf = wf.merge(pub, on=["station", "cycle"], how="left")
    avail = []
    for cyc, p, fallback in zip(wf["cycle"], wf["published"], wf["available"]):
        t = email.utils.parsedate_to_datetime(p) if isinstance(p, str) else None
        # A Last-Modified far from the cycle means the object was re-uploaded later.
        avail.append(t if t is not None and dt.timedelta(0) < t - cyc < dt.timedelta(hours=6) else fallback)
    wf["available"] = pd.to_datetime(avail, utc=True)
    return wf


def build_panel(conn, prices: Prices) -> pd.DataFrame:
    ev = load_events(conn)
    nbp = nbp_lookup(conn)
    nbp_by = {k: g.sort_values("available") for k, g in nbp.groupby(["station", "date"])}
    rows = []
    for (event_id, station, date), g in ev.groupby(["event_id", "station", "date"], sort=False):
        day = dt.date.fromisoformat(date)
        brackets = [Bracket(None if pd.isna(lo) else int(lo), None if pd.isna(hi) else int(hi), "F") for lo, hi in zip(g["lo"], g["hi"])]
        for label, (off, hour) in {**DECISIONS, **H3_TIMES}.items():
            t = decision_utc(station, day, off, hour)
            q_raw = np.array([prices.at(m, t) for m in g["market_id"]])
            model_raw = model_cal = np.full(len(g), np.nan)
            fhr = age_h = np.nan
            cand = nbp_by.get((station, day))
            if cand is not None and label in DECISIONS:
                ok = cand[cand["available"] <= t]
                if len(ok):
                    r = ok.iloc[-1]
                    d = PercentileDistribution.from_percentiles(r.p10, r.p25, r.p50, r.p75, r.p90)
                    model_raw = np.array(bracket_probabilities(d, brackets))
                    if not (pd.isna(r["shift"]) or pd.isna(r["tail_scale"])):
                        model_cal = np.array(bracket_probabilities(d.calibrated(r["shift"], 1.0, r["tail_scale"]), brackets))
                    fhr, age_h = r.fhr, (t - r.cycle).total_seconds() / 3600
            for i, m in enumerate(g.itertuples()):
                rows.append(
                    {"event_id": event_id, "station": station, "date": date, "decision": label, "bracket": m.group_title,
                     "idx": i, "win": int(m.win), "price": q_raw[i], "m_raw": model_raw[i], "m_cal": model_cal[i],
                     "fhr": fhr, "forecast_age_h": age_h}
                )
    panel = pd.DataFrame(rows)
    panel["price_sum"] = panel.groupby(["event_id", "decision"])["price"].transform(lambda s: s.sum(min_count=len(s)))
    panel["q"] = panel["price"] / panel["price_sum"]
    return panel


# ---- success criterion 2: conditional logit ------------------------------------------


def conditional_logit(X: np.ndarray, win: np.ndarray, cluster: np.ndarray, weights: np.ndarray | None = None):
    """ML fit of P(j) ∝ exp(x_j · beta) within each city-day; SEs clustered by date.

    ``X`` has shape [city-days, brackets, k]; ``win`` is the winning bracket's index;
    ``cluster`` the date code of each city-day; ``weights`` repeats city-days (bootstrap).
    """
    n, _, k = X.shape
    w = np.ones(n) if weights is None else weights
    rows = np.arange(n)

    def parts(beta):
        u = X @ beta
        u -= u.max(axis=1, keepdims=True)
        p = np.exp(u)
        p /= p.sum(axis=1, keepdims=True)
        xbar = np.einsum("nj,njk->nk", p, X)
        return p, xbar

    def negll(beta):
        p, _ = parts(beta)
        return -np.sum(w * np.log(p[rows, win]))

    def grad(beta):
        _, xbar = parts(beta)
        return -((w[:, None] * (X[rows, win] - xbar)).sum(axis=0))

    res = optimize.minimize(negll, np.zeros(k), jac=grad, method="BFGS")
    if weights is not None:
        return res.x, None, None, None
    p, xbar = parts(res.x)
    dev = X - xbar[:, None, :]
    hess = np.einsum("nj,nja,njb->ab", p, dev, dev)
    score = X[rows, win] - xbar
    codes = np.unique(cluster)
    S = np.array([score[cluster == c].sum(axis=0) for c in codes])
    G = len(codes)
    inv = np.linalg.inv(hess)
    cov = inv @ (S.T @ S) @ inv * G / (G - 1)
    return res.x, np.sqrt(np.diag(cov)), -res.fun, G


def criterion_two(panel: pd.DataFrame, col: str) -> pd.DataFrame:
    out = []
    for label in DECISIONS:
        sub = panel[(panel["decision"] == label)].dropna(subset=["q", col])
        Xs, wins, dates = [], [], []
        for (_, date), g in sub.groupby(["event_id", "date"]):
            if len(g) != 11 or g["win"].sum() != 1:
                continue
            Xs.append(np.column_stack([np.log(g["q"].clip(lower=P_FLOOR)), np.log(g[col].clip(lower=P_FLOOR))]))
            wins.append(int(np.argmax(g["win"].to_numpy())))
            dates.append(date)
        if len(Xs) < 30:
            continue
        X, win = np.array(Xs), np.array(wins)
        codes, cluster = np.unique(dates, return_inverse=True)
        beta, se, _, G = conditional_logit(X, win, cluster)
        boots = []
        for _ in range(2000):  # resample whole dates
            draw = np.bincount(RNG.integers(0, G, G), minlength=G)
            boots.append(conditional_logit(X, win, cluster, weights=draw[cluster].astype(float))[0][1])
        lo, hi = np.percentile(boots, [2.5, 97.5])
        rows = np.arange(len(win))
        out.append(
            {"decision": label, "model": col, "city_days": len(win), "dates": G,
             "beta_market": beta[0], "se_market": se[0], "beta_model": beta[1], "se_model": se[1],
             "p_one_sided": float(stats.t.sf(beta[1] / se[1], G - 1)), "boot_lo": lo, "boot_hi": hi,
             "boot_share_positive": float(np.mean(np.array(boots) > 0)),
             "logscore_market": float(X[rows, win, 0].mean()), "logscore_model": float(X[rows, win, 1].mean()),
             "forecast_age_h": float(sub["forecast_age_h"].mean())}
        )
    return pd.DataFrame(out)


# ---- optimistic bound on the H1-M0 rule, and H3 --------------------------------------


def boot_mean_ci(values_by_date: dict[str, list[float]]) -> tuple[float, float, float]:
    keys = sorted(values_by_date)
    if not keys:
        return (np.nan, np.nan, np.nan)
    allv = np.concatenate([values_by_date[k] for k in keys])
    means = []
    for _ in range(4000):
        v = np.concatenate([values_by_date[k] for k in RNG.choice(keys, len(keys))])
        means.append(v.mean())
    lo, hi = np.percentile(means, [2.5, 97.5])
    return float(allv.mean()), float(lo), float(hi)


def h1_mid_price_bound(panel: pd.DataFrame, col: str, threshold: float = 0.05) -> pd.DataFrame:
    """Return per dollar of the plan's H1 rule if every order filled at the 5-minute price
    plus ``slip``. This is an upper bound, not a paper-trading result."""
    out = []
    for label in DECISIONS:
        sub = panel[panel["decision"] == label].dropna(subset=["price", col])
        for slip in (0.0, 0.01, 0.02, 0.03):
            by_date: dict[str, list[float]] = {}
            n = 0
            for (_, date), g in sub.groupby(["event_id", "date"]):
                best = None
                for r in g.itertuples():
                    for side, cost0, p_model, wins in (("YES", r.price, getattr(r, col), r.win), ("NO", 1 - r.price, 1 - getattr(r, col), 1 - r.win)):
                        cost = cost0 + slip
                        if not (0.01 <= cost <= 0.97):
                            continue  # the plan's tail guard: nothing above 97 cents
                        fee = FEE_RATE * cost * (1 - cost)
                        edge = p_model - cost - fee
                        if edge >= threshold and (best is None or edge > best[0]):
                            best = (edge, cost, fee, wins)
                if best:
                    _, cost, fee, wins = best
                    by_date.setdefault(date, []).append((wins - cost - fee) / (cost + fee))
                    n += 1
            mean, lo, hi = boot_mean_ci(by_date)
            out.append({"decision": label, "model": col, "slip": slip, "trades": n, "return_per_dollar": mean, "ci_lo": lo, "ci_hi": hi})
    return pd.DataFrame(out)


def h3(panel: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    curve, strat = [], []
    edges = [0, 0.05, 0.15, 0.30, 0.50, 0.70, 0.85, 0.95, 1.0001]
    for label in list(DECISIONS) + list(H3_TIMES):
        sub = panel[panel["decision"] == label].dropna(subset=["price"])
        sub = sub.assign(bucket=pd.cut(sub["price"], edges, right=False))
        for b, g in sub.groupby("bucket", observed=True):
            curve.append({"decision": label, "price_bucket": str(b), "n": len(g), "mean_price": g["price"].mean(),
                          "win_rate": g["win"].mean(), "gap": g["win"].mean() - g["price"].mean()})
        if label in H3_TIMES:
            fav = sub[(sub["price"] >= 0.70) & (sub["price"] <= 0.95)]
            for slip in (0.0, 0.01, 0.02):
                by_date: dict[str, list[float]] = {}
                for r in fav.itertuples():
                    cost = 1 - r.price + slip
                    fee = FEE_RATE * cost * (1 - cost)
                    by_date.setdefault(r.date, []).append(((1 - r.win) - cost - fee) / (cost + fee))
                mean, lo, hi = boot_mean_ci(by_date)
                strat.append({"decision": label, "slip": slip, "trades": len(fav), "favourite_win_rate": fav["win"].mean(),
                              "mean_price": fav["price"].mean(), "return_per_dollar": mean, "ci_lo": lo, "ci_hi": hi})
    return pd.DataFrame(curve), pd.DataFrame(strat)


def main() -> None:
    conn = sqlite3.connect(sys.argv[1])
    out = Path(sys.argv[3]) if len(sys.argv) > 3 else Path(__file__).parent / "data"
    out.mkdir(parents=True, exist_ok=True)
    pd.set_option("display.width", 220)
    panel = build_panel(conn, Prices(sys.argv[2]))
    n_events = panel["event_id"].nunique()
    print(f"panel: {n_events} city-days, {panel['date'].nunique()} dates, {panel['station'].nunique()} stations")
    cover = panel.groupby("decision").apply(lambda g: pd.Series({
        "city_days_priced": g.dropna(subset=["q"])["event_id"].nunique(),
        "city_days_with_model": g.dropna(subset=["m_cal"])["event_id"].nunique(),
        "median_price_sum": g.drop_duplicates(["event_id"])["price_sum"].median()}), include_groups=False)
    print("\nCoverage by decision time:")
    print(cover.round(3).to_string())

    c2 = pd.concat([criterion_two(panel, "m_cal"), criterion_two(panel, "m_raw")], ignore_index=True)
    print("\nCriterion 2 (retrospective): conditional logit of the winner on log market price and log model probability")
    print(c2.round(4).to_string(index=False))

    bound = pd.concat([h1_mid_price_bound(panel, "m_cal")], ignore_index=True)
    print("\nH1-M0 rule at the 5-minute price plus slip (upper bound; no depth, no spread):")
    print(bound.round(4).to_string(index=False))

    curve, strat = h3(panel)
    print("\nCalibration of YES prices by decision time:")
    print(curve.round(4).to_string(index=False))
    print("\nH3 rule: buy NO where YES is 0.70-0.95 (upper bound, same caveat):")
    print(strat.round(4).to_string(index=False))

    panel.round(5).to_csv(out / "market_panel.csv.gz", index=False)
    c2.to_csv(out / "criterion2.csv", index=False)
    bound.to_csv(out / "h1_mid_price_bound.csv", index=False)
    curve.to_csv(out / "price_calibration.csv", index=False)
    strat.to_csv(out / "h3_mid_price_bound.csv", index=False)


if __name__ == "__main__":
    main()
