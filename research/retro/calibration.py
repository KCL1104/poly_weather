"""Part 1 of the retrospective study: how well calibrated is NBM against the settlement value?

Uses no market data. For 13 months of archived NBP bulletins and METAR history it
compares the forecast percentiles of the daily maximum with the value the market
rule would have settled on, then scores three ways of turning a bulletin into
bracket probabilities, each fitted only on days before the one being scored:

  raw        the five percentiles as issued
  shift      plus a per-station, per-lead bias (decayed mean of settlement - median)
  shift+s    plus one pooled spread factor per lead (root-mean-square based)
  shift+tail the shift plus one pooled tail scale per lead (middle left as issued)

Usage: python calibration.py <retro.sqlite> [out_dir]
"""

from __future__ import annotations

import datetime as dt
import sqlite3
import sys
from pathlib import Path

import numpy as np
import pandas as pd

from common import US, daily_extremes, nbp_max_rows
from polyweather.models.m0 import GAUSSIAN_MEAN_TAIL_EXCESS, PercentileDistribution

LEADS = (17, 23, 29, 35, 41, 47, 53)  # forecast hours of the NBP "max" column
HALF_LIFE_DAYS = 30.0
MIN_HISTORY = 30
SPREAD_WINDOW_DAYS = 120
Z90 = 1.2815515655446004


def build_pairs(conn: sqlite3.Connection) -> pd.DataFrame:
    obs = pd.concat([daily_extremes(conn, s) for s in US], ignore_index=True)
    obs = obs[obs["n"] >= 20]  # a day needs most of its hourly reports to be a usable truth
    fc = nbp_max_rows(conn)
    fc = fc[fc["fhr"].isin(LEADS)]
    df = fc.merge(obs[["station", "date", "high_f"]], on=["station", "date"], how="inner")
    df["err"] = df["high_f"] - df["p50"]
    df["sigma_fc"] = ((df["p90"] - df["p10"]) / (2 * Z90)).clip(lower=0.4)
    return df.sort_values(["station", "fhr", "date"]).reset_index(drop=True)


def add_walk_forward(df: pd.DataFrame) -> pd.DataFrame:
    """Per-station shift and pooled spread factor, each from strictly earlier days."""
    df = df.copy()
    df["shift"] = np.nan
    for (_, _), g in df.groupby(["station", "fhr"]):
        days = np.array([d.toordinal() for d in g["date"]])
        err = g["err"].to_numpy(float)
        out = np.full(len(g), np.nan)
        for i in range(len(g)):
            past = days < days[i]
            if past.sum() >= MIN_HISTORY:
                w = 0.5 ** ((days[i] - days[past]) / HALF_LIFE_DAYS)
                out[i] = np.sum(w * err[past]) / np.sum(w)
        df.loc[g.index, "shift"] = out
    df["resid"] = df["err"] - df["shift"]
    df["spread"] = np.nan
    for fhr, g in df.dropna(subset=["shift"]).groupby("fhr"):
        days = np.array([d.toordinal() for d in g["date"]])
        ratio2 = (g["resid"] / g["sigma_fc"]).to_numpy(float) ** 2
        out = np.full(len(g), np.nan)
        for day in np.unique(days):
            past = (days < day) & (days >= day - SPREAD_WINDOW_DAYS)
            if past.sum() >= 200:
                out[days == day] = np.sqrt(np.mean(ratio2[past]))
        df.loc[g.index, "spread"] = out

    # Tail scale: mean overshoot beyond the shifted 10th/90th percentile, relative to
    # what Gaussian tails would give, pooled over stations on a trailing window.
    excess = []
    for r in df.itertuples():
        if pd.isna(r.shift):
            excess.append(np.nan)
            continue
        d = PercentileDistribution.from_percentiles(r.p10, r.p25, r.p50, r.p75, r.p90).calibrated(r.shift)
        e = d.tail_excess_z(float(r.high_f))
        excess.append(np.nan if e is None else e)
    df["tail_excess"] = excess
    df["tail_scale"] = np.nan
    for fhr, g in df.dropna(subset=["shift"]).groupby("fhr"):
        days = np.array([d.toordinal() for d in g["date"]])
        ex = g["tail_excess"].to_numpy(float)
        out = np.full(len(g), np.nan)
        for day in np.unique(days):
            past = (days < day) & (days >= day - SPREAD_WINDOW_DAYS) & ~np.isnan(ex)
            if past.sum() >= 40:
                out[days == day] = float(np.clip(np.mean(ex[past]) / GAUSSIAN_MEAN_TAIL_EXCESS, 1.0, 4.0))
        df.loc[g.index, "tail_scale"] = out
    return df


def bin_prob(row, shift: float, spread: float, tail: float = 1.0) -> tuple[float, float]:
    """(probability of the 2 °F bin holding the settlement value, PIT of that value)."""
    d = PercentileDistribution.from_percentiles(row.p10, row.p25, row.p50, row.p75, row.p90).calibrated(shift, spread, tail)
    lo = (int(row.high_f) // 2) * 2  # bins [even, odd], like the market's ladders
    p = d.prob_between(lo - 0.5, lo + 1.5)
    u = 0.5 * (d.cdf(row.high_f - 0.5) + d.cdf(row.high_f + 0.5))
    return p, u


def score(df: pd.DataFrame) -> pd.DataFrame:
    rows = []
    for variant in ("raw", "shift", "shift+s", "shift+tail"):
        sub = df.dropna(subset=["shift", "spread", "tail_scale"])
        ps, us = [], []
        for r in sub.itertuples():
            sh = 0.0 if variant == "raw" else r.shift
            sp = r.spread if variant == "shift+s" else 1.0
            tl = r.tail_scale if variant == "shift+tail" else 1.0
            p, u = bin_prob(r, sh, sp, tl)
            ps.append(max(p, 1e-4))
            us.append(u)
        sub = sub.assign(p=ps, u=us)
        for fhr, g in sub.groupby("fhr"):
            rows.append(
                {
                    "variant": variant, "fhr": fhr, "n": len(g),
                    "log_score": float(np.mean(np.log(g["p"]))),
                    "mean_p_winner": float(g["p"].mean()),
                    "p_winner_under_1pct": float((g["p"] < 0.01).mean()),
                    "cover80": float(((g["u"] > 0.10) & (g["u"] < 0.90)).mean()),
                    "cover50": float(((g["u"] > 0.25) & (g["u"] < 0.75)).mean()),
                    "below_p10": float((g["u"] <= 0.10).mean()),
                    "above_p90": float((g["u"] >= 0.90).mean()),
                }
            )
    return pd.DataFrame(rows)


def reliability(df: pd.DataFrame) -> pd.DataFrame:
    """When the model says p for a 2 °F bracket, how often does that bracket hold the value?

    Each forecast is scored on eleven brackets centred on its median, like a market ladder.
    """
    rows = []
    for r in df[df["fhr"].isin((17, 29))].itertuples():
        d = PercentileDistribution.from_percentiles(r.p10, r.p25, r.p50, r.p75, r.p90).calibrated(r.shift, 1.0, r.tail_scale)
        centre = (int(round(d.median)) // 2) * 2
        for k in range(-5, 6):
            lo = centre + 2 * k
            p = d.prob_between(None if k == -5 else lo - 0.5, None if k == 5 else lo + 1.5)
            hit = (r.high_f < lo + 2) if k == -5 else (r.high_f >= lo) if k == 5 else (lo <= r.high_f <= lo + 1)
            rows.append((r.fhr, p, int(hit)))
    t = pd.DataFrame(rows, columns=["fhr", "p", "hit"])
    t["bucket"] = pd.cut(t["p"], [0, 0.01, 0.03, 0.07, 0.15, 0.25, 0.35, 0.50, 1.0], include_lowest=True)
    return t.groupby(["fhr", "bucket"], observed=True).agg(n=("hit", "size"), predicted=("p", "mean"), observed=("hit", "mean")).reset_index()


def main() -> None:
    conn = sqlite3.connect(sys.argv[1])
    out = Path(sys.argv[2]) if len(sys.argv) > 2 else Path(__file__).parent / "data"
    out.mkdir(parents=True, exist_ok=True)
    pairs = build_pairs(conn)
    pd.set_option("display.width", 200)
    print(f"pairs: {len(pairs)} rows, {pairs['station'].nunique()} stations, {pairs['date'].min()} .. {pairs['date'].max()}")

    raw = pairs.groupby("fhr").agg(
        n=("err", "size"), bias=("err", "mean"), mae=("err", lambda e: e.abs().mean()),
        rmse=("err", lambda e: float(np.sqrt((e**2).mean()))), sigma_fc=("sigma_fc", "mean"),
        in_p10_p90=("err", lambda e: float("nan")),
    )
    raw["in_p10_p90"] = pairs.assign(ok=(pairs.high_f >= pairs.p10) & (pairs.high_f <= pairs.p90)).groupby("fhr")["ok"].mean()
    raw["below_p10"] = pairs.assign(ok=pairs.high_f < pairs.p10).groupby("fhr")["ok"].mean()
    raw["above_p90"] = pairs.assign(ok=pairs.high_f > pairs.p90).groupby("fhr")["ok"].mean()
    print("\nRaw NBP against the settlement value, by forecast hour (bias = settlement - median, °F):")
    print(raw.round(3).to_string())

    by_station = pairs[pairs.fhr.isin((17, 29))].pivot_table(index="station", columns="fhr", values="err", aggfunc=["mean", "std"]).round(2)
    print("\nBias and spread of (settlement - median) by station, forecast hours 17 and 29:")
    print(by_station.to_string())

    wf = add_walk_forward(pairs)
    scored = score(wf)
    print("\nWalk-forward scores on 2 °F bins (higher log score is better; cover80 should be 0.80):")
    print(scored.round(4).to_string(index=False))
    params = wf.dropna(subset=["spread", "tail_scale"]).groupby("fhr")[["tail_scale", "spread"]].agg(["mean", "min", "max"]).round(2)
    print("\nFitted pooled parameters by forecast hour:")
    print(params.to_string())
    rel = reliability(wf.dropna(subset=["shift", "tail_scale"]))
    print("\nReliability of bracket probabilities (variant shift+tail, forecast hours 17 and 29):")
    print(rel.round(4).to_string(index=False))
    rel.to_csv(out / "reliability_shift_tail.csv", index=False)

    pairs.round(3).to_csv(out / "nbp_vs_settlement.csv.gz", index=False)
    scored.to_csv(out / "calibration_scores.csv", index=False)
    raw.round(4).to_csv(out / "nbp_raw_diagnostics.csv")


if __name__ == "__main__":
    main()
