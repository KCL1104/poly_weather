"""Forecast-only test: can public guidance match the market's error on the settlement value?

For every US station-day and three decision times (station local: day-before 08:00,
day-before 18:00, same-day 08:00) this builds, from what had been published by then:

  nbs    NBM hourly deterministic bulletin, the day's maximum from the last cycle
         published before the decision
  nbp    NBM probabilistic bulletin, median of the daily maximum
  lamp   GFS LAMP, maximum of the hourly temperatures over the local day
  gfs    GFS MOS, the max/min ("N/X") value for the day
  hrrr   HRRR 3 km, maximum of the hourly 2 m temperatures at the nearest grid point
  ecmwf  ECMWF high-resolution forecast, maximum of the 3-hour maxima (when downloaded)

Each source gets a per-station bias correction (decayed mean of past errors), and the
corrected sources are combined by a pooled linear regression. Both are walk-forward:
a forecast for day D uses only days whose settlement was known two days before D.
The yardstick is the mean implied by market prices at the same decision times.

Usage: python evaluate.py <retro.sqlite> <forecast_test.sqlite> <market_panel.csv.gz> [out_dir]
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
from scipy import stats

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "retro"))
from common import US, daily_extremes, nbp_max_rows  # noqa: E402
from market_tests import P_FLOOR, conditional_logit  # noqa: E402

from polyweather import stations  # noqa: E402

UTC = dt.timezone.utc
DECISIONS = {"D-1 08:00": (-1, 8), "D-1 18:00": (-1, 18), "D 08:00": (0, 8)}
TEST_START = dt.date(2026, 10, 24)
HALF_LIFE = 30.0
MIN_HISTORY = 15
LAMP_LAG = dt.timedelta(minutes=40)   # the HH:30 cycle, labelled HH:00 in the archive, is out by HH:35
GFS_MOS_LAG = dt.timedelta(hours=4, minutes=30)
ECMWF_LAG = dt.timedelta(hours=7, minutes=50)
NBS_LAG = dt.timedelta(minutes=45)
ECMWF_MIN_DAYS = 12
RNG = np.random.default_rng(20261124)


def c_to_f(c):
    return c * 9.0 / 5.0 + 32.0


def decision_utc(station: str, day: dt.date, offset: int, hour: int) -> dt.datetime:
    zone = ZoneInfo(stations.get(station).tz)
    return dt.datetime.combine(day + dt.timedelta(days=offset), dt.time(hour), zone).astimezone(UTC)


def utc(day: dt.date, hour: int, plus_days: int = 0) -> dt.datetime:
    return dt.datetime(day.year, day.month, day.day, hour, tzinfo=UTC) + dt.timedelta(days=plus_days)


# ---- loaders: one lookup structure per source -------------------------------------------


def load_nbp(conn) -> dict:
    df = nbp_max_rows(conn)
    pub = pd.read_sql_query("SELECT station, cycle, published FROM guidance_blocks WHERE product='nbp'", conn)
    pub["cycle"] = pd.to_datetime(pub["cycle"], utc=True)
    df = df.merge(pub, on=["station", "cycle"], how="left")
    avail = []
    for cyc, p, fallback in zip(df["cycle"], df["published"], df["available"]):
        t = email.utils.parsedate_to_datetime(p) if isinstance(p, str) else None
        avail.append(t if t is not None and dt.timedelta(0) < t - cyc < dt.timedelta(hours=6) else fallback)
    df["available"] = pd.to_datetime(avail, utc=True)
    return {k: g.sort_values("available") for k, g in df.groupby(["station", "date"])}


def load_mos(conn, model: str) -> dict:
    df = pd.read_sql_query("SELECT station, runtime, ftime, n_x, tmp FROM mos WHERE model=?", conn, params=(model,))
    df["runtime"] = pd.to_datetime(df["runtime"], utc=True)
    df["ftime"] = pd.to_datetime(df["ftime"], utc=True)
    if model == "LAV":  # the long-term archive keeps four cycles a day; use only those throughout
        df = df[df["runtime"].dt.hour.isin((0, 6, 12, 18))]
    return {st: {rt: g.set_index("ftime") for rt, g in gs.groupby("runtime")} for st, gs in df.groupby("station")}


def load_grid(conn, table: str, time_col: str, value: str, where: str = "") -> dict:
    df = pd.read_sql_query(f"SELECT station, {time_col} AS run, valid, {value} AS v FROM {table} {where}", conn)
    if df.empty:
        return {}
    df["run"] = pd.to_datetime(df["run"], utc=True)
    df["valid"] = pd.to_datetime(df["valid"], utc=True)
    return {st: {rt: g.set_index("valid")["v"] for rt, g in gs.groupby("run")} for st, gs in df.groupby("station")}


def load_obs(conn) -> dict:
    out = {}
    for st in US:
        df = pd.read_sql_query(
            "SELECT obs_time, temp_tenths_c FROM metar_obs WHERE station=? AND temp_tenths_c IS NOT NULL "
            "AND COALESCE(metar_type,'METAR') != 'SPECI'", conn, params=(st,))
        df["t"] = pd.to_datetime(df["obs_time"], utc=True)
        s = df.drop_duplicates("t").set_index("t")["temp_tenths_c"].sort_index() / 10.0
        out[st] = c_to_f(s)
    return out


# ---- one feature row per station, day and decision time ----------------------------------


def latest(runs: dict, not_after: pd.Timestamp, lag: dt.timedelta):
    ok = [rt for rt in runs if rt + lag <= not_after]
    return max(ok) if ok else None


def build_features(retro, ftest) -> pd.DataFrame:
    truth = pd.concat([daily_extremes(retro, s) for s in US], ignore_index=True)
    truth = truth[truth["n"] >= 20].set_index(["station", "date"])["high_f"]
    nbp, lamp, gfs = load_nbp(retro), load_mos(ftest, "LAV"), load_mos(ftest, "GFS")
    hrrr = load_grid(ftest, "hrrr", "cycle", "tmp_c")
    nbs = load_grid(ftest, "nbs", "cycle", "txn", "WHERE kind='max'")
    ecmwf = load_grid(ftest, "ecmwf_oper", "run", "value_c", "WHERE param='mx2t3'")
    obs = load_obs(retro)
    first = min(rt for runs in hrrr.values() for rt in runs).date() + dt.timedelta(days=2)
    last = max(d for _, d in truth.index)
    rows = []
    for st in US:
        zone = ZoneInfo(stations.get(st).tz)
        day = first
        while day <= last:
            y = truth.get((st, day))
            day_start = dt.datetime.combine(day, dt.time(0), zone).astimezone(UTC)
            day_end = day_start + dt.timedelta(days=1)
            window = (pd.Timestamp(utc(day, 12)), pd.Timestamp(utc(day, 3, 1)))  # 12Z to 03Z: where the high falls
            for label, (off, hour) in DECISIONS.items():
                t = pd.Timestamp(decision_utc(st, day, off, hour))
                row = {"station": st, "date": day, "decision": label, "y": y}

                cand = nbp.get((st, day))
                if cand is not None:
                    ok = cand[cand["available"] <= t]
                    if len(ok):
                        row["nbp"] = float(ok.iloc[-1]["p50"])

                rt = latest(nbs.get(st, {}), t, NBS_LAG)
                if rt is not None and t - rt <= pd.Timedelta(hours=3):
                    v = nbs[st][rt].get(pd.Timestamp(utc(day, 0, 1)))
                    if v is not None and pd.notna(v):
                        row["nbs"] = float(v)

                rt = latest(lamp.get(st, {}), t, LAMP_LAG)
                if rt is not None:
                    s = lamp[st][rt]["tmp"]
                    s = s[(s.index >= max(pd.Timestamp(day_start), t)) & (s.index < pd.Timestamp(day_end))]
                    covers = len(s) and s.index.max() >= window[1] - pd.Timedelta(hours=3)
                    if covers:
                        row["lamp"] = float(s.max())

                rt = latest(gfs.get(st, {}), t, GFS_MOS_LAG)
                if rt is not None:
                    g = gfs[st][rt]
                    key = pd.Timestamp(utc(day, 0, 1))
                    if key in g.index and pd.notna(g.loc[key, "n_x"]):
                        row["gfs"] = float(g.loc[key, "n_x"])

                cycle = {"D-1 08:00": utc(day, 6, -1), "D-1 18:00": utc(day, 18, -1)}.get(label)
                if cycle is None:
                    cycle = (t - pd.Timedelta(hours=2)).floor("h").to_pydatetime()
                s = hrrr.get(st, {}).get(pd.Timestamp(cycle))
                if s is not None:
                    s = s[(s.index >= window[0]) & (s.index <= window[1]) & (s.index > t - pd.Timedelta(hours=1))]
                    if len(s) >= 10:
                        row["hrrr"] = float(c_to_f(s.max()))

                run = {"D-1 08:00": utc(day, 0, -1), "D-1 18:00": utc(day, 12, -1), "D 08:00": utc(day, 0)}[label]
                s = ecmwf.get(st, {}).get(pd.Timestamp(run))
                if s is not None and pd.Timestamp(run) + ECMWF_LAG <= t:
                    s = s[(s.index > window[0]) & (s.index <= window[1])]
                    if len(s) >= 5:
                        row["ecmwf"] = float(c_to_f(s.max()))

                o = obs[st]
                past = o[o.index <= t]
                if len(past):
                    row["temp_now"] = float(past.iloc[-1])
                    local_day_start = pd.Timestamp(dt.datetime.combine(t.tz_convert(zone).date(), dt.time(0), zone).astimezone(UTC))
                    today = past[past.index >= local_day_start]
                    row["max_so_far"] = float(today.max()) if len(today) else np.nan
                rows.append(row)
            day += dt.timedelta(days=1)
    return pd.DataFrame(rows)


# ---- walk-forward bias correction and combination -------------------------------------------


def add_bias_corrected(df: pd.DataFrame, sources: list[str]) -> pd.DataFrame:
    df = df.copy()
    for src in sources:
        df[f"{src}_c"] = np.nan
        for _, g in df.dropna(subset=[src]).groupby(["station", "decision"]):
            g = g.sort_values("date")
            days = np.array([d.toordinal() for d in g["date"]])
            err = (g["y"] - g[src]).to_numpy(float)
            out = np.full(len(g), np.nan)
            for i in range(len(g)):
                past = (days <= days[i] - 2) & ~np.isnan(err)
                if past.sum() >= MIN_HISTORY:
                    w = 0.5 ** ((days[i] - days[past]) / HALF_LIFE)
                    out[i] = g[src].iloc[i] + np.sum(w * err[past]) / np.sum(w)
            df.loc[g.index, f"{src}_c"] = out
    return df


def combine(df: pd.DataFrame, sources: list[str], name: str, extra: list[str] = (), min_days: int = 30) -> pd.DataFrame:
    """Pooled regression of the settlement value on the corrected sources, refitted for
    every target day on the days already settled (at least ``min_days`` of them)."""
    df = df.copy()
    df[name] = np.nan
    cols = [f"{s}_c" for s in sources] + list(extra)
    for label, g in df.groupby("decision"):
        g = g.dropna(subset=cols)
        days = np.array([d.toordinal() for d in g["date"]])
        X = g[cols].to_numpy(float)
        y = g["y"].to_numpy(float)
        ref = X[:, 0]
        out = np.full(len(g), np.nan)
        for day in np.unique(days):
            train = (days <= day - 2) & ~np.isnan(y)
            if len(np.unique(days[train])) < min_days:
                continue
            # Regress on differences from the first source so the weights sum to one.
            A = np.column_stack([X[train, 1:] - ref[train, None], np.ones(train.sum())])
            beta, *_ = np.linalg.lstsq(A.T @ A + 1e-3 * np.eye(A.shape[1]), A.T @ (y[train] - ref[train]), rcond=None)
            cur = days == day
            out[cur] = ref[cur] + np.column_stack([X[cur, 1:] - ref[cur, None], np.ones(cur.sum())]) @ beta
        df.loc[g.index, name] = out
    return df


# ---- evaluation ----------------------------------------------------------------------------------


def market_means(panel: pd.DataFrame, retro) -> pd.DataFrame:
    ev = pd.read_sql_query("SELECT event_id, group_index AS idx, lo, hi FROM markets", retro)
    ev["event_id"] = ev["event_id"].astype(str)
    p = panel.assign(event_id=panel["event_id"].astype(str)).merge(ev, on=["event_id", "idx"])
    p["centre"] = np.where(p["lo"].isna(), p["hi"] - 0.5, np.where(p["hi"].isna(), p["lo"] + 0.5, (p["lo"] + p["hi"]) / 2))
    p = p.dropna(subset=["q"])
    out = p.groupby(["station", "date", "decision"]).apply(lambda d: (d["q"] * d["centre"]).sum(), include_groups=False)
    return out.rename("market").reset_index()


def boot_ci(diff_by_date: pd.Series) -> tuple[float, float]:
    groups = [g.to_numpy() for _, g in diff_by_date.groupby(level=0)]
    means = []
    for _ in range(4000):
        pick = RNG.integers(0, len(groups), len(groups))
        means.append(np.concatenate([groups[i] for i in pick]).mean())
    return tuple(np.percentile(means, [2.5, 97.5]))


def main() -> None:
    retro = sqlite3.connect(sys.argv[1])
    ftest = sqlite3.connect(sys.argv[2])
    panel = pd.read_csv(sys.argv[3])
    out = Path(sys.argv[4]) if len(sys.argv) > 4 else Path(__file__).parent / "data"
    out.mkdir(parents=True, exist_ok=True)
    pd.set_option("display.width", 220)

    feats = build_features(retro, ftest)
    sources = [s for s in ("nbs", "nbp", "lamp", "gfs", "hrrr", "ecmwf") if s in feats and feats[s].notna().sum() > 500]
    print("rows:", len(feats), "| days:", feats["date"].min(), "..", feats["date"].max(), "| sources:", sources)
    print("\nCoverage (share of station-days with a value), by decision time:")
    print(feats.groupby("decision")[sources + ["temp_now"]].agg(lambda s: s.notna().mean()).round(3).to_string())

    feats = add_bias_corrected(feats.dropna(subset=["y"]), sources)
    main_sources = [s for s in sources if s != "ecmwf"]
    # Two sources are not always there: LAMP reaches only 38 hours, and NBS drops the
    # same-day maximum from its mid-morning cycles onward. A row uses the blend fitted
    # on the largest set of sources it has.
    optional = [s for s in ("nbs", "lamp") if s in main_sources]
    patterns = [main_sources] + [[s for s in main_sources if s != o] for o in optional]
    patterns.append([s for s in main_sources if s not in optional])
    feats["blend"] = np.nan
    feats["blend_obs"] = np.nan
    for i, pattern in enumerate(patterns):
        feats = combine(feats, pattern, f"_b{i}")
        feats = combine(feats, pattern, f"_o{i}", extra=["temp_now"])
        feats["blend"] = feats["blend"].fillna(feats[f"_b{i}"])
        feats["blend_obs"] = feats["blend_obs"].fillna(feats[f"_o{i}"])
    if "ecmwf" in sources:
        feats["blend_ecmwf"] = np.nan
        for i, pattern in enumerate(patterns):
            # ECMWF was downloaded for a shorter period, so its blend starts on less history.
            feats = combine(feats, pattern + ["ecmwf"], f"_e{i}", min_days=ECMWF_MIN_DAYS)
            feats["blend_ecmwf"] = feats["blend_ecmwf"].fillna(feats[f"_e{i}"])
    feats = feats.drop(columns=[c for c in feats.columns if c.startswith("_")])
    core = [s for s in main_sources if s not in optional]
    feats["mean_of_sources"] = feats[[f"{s}_c" for s in core]].mean(axis=1, skipna=False)

    mk = market_means(panel, retro)
    mk["date"] = pd.to_datetime(mk["date"]).dt.date
    test = feats[feats["date"] >= TEST_START].merge(mk, on=["station", "date", "decision"], how="inner")
    cols = [f"{s}_c" for s in sources] + ["mean_of_sources", "blend", "blend_obs"] + (["blend_ecmwf"] if "ecmwf" in sources else [])
    rows = []
    for label in DECISIONS:
        t = test[test["decision"] == label].dropna(subset=["blend", "market"])
        row = {"decision": label, "city_days": len(t), "market": (t["market"] - t["y"]).abs().mean()}
        for c in cols:
            tt = t.dropna(subset=[c])
            row[c] = (tt[c] - tt["y"]).abs().mean()
            if c == "blend_ecmwf":  # ECMWF covers fewer days: compare like with like
                row["n_with_ecmwf"] = len(tt)
                row["blend_same_rows"] = (tt["blend"] - tt["y"]).abs().mean()
                row["market_same_rows"] = (tt["market"] - tt["y"]).abs().mean()
            if c == "blend":
                d = ((tt[c] - tt["y"]).abs() - (tt["market"] - tt["y"]).abs())
                d.index = pd.Index(tt["date"])
                lo, hi = boot_ci(d)
                row["blend_minus_market"] = d.mean()
                row["ci_lo"], row["ci_hi"] = lo, hi
        rows.append(row)
    mae = pd.DataFrame(rows)
    print("\nMean absolute error of the settlement value on the market sample (°F); '_c' = bias-corrected:")
    print(mae.round(3).to_string(index=False))

    # How good could any linear combination of these inputs be? Fit station effects and
    # weights on the test month itself, i.e. with hindsight, and compare with the market.
    core = [s for s in main_sources if s not in optional]
    bound_rows = []
    for label in DECISIONS:
        t = test[test["decision"] == label].dropna(subset=core + ["market"])
        X = np.column_stack([pd.get_dummies(t["station"]).to_numpy(float), t[core].to_numpy(float)])
        beta, *_ = np.linalg.lstsq(X, t["y"].to_numpy(float), rcond=None)
        bound_rows.append({"decision": label, "city_days": len(t), "sources": "+".join(core),
                           "hindsight_fit": float(np.abs(X @ beta - t["y"]).mean()),
                           "market": float((t["market"] - t["y"]).abs().mean())})
    bound = pd.DataFrame(bound_rows)
    print("\nHindsight bound: weights and station effects fitted on the test month itself:")
    print(bound.round(3).to_string(index=False))
    bound.to_csv(out / "hindsight_bound.csv", index=False)

    # Does the best public blend carry information the market price lacks? Same test as
    # success criterion 2, with a normal distribution around the blend whose width is the
    # blend's own walk-forward error.
    out_rows = []
    for label in DECISIONS:
        f = feats[(feats["decision"] == label)].dropna(subset=["blend"]).sort_values("date")
        resid = (f["y"] - f["blend"])
        days = np.array([d.toordinal() for d in f["date"]])
        sigma = {}
        for day in np.unique(days):
            past = days <= day - 2
            if past.sum() >= 100:
                sigma[day] = float(np.sqrt(np.mean(resid.to_numpy()[past] ** 2)))
        ev = pd.read_sql_query("SELECT event_id, group_index AS idx, lo, hi FROM markets", retro)
        ev["event_id"] = ev["event_id"].astype(str)
        p = panel[panel["decision"] == label].assign(event_id=lambda d: d["event_id"].astype(str)).merge(ev, on=["event_id", "idx"])
        p["date_d"] = pd.to_datetime(p["date"]).dt.date
        mu = f.set_index(["station", "date"])["blend"]
        Xs, wins, dates = [], [], []
        for (station, day), g in p.dropna(subset=["q"]).groupby(["station", "date_d"]):
            if (station, day) not in mu.index or day.toordinal() not in sigma or len(g) != 11 or g["win"].sum() != 1:
                continue
            g = g.sort_values("idx")
            m, s = mu.loc[(station, day)], sigma[day.toordinal()]
            upper = np.where(g["hi"].isna(), np.inf, g["hi"] + 0.5)
            lower = np.where(g["lo"].isna(), -np.inf, g["lo"] - 0.5)
            prob = stats.norm.cdf((upper - m) / s) - stats.norm.cdf((lower - m) / s)
            Xs.append(np.column_stack([np.log(g["q"].clip(lower=P_FLOOR)), np.log(np.clip(prob, P_FLOOR, None))]))
            wins.append(int(np.argmax(g["win"].to_numpy())))
            dates.append(str(day))
        if len(Xs) < 30:
            continue
        X, win = np.array(Xs), np.array(wins)
        _, cluster = np.unique(dates, return_inverse=True)
        beta, se, _, G = conditional_logit(X, win, cluster)
        r = np.arange(len(win))
        out_rows.append({"decision": label, "city_days": len(win), "dates": G, "beta_market": beta[0], "beta_blend": beta[1],
                         "se_blend": se[1], "p_one_sided": float(stats.t.sf(beta[1] / se[1], G - 1)),
                         "logscore_market": float(X[r, win, 0].mean()), "logscore_blend": float(X[r, win, 1].mean()),
                         "sigma": float(np.mean(list(sigma.values())))})
    c2 = pd.DataFrame(out_rows)
    print("\nCriterion 2 (retrospective) with the blend in place of M0:")
    print(c2.round(4).to_string(index=False))

    feats.to_csv(out / "forecast_features.csv.gz", index=False)
    mae.to_csv(out / "mae_on_market_sample.csv", index=False)
    c2.to_csv(out / "criterion2_blend.csv", index=False)


if __name__ == "__main__":
    main()
