"""SQLite store for the catalog, weather archives and operational logs.

High-volume order-book data lives in Parquet (see parquet_sink); everything that is
small and benefits from keys and updates lives here. Several processes write to the
same file, so the database runs in WAL mode with a generous busy timeout.
"""

from __future__ import annotations

import sqlite3
from pathlib import Path

SCHEMA = """
CREATE TABLE IF NOT EXISTS events (
    event_id          TEXT PRIMARY KEY,
    slug              TEXT NOT NULL UNIQUE,
    title             TEXT,
    series_slug       TEXT,
    event_date        TEXT,
    start_date        TEXT,
    end_date          TEXT,
    closed            INTEGER NOT NULL DEFAULT 0,
    closed_time       TEXT,
    resolution_source TEXT,
    rules_text        TEXT,
    rules_hash        TEXT,
    neg_risk          INTEGER,
    volume            REAL,
    first_seen_ms     INTEGER NOT NULL,
    last_seen_ms      INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS events_series ON events(series_slug, event_date);
CREATE INDEX IF NOT EXISTS events_closed ON events(closed, event_date);

CREATE TABLE IF NOT EXISTS markets (
    market_id        TEXT PRIMARY KEY,
    event_id         TEXT NOT NULL REFERENCES events(event_id),
    slug             TEXT,
    question         TEXT,
    group_title      TEXT,
    group_index      INTEGER,
    condition_id     TEXT,
    yes_token        TEXT,
    no_token         TEXT,
    lo               INTEGER,           -- inclusive lower bound in whole degrees; NULL = open
    hi               INTEGER,           -- inclusive upper bound in whole degrees; NULL = open
    unit             TEXT,              -- 'F' or 'C'
    bracket_ok       INTEGER NOT NULL DEFAULT 0,
    fee_rate         REAL,
    fee_exponent     REAL,
    fee_taker_only   INTEGER,
    fee_schedule     TEXT,
    tick_size        REAL,
    min_order_size   REAL,
    neg_risk         INTEGER,
    closed           INTEGER NOT NULL DEFAULT 0,
    closed_time      TEXT,
    winner           TEXT,              -- 'YES' / 'NO' once resolved
    outcome_prices   TEXT,
    volume           REAL,
    first_seen_ms    INTEGER NOT NULL,
    last_seen_ms     INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS markets_event ON markets(event_id, group_index);
CREATE INDEX IF NOT EXISTS markets_yes ON markets(yes_token);
CREATE INDEX IF NOT EXISTS markets_no ON markets(no_token);

CREATE TABLE IF NOT EXISTS resolution_specs (
    event_id         TEXT PRIMARY KEY REFERENCES events(event_id),
    status           TEXT NOT NULL,     -- 'ok' or 'unparsed'
    reasons          TEXT,              -- JSON list, empty when ok
    template_id      TEXT,
    station          TEXT,
    measure          TEXT,              -- 'high' or 'low'
    unit             TEXT,
    obs_date         TEXT,
    tz               TEXT,
    source_url       TEXT,
    window_start_utc TEXT,
    window_end_utc   TEXT,
    spec_json        TEXT,
    parser_version   TEXT,
    parsed_ms        INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS specs_station ON resolution_specs(station, obs_date);

CREATE TABLE IF NOT EXISTS change_log (
    id        INTEGER PRIMARY KEY AUTOINCREMENT,
    ts_ms     INTEGER NOT NULL,
    event_id  TEXT,
    station   TEXT,
    field     TEXT NOT NULL,
    old_value TEXT,
    new_value TEXT
);

CREATE TABLE IF NOT EXISTS station_status (
    station  TEXT PRIMARY KEY,
    state    TEXT NOT NULL,             -- 'active' or 'frozen'
    reason   TEXT,
    since_ms INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS pause_log (
    id      INTEGER PRIMARY KEY AUTOINCREMENT,
    ts_ms   INTEGER NOT NULL,
    scope   TEXT NOT NULL,              -- 'station:KLGA', 'all', 'ledger:H1-M0', ...
    action  TEXT NOT NULL,              -- 'pause' or 'resume'
    trigger TEXT NOT NULL,
    detail  TEXT
);

CREATE TABLE IF NOT EXISTS gaps (
    id       INTEGER PRIMARY KEY AUTOINCREMENT,
    source   TEXT NOT NULL,             -- 'book', 'metar', 'nbm', ...
    key      TEXT,
    start_ms INTEGER NOT NULL,
    end_ms   INTEGER,                   -- NULL while the gap is still open
    reason   TEXT
);
CREATE INDEX IF NOT EXISTS gaps_source ON gaps(source, start_ms);

CREATE TABLE IF NOT EXISTS metar_obs (
    station       TEXT NOT NULL,
    obs_time      TEXT NOT NULL,        -- UTC ISO
    raw_text      TEXT NOT NULL,
    metar_type    TEXT,                 -- 'METAR' / 'SPECI' as labelled by the source
    temp_tenths_c INTEGER,              -- value the resolution rule would take from this report
    temp_source   TEXT,                 -- 'tgroup' / 'body'
    source        TEXT NOT NULL,        -- 'awc_cache' / 'awc_api' / 'iem'
    first_seen_ms INTEGER NOT NULL,     -- when this system first saw this exact text
    receipt_time  TEXT,                 -- when AWC says it received the report
    PRIMARY KEY (station, obs_time, raw_text)
);
CREATE INDEX IF NOT EXISTS metar_station_time ON metar_obs(station, obs_time);

CREATE TABLE IF NOT EXISTS guidance_blocks (
    product      TEXT NOT NULL,         -- 'nbp', 'nbs', 'nbh', 'nbe', 'lav'
    station      TEXT NOT NULL,
    cycle        TEXT NOT NULL,         -- model cycle, UTC ISO
    raw_block    TEXT NOT NULL,
    source       TEXT NOT NULL,         -- 'nomads' / 's3' / ...
    published    TEXT,                  -- Last-Modified reported by the source
    fetched_ms   INTEGER NOT NULL,
    PRIMARY KEY (product, station, cycle)
);

CREATE TABLE IF NOT EXISTS nbm_txn (
    station   TEXT NOT NULL,
    cycle     TEXT NOT NULL,
    valid_utc TEXT NOT NULL,
    fhr       INTEGER NOT NULL,
    kind      TEXT NOT NULL,            -- 'max' (column valid 00Z) or 'min' (12Z)
    mean      INTEGER,
    sd        INTEGER,
    p10       INTEGER,
    p25       INTEGER,
    p50       INTEGER,
    p75       INTEGER,
    p90       INTEGER,
    PRIMARY KEY (station, cycle, valid_utc)
);
CREATE INDEX IF NOT EXISTS nbm_txn_valid ON nbm_txn(station, valid_utc, kind);

CREATE TABLE IF NOT EXISTS lamp_hourly (
    station   TEXT NOT NULL,
    cycle     TEXT NOT NULL,
    valid_utc TEXT NOT NULL,
    fhr       INTEGER NOT NULL,
    tmp_f     INTEGER,
    dpt_f     INTEGER,
    seen_ms   INTEGER,                  -- when this row was stored; NULL in archives older than the column
    PRIMARY KEY (station, cycle, valid_utc)
);
CREATE INDEX IF NOT EXISTS lamp_valid ON lamp_hourly(station, valid_utc);

CREATE TABLE IF NOT EXISTS forecast_fetches (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    source       TEXT NOT NULL,         -- 'openmeteo' / 'ecmwf_open'
    model        TEXT NOT NULL,
    run_init     TEXT,                  -- model run time when the source states it
    fetched_ms   INTEGER NOT NULL,
    content_hash TEXT NOT NULL,
    path         TEXT NOT NULL,         -- file under raw/, relative to the data dir
    n_stations   INTEGER,
    UNIQUE (source, model, content_hash)
);

CREATE TABLE IF NOT EXISTS settlement_checks (
    event_id              TEXT PRIMARY KEY REFERENCES events(event_id),
    station               TEXT,
    obs_date              TEXT,
    measure               TEXT,
    n_counted             INTEGER,
    value_tenths_c        INTEGER,
    value_unit            INTEGER,      -- whole degrees in the market's unit
    reconstructed_bracket TEXT,
    actual_bracket        TEXT,
    is_match              INTEGER,
    detail                TEXT,
    checked_ms            INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS daily_checks (
    date    TEXT NOT NULL,
    name    TEXT NOT NULL,
    ok      INTEGER NOT NULL,
    detail  TEXT,
    ts_ms   INTEGER NOT NULL,
    PRIMARY KEY (date, name)
);

CREATE TABLE IF NOT EXISTS job_runs (
    id     INTEGER PRIMARY KEY AUTOINCREMENT,
    job    TEXT NOT NULL,
    ts_ms  INTEGER NOT NULL,
    ok     INTEGER NOT NULL,
    detail TEXT
);
CREATE INDEX IF NOT EXISTS job_runs_job ON job_runs(job, ts_ms);

CREATE TABLE IF NOT EXISTS offloaded (
    path        TEXT PRIMARY KEY,       -- relative to the data dir, e.g. raw/book/date=.../book-....parquet
    size        INTEGER NOT NULL,
    uploaded_ms INTEGER NOT NULL,
    pruned_ms   INTEGER                 -- when the local copy was deleted
);

CREATE TABLE IF NOT EXISTS kv (
    key   TEXT PRIMARY KEY,
    value TEXT
);

-- Paper trading. `run` is 'live' for the ledger of record and 'replay:<obs date>' for the
-- daily recomputation from scratch that the ledger is reconciled against.
CREATE TABLE IF NOT EXISTS paper_signals (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    run           TEXT NOT NULL,
    ledger        TEXT NOT NULL,          -- 'H2a' / 'H2b'
    ts_ms         INTEGER NOT NULL,       -- when the system knew what triggered the signal
    event_id      TEXT NOT NULL,
    market_id     TEXT NOT NULL,
    station       TEXT NOT NULL,
    obs_date      TEXT NOT NULL,
    side          TEXT NOT NULL,          -- the token that would be bought: 'YES' / 'NO'
    model_prob    REAL,                   -- probability that this token pays 1
    market_mid    REAL,                   -- midpoint of this token's book at the signal
    best_bid      REAL,
    best_bid_size REAL,
    best_ask      REAL,
    best_ask_size REAL,
    edge          REAL,                   -- model_prob minus the cost of a share at the best ask, fee included
    decision      TEXT NOT NULL,          -- 'enter' / 'skip'
    skip_reason   TEXT,                   -- below_threshold / over_cap / no_liquidity / frozen / data_gap / not_best
    model_version TEXT NOT NULL,
    source_batch  TEXT,                   -- the report or forecast cycle behind the signal
    detail        TEXT,                   -- JSON
    UNIQUE (run, ledger, ts_ms, market_id, side)
);
CREATE INDEX IF NOT EXISTS paper_signals_day ON paper_signals(run, ledger, obs_date);

CREATE TABLE IF NOT EXISTS paper_fills (
    id         INTEGER PRIMARY KEY AUTOINCREMENT,
    signal_id  INTEGER NOT NULL REFERENCES paper_signals(id),
    run        TEXT NOT NULL,
    ledger     TEXT NOT NULL,
    variant    TEXT NOT NULL,             -- 'conservative' (the one that counts) / 'optimistic'
    fill_ms    INTEGER NOT NULL,
    event_id   TEXT NOT NULL,
    market_id  TEXT NOT NULL,
    station    TEXT NOT NULL,
    obs_date   TEXT NOT NULL,
    side       TEXT NOT NULL,
    shares     REAL NOT NULL,
    avg_price  REAL NOT NULL,
    notional   REAL NOT NULL,             -- sum of price x shares
    fee        REAL NOT NULL,
    levels     TEXT,                      -- JSON [[price, shares], ...]
    payout     REAL,                      -- filled in when the market resolves: shares if the token won, else 0
    settled_ms INTEGER,
    UNIQUE (signal_id, variant)
);
CREATE INDEX IF NOT EXISTS paper_fills_day ON paper_fills(run, ledger, variant, obs_date);

-- H2a: what a ruled-out bracket's NO side cost at fixed delays after the report was seen.
CREATE TABLE IF NOT EXISTS paper_measurements (
    run          TEXT NOT NULL,
    ledger       TEXT NOT NULL,
    event_id     TEXT NOT NULL,
    market_id    TEXT NOT NULL,
    station      TEXT NOT NULL,
    obs_date     TEXT NOT NULL,
    trigger      TEXT NOT NULL,           -- observation time of the report that ruled the bracket out
    seen_ms      INTEGER NOT NULL,        -- when this system first saw that report
    offset_s     INTEGER NOT NULL,        -- seconds after seen_ms
    at_ms        INTEGER NOT NULL,
    status       TEXT NOT NULL,           -- 'pending' / 'ok' / 'gap'
    best_ask     REAL,
    ask_size     REAL,
    fillable_usd REAL,                    -- a quarter of every sell order priced below 1
    edge         REAL,                    -- 1 minus the cost of a share at the best ask, fee included
    PRIMARY KEY (run, ledger, market_id, trigger, offset_s)
);
"""


def connect(path: Path | str) -> sqlite3.Connection:
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(path, timeout=60.0, isolation_level=None)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    conn.execute("PRAGMA busy_timeout=60000")
    conn.execute("PRAGMA foreign_keys=ON")
    conn.executescript(SCHEMA)
    _migrate(conn)
    return conn


def _migrate(conn: sqlite3.Connection) -> None:
    """Bring a database made by an earlier version up to the current tables."""
    if "seen_ms" not in {r[1] for r in conn.execute("PRAGMA table_info(lamp_hourly)")}:
        conn.execute("ALTER TABLE lamp_hourly ADD COLUMN seen_ms INTEGER")


def log_job(conn: sqlite3.Connection, job: str, ok: bool, detail: str = "") -> None:
    import time

    conn.execute(
        "INSERT INTO job_runs(job, ts_ms, ok, detail) VALUES(?,?,?,?)",
        (job, time.time_ns() // 1_000_000, int(ok), detail[:2000]),
    )


def kv_get(conn: sqlite3.Connection, key: str, default: str | None = None) -> str | None:
    row = conn.execute("SELECT value FROM kv WHERE key=?", (key,)).fetchone()
    return row["value"] if row else default


def kv_set(conn: sqlite3.Connection, key: str, value: str) -> None:
    conn.execute(
        "INSERT INTO kv(key, value) VALUES(?, ?) ON CONFLICT(key) DO UPDATE SET value=excluded.value",
        (key, value),
    )
