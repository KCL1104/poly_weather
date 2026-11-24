"""Copy finished archive files to an S3-compatible bucket and prune old local copies.

On hosts where the disk is small or tied to one container (Railway: one volume per
service, 5 GB on the Hobby plan), the order-book archive cannot live on the volume.
Every Parquet file is immutable once written, so each one is uploaded once, recorded
in ``offloaded``, and its local copy deleted after ``retention_hours``. The SQLite
database stays on the volume and a consistent copy is uploaded once a day.

Configuration comes from the usual S3 environment variables (the names Railway's
bucket preset for the AWS SDK sets): AWS_ENDPOINT_URL, AWS_ACCESS_KEY_ID,
AWS_SECRET_ACCESS_KEY, AWS_DEFAULT_REGION and AWS_S3_BUCKET_NAME. Optional:
POLYWEATHER_S3_PREFIX (default "polyweather"), POLYWEATHER_LOCAL_RETENTION_HOURS
(default 36) and POLYWEATHER_S3_ADDRESSING_STYLE (virtual, path or auto; default
"auto", which finds the style the bucket answers to and remembers it).
"""

from __future__ import annotations

import gzip
import logging
import os
import shutil
import sqlite3
import tempfile
import time
from dataclasses import dataclass
from pathlib import Path

from ..settings import Settings
from ..util.timeutil import ms_to_iso, now_ms

log = logging.getLogger(__name__)

SETTLE_SECONDS = 120  # a file this old is finished: writers rename into place when done
DB_BACKUP_EVERY_MS = 24 * 3600 * 1000


@dataclass(frozen=True)
class BucketConfig:
    bucket: str
    prefix: str
    retention_hours: float

    @staticmethod
    def from_env() -> "BucketConfig | None":
        bucket = os.environ.get("POLYWEATHER_S3_BUCKET") or os.environ.get("AWS_S3_BUCKET_NAME") or os.environ.get("BUCKET")
        if not bucket:
            return None
        return BucketConfig(
            bucket=bucket,
            prefix=os.environ.get("POLYWEATHER_S3_PREFIX", "polyweather").strip("/"),
            retention_hours=float(os.environ.get("POLYWEATHER_LOCAL_RETENTION_HOURS", "36")),
        )


def _client(style: str):
    import boto3  # optional dependency: pip install polyweather[s3]
    from botocore.config import Config

    return boto3.client(
        "s3",
        config=Config(
            s3={"addressing_style": style},
            request_checksum_calculation="when_required",
            response_checksum_validation="when_required",
            retries={"max_attempts": 5, "mode": "standard"},
        ),
    )


def make_s3_client(conn: sqlite3.Connection | None = None, bucket: str | None = None, *, factory=_client):
    """S3 client from the environment, tuned for S3-compatible stores.

    Checksums are sent only when an operation requires them, because not every
    S3-compatible store accepts the newer default checksum headers.

    The URL style follows POLYWEATHER_S3_ADDRESSING_STYLE when it says virtual or
    path. Otherwise, with a custom endpoint, the style is found by asking for the
    bucket: virtual-hosted first (what Railway's buckets use), then path-style (older
    buckets, MinIO). The SDK on its own would pick path-style for any custom endpoint.
    The style that answered is remembered in ``kv`` and tried first next time.
    """
    style = os.environ.get("POLYWEATHER_S3_ADDRESSING_STYLE", "auto").strip().lower()
    if style in ("virtual", "path"):
        return factory(style)
    if not os.environ.get("AWS_ENDPOINT_URL") or not bucket:
        return factory("auto")

    remembered = None
    if conn is not None:
        row = conn.execute("SELECT value FROM kv WHERE key='s3_addressing_style'").fetchone()
        remembered = row[0] if row else None
    order = ["virtual", "path"]
    if remembered in order:
        order.sort(key=lambda candidate: candidate != remembered)
    for candidate in order:
        client = factory(candidate)
        try:
            client.head_bucket(Bucket=bucket)
        except Exception as exc:  # wrong style shows up as DNS, 403, 404 or redirect errors
            log.warning("bucket did not answer with %s-style URLs: %s", candidate, exc)
            continue
        if conn is not None and candidate != remembered:
            conn.execute(
                "INSERT INTO kv(key, value) VALUES('s3_addressing_style', ?) "
                "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
                (candidate,),
            )
        return client
    log.warning("bucket answered to neither URL style; uploading with %s-style URLs anyway", order[0])
    return factory(order[0])


@dataclass
class OffloadResult:
    uploaded: int = 0
    uploaded_bytes: int = 0
    pruned: int = 0
    pruned_bytes: int = 0
    db_backup: str | None = None


def _key(cfg: BucketConfig, rel: str) -> str:
    return f"{cfg.prefix}/{rel}" if cfg.prefix else rel


def upload_new_files(conn: sqlite3.Connection, settings: Settings, cfg: BucketConfig, s3, result: OffloadResult) -> None:
    done = {r[0] for r in conn.execute("SELECT path FROM offloaded")}
    cutoff = time.time() - SETTLE_SECONDS
    for path in sorted(settings.raw_dir.rglob("*.parquet")):
        rel = path.relative_to(settings.data_dir).as_posix()
        if rel in done or path.stat().st_mtime > cutoff:
            continue
        size = path.stat().st_size
        s3.upload_file(str(path), cfg.bucket, _key(cfg, rel))
        conn.execute("INSERT OR REPLACE INTO offloaded(path, size, uploaded_ms) VALUES(?,?,?)", (rel, size, now_ms()))
        result.uploaded += 1
        result.uploaded_bytes += size


def prune_local(conn: sqlite3.Connection, settings: Settings, cfg: BucketConfig, result: OffloadResult) -> None:
    """Delete local copies that are safely in the bucket and older than the retention."""
    cutoff = time.time() - cfg.retention_hours * 3600
    rows = conn.execute("SELECT path, size FROM offloaded WHERE pruned_ms IS NULL").fetchall()
    for rel, size in rows:
        path = settings.data_dir / rel
        if not path.exists():
            conn.execute("UPDATE offloaded SET pruned_ms=? WHERE path=?", (now_ms(), rel))
            continue
        if path.stat().st_mtime < cutoff:
            path.unlink()
            conn.execute("UPDATE offloaded SET pruned_ms=? WHERE path=?", (now_ms(), rel))
            result.pruned += 1
            result.pruned_bytes += size
    for directory in sorted(settings.raw_dir.rglob("date=*"), reverse=True):
        if directory.is_dir() and not any(directory.iterdir()):
            directory.rmdir()


def backup_database(conn: sqlite3.Connection, settings: Settings, cfg: BucketConfig, s3, result: OffloadResult, *, force: bool = False) -> None:
    """Upload a consistent, compressed copy of the database, at most once a day."""
    row = conn.execute("SELECT value FROM kv WHERE key='db_backup_ms'").fetchone()
    if not force and row and now_ms() - int(row[0]) < DB_BACKUP_EVERY_MS:
        return
    with tempfile.TemporaryDirectory() as tmp:
        copy = Path(tmp) / "polyweather.sqlite"
        target = sqlite3.connect(copy)
        try:
            conn.backup(target)  # safe while other processes are writing
        finally:
            target.close()
        packed = Path(tmp) / "polyweather.sqlite.gz"
        with open(copy, "rb") as src, gzip.open(packed, "wb", compresslevel=6) as dst:
            shutil.copyfileobj(src, dst)
        day = ms_to_iso(now_ms())[:10]
        for name in (f"db/polyweather-{day}.sqlite.gz", "db/polyweather-latest.sqlite.gz"):
            s3.upload_file(str(packed), cfg.bucket, _key(cfg, name))
        result.db_backup = f"db/polyweather-{day}.sqlite.gz"
    conn.execute(
        "INSERT INTO kv(key, value) VALUES('db_backup_ms', ?) ON CONFLICT(key) DO UPDATE SET value=excluded.value",
        (str(now_ms()),),
    )


def run(conn: sqlite3.Connection, settings: Settings, cfg: BucketConfig, s3=None, *, backup: bool | None = None) -> OffloadResult:
    """One pass: upload what is new, prune what is old, back up the database if due."""
    s3 = s3 or make_s3_client(conn, cfg.bucket)
    result = OffloadResult()
    upload_new_files(conn, settings, cfg, s3, result)
    prune_local(conn, settings, cfg, result)
    if backup is not False:
        backup_database(conn, settings, cfg, s3, result, force=bool(backup))
    return result


def disk_usage(settings: Settings) -> dict:
    usage = shutil.disk_usage(settings.data_dir)
    return {"total_gb": round(usage.total / 1e9, 2), "used_gb": round(usage.used / 1e9, 2),
            "used_share": round(usage.used / usage.total, 3)}
