import datetime as dt
import gzip
import os
import sqlite3
import time

import boto3
import pytest
from moto import mock_aws

from polyweather.ops import gapcheck, offload
from polyweather.settings import Settings, load_settings

BUCKET = "polyweather-test"


@pytest.fixture
def s3(monkeypatch):
    for key in ("AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY"):
        monkeypatch.setenv(key, "test")
    monkeypatch.setenv("AWS_DEFAULT_REGION", "us-east-1")
    monkeypatch.delenv("AWS_ENDPOINT_URL", raising=False)
    with mock_aws():
        client = boto3.client("s3")
        client.create_bucket(Bucket=BUCKET)
        yield client


@pytest.fixture
def settings(tmp_path):
    s = Settings(data_dir=tmp_path, scope="us")
    s.ensure_dirs()
    return s


def _file(settings, rel: str, age_s: float, size: int = 100) -> None:
    path = settings.data_dir / rel
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(b"x" * size)
    t = time.time() - age_s
    os.utime(path, (t, t))


def _keys(s3) -> list[str]:
    return sorted(o["Key"] for o in s3.list_objects_v2(Bucket=BUCKET).get("Contents", []))


def test_bucket_config_from_environment(monkeypatch):
    for name in ("POLYWEATHER_S3_BUCKET", "AWS_S3_BUCKET_NAME", "BUCKET"):
        monkeypatch.delenv(name, raising=False)
    assert offload.BucketConfig.from_env() is None
    monkeypatch.setenv("AWS_S3_BUCKET_NAME", "from-railway")
    monkeypatch.setenv("POLYWEATHER_LOCAL_RETENTION_HOURS", "12")
    cfg = offload.BucketConfig.from_env()
    assert (cfg.bucket, cfg.prefix, cfg.retention_hours) == ("from-railway", "polyweather", 12.0)
    monkeypatch.setenv("POLYWEATHER_S3_BUCKET", "explicit")
    assert offload.BucketConfig.from_env().bucket == "explicit"


def test_data_dir_defaults_to_the_railway_volume(monkeypatch, tmp_path):
    monkeypatch.delenv("POLYWEATHER_DATA_DIR", raising=False)
    monkeypatch.setenv("RAILWAY_VOLUME_MOUNT_PATH", str(tmp_path / "vol"))
    assert load_settings().data_dir == (tmp_path / "vol").resolve()
    monkeypatch.setenv("POLYWEATHER_DATA_DIR", str(tmp_path / "explicit"))
    assert load_settings().data_dir == (tmp_path / "explicit").resolve()


def test_uploads_once_prunes_after_retention_and_backs_up_the_database(conn, settings, s3):
    cfg = offload.BucketConfig(BUCKET, "pw", retention_hours=24)
    old = "raw/book/date=2026-11-22/book-1-1-000001.parquet"      # two days old: upload, then prune
    recent = "raw/book/date=2026-11-24/book-2-1-000001.parquet"   # an hour old: upload, keep
    fresh = "raw/snapshots/date=2026-11-24/snap-3-1-000001.parquet"  # just written: leave alone
    _file(settings, old, age_s=48 * 3600, size=300)
    _file(settings, recent, age_s=3600, size=200)
    _file(settings, fresh, age_s=5)
    conn.execute("INSERT INTO kv(key, value) VALUES('marker', 'kept')")

    r = offload.run(conn, settings, cfg, s3)
    assert (r.uploaded, r.uploaded_bytes, r.pruned, r.pruned_bytes) == (2, 500, 1, 300)
    assert not (settings.data_dir / old).exists() and (settings.data_dir / recent).exists()
    assert not (settings.data_dir / old).parent.exists()  # the emptied date directory is removed
    assert (settings.data_dir / fresh).exists()
    day = dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%d")
    assert _keys(s3) == sorted([f"pw/{old}", f"pw/{recent}", "pw/db/polyweather-latest.sqlite.gz", f"pw/db/polyweather-{day}.sqlite.gz"])

    # The backup is a complete, readable database.
    body = s3.get_object(Bucket=BUCKET, Key="pw/db/polyweather-latest.sqlite.gz")["Body"].read()
    restored = settings.data_dir / "restored.sqlite"
    restored.write_bytes(gzip.decompress(body))
    assert sqlite3.connect(restored).execute("SELECT value FROM kv WHERE key='marker'").fetchone()[0] == "kept"

    # A second pass has nothing to do and does not repeat the daily backup.
    r2 = offload.run(conn, settings, cfg, s3)
    assert (r2.uploaded, r2.pruned, r2.db_backup) == (0, 0, None)
    rows = conn.execute("SELECT path, pruned_ms IS NOT NULL FROM offloaded ORDER BY path").fetchall()
    assert [tuple(x) for x in rows] == [(old, 1), (recent, 0)]


def test_gap_check_counts_files_that_moved_to_the_bucket(conn, settings, s3):
    cfg = offload.BucketConfig(BUCKET, "pw", retention_hours=1)
    day = dt.date(2026, 11, 22)
    for i in range(3):
        _file(settings, f"raw/book/date={day}/book-{i}-1-000001.parquet", age_s=48 * 3600)
    assert gapcheck.check_book(conn, settings, day).detail["raw_files"] == 3
    offload.run(conn, settings, cfg, s3, backup=False)
    assert not (settings.raw_dir / "book" / f"date={day}").exists()
    assert gapcheck.check_book(conn, settings, day).detail["raw_files"] == 3  # still accounted for


def test_cli_offload_without_a_bucket_is_a_no_op(tmp_path, monkeypatch, capsys):
    from polyweather import cli

    monkeypatch.setenv("POLYWEATHER_DATA_DIR", str(tmp_path))
    for name in ("POLYWEATHER_S3_BUCKET", "AWS_S3_BUCKET_NAME", "BUCKET"):
        monkeypatch.delenv(name, raising=False)
    assert cli.main(["offload"]) == 0
    assert "no bucket configured" in capsys.readouterr().out
