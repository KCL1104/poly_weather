import datetime as dt
import sys
import time

import pytest

from polyweather.ops import supervisor
from polyweather.ops.supervisor import Job, Service, Supervisor
from polyweather.settings import Settings

UTC = dt.timezone.utc


def test_job_slots_change_exactly_when_due():
    every = Job("catalog", [], every_s=600)
    t0 = dt.datetime(2026, 11, 24, 12, 0, 1, tzinfo=UTC)
    assert every.slot(t0) == every.slot(t0 + dt.timedelta(seconds=598))
    assert every.slot(t0 + dt.timedelta(seconds=599)) == every.slot(t0) + 1

    offset = Job("nbm", [], every_s=600, offset_s=180)
    base = dt.datetime(2026, 11, 24, 12, 2, 59, tzinfo=UTC)
    assert offset.slot(base + dt.timedelta(seconds=1)) == offset.slot(base) + 1  # fires at :03, :13, ...

    daily = Job("check-settlement", [], daily_utc=("00:15", "12:15"))
    day = dt.datetime(2026, 11, 24, tzinfo=UTC)
    slots = [daily.slot(day + dt.timedelta(hours=h, minutes=m)) for h, m in ((0, 14), (0, 15), (12, 14), (12, 15), (23, 59))]
    assert slots[0] != slots[1] and slots[1] == slots[2] and slots[2] != slots[3] and slots[3] == slots[4]
    assert daily.slot(day + dt.timedelta(days=1)) != slots[4]  # midnight starts a new day's count


def test_default_jobs_add_offload_only_with_a_bucket():
    local = {j.name: j for j in supervisor.default_jobs(False)}
    assert "offload" not in local and local["backup"].daily_utc == ("00:50",)
    jobs = {j.name: j for j in supervisor.default_jobs(True, ensemble_source="ecmwf_open")}
    assert jobs["offload"].every_s == 900 and jobs["ensemble"].args[-1] == "ecmwf_open"
    assert jobs["check-gaps"].daily_utc == ("00:30",)
    assert "backup" not in jobs  # the bucket upload already includes the database


@pytest.fixture
def harness(tmp_path):
    """A supervisor whose children are a tiny script: `sleep N` or `exit N`."""
    script = tmp_path / "child.py"
    script.write_text(
        "import sys, time\n"
        "kind, value = sys.argv[1], float(sys.argv[2])\n"
        "open(sys.argv[3], 'a').write(kind + '\\n')\n"
        "time.sleep(value) if kind == 'sleep' else sys.exit(int(value))\n"
    )
    settings = Settings(data_dir=tmp_path, scope="us")
    settings.ensure_dirs()
    now = [dt.datetime(2026, 11, 24, 12, 0, 0, tzinfo=UTC)]
    alerts: list[str] = []
    log = tmp_path / "runs.log"

    def make(services, jobs):
        sup = Supervisor(settings, services, jobs, command=[sys.executable, str(script)], clock=lambda: now[0])
        sup.alert = alerts.append
        return sup

    return make, now, alerts, log


def _wait(pred, timeout=5.0):
    end = time.time() + timeout
    while time.time() < end:
        if pred():
            return True
        time.sleep(0.02)
    return False


def test_jobs_run_when_due_and_failures_alert(harness):
    make, now, alerts, log = harness
    ok = Job("ok", ["exit", "0", str(log)], every_s=600)
    bad = Job("bad", ["exit", "3", str(log)], every_s=600)
    sup = make([], [ok, bad])
    sup.tick()
    assert ok.process is None and not log.exists()  # nothing fires at start-up
    now[0] += dt.timedelta(seconds=600)
    sup.tick()
    assert ok.process is not None and bad.process is not None
    assert _wait(lambda: ok.process.poll() is not None and bad.process.poll() is not None)
    sup.tick()
    assert alerts == ["polyweather job bad failed with exit code 3"]
    assert log.read_text().count("exit") == 2
    sup.tick()  # same slot: no second run
    assert log.read_text().count("exit") == 2


def test_a_job_still_running_is_skipped_not_doubled(harness):
    make, now, alerts, log = harness
    slow = Job("slow", ["sleep", "30", str(log)], every_s=600)
    sup = make([], [slow])
    sup.tick()
    now[0] += dt.timedelta(seconds=600)
    sup.tick()
    first = slow.process
    assert _wait(log.exists)
    now[0] += dt.timedelta(seconds=600)
    sup.tick()
    assert slow.process is first and log.read_text().count("sleep") == 1
    sup.stopping = True
    sup.shutdown(grace_s=5)
    assert first.poll() is not None and alerts == []


def test_a_collector_that_exits_is_restarted_with_an_alert(harness, monkeypatch):
    make, now, alerts, log = harness
    svc = Service("recorder", ["exit", "1", str(log)])
    sup = make([svc], [])
    sup.tick()
    assert svc.process is not None
    assert _wait(lambda: svc.process.poll() is not None)
    sup.tick()
    assert svc.process is None and svc.restarts == 1
    assert "recorder exited with code 1" in alerts[0] and "restarting in 2s" in alerts[0]
    svc.next_start = 0.0  # skip the backoff wait
    sup.tick()
    assert svc.process is not None and _wait(lambda: log.read_text().count("exit") == 2)
    sup.stopping = True
    sup.shutdown(grace_s=5)


def test_shutdown_terminates_children_and_stops_restarting(harness):
    make, now, alerts, log = harness
    svc = Service("recorder", ["sleep", "60", str(log)])
    sup = make([svc], [])
    sup.tick()
    assert _wait(log.exists)
    sup.stop()
    sup.shutdown(grace_s=5)
    assert svc.process.poll() is not None
    sup.tick()
    assert alerts == []  # a child that we stopped is not reported as a crash


def test_first_boot_steps_only_on_an_empty_database(conn, nyc_event):
    from polyweather.catalog import scanner

    steps = supervisor.first_boot_steps(conn)
    assert [s[0][:2] for s in steps] == [["catalog", "scan"], ["metar", "backfill-awc"]]
    assert "--history" in steps[0][0]
    scanner.ingest(conn, [nyc_event])
    assert supervisor.first_boot_steps(conn) == []
