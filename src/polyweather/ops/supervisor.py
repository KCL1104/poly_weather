"""Run the whole system in one process tree, for hosts without systemd.

``polyweather run-all`` starts the two long-running collectors (order-book recorder,
METAR poller) and runs every periodic job on its schedule, each as a child process
running the same CLI command a systemd unit would run. A collector that exits is
restarted with backoff; a job that is still running when it comes due again is
skipped. A stop request (SIGTERM from Docker or a redeploy, Ctrl+C in a terminal) is
passed on so the recorder can flush.

This is the way to run on a home computer (any operating system, with or without
Docker) and on platforms such as Railway, which give a service one container and one
volume and whose scheduled ("cron") services cannot share that volume.
"""

from __future__ import annotations

import datetime as dt
import json
import logging
import os
import subprocess
import sys
import time
from dataclasses import dataclass, field

import httpx

from ..settings import Settings
from ..util.stop import ask_to_stop, on_stop, spawn_options
from ..util.timeutil import UTC
from . import offload

log = logging.getLogger(__name__)

DISK_ALERT_SHARE = 0.85
TICK_SECONDS = 2.0


@dataclass
class Service:
    name: str
    args: list[str]
    process: subprocess.Popen | None = None
    restarts: int = 0
    next_start: float = 0.0
    started: float = 0.0


@dataclass
class Job:
    name: str
    args: list[str]
    every_s: int | None = None  # run when (seconds since midnight UTC - offset) crosses a multiple of this
    offset_s: int = 0
    daily_utc: tuple[str, ...] = ()  # or at these HH:MM times every day
    at_start: bool = False  # also run once when the supervisor starts
    process: subprocess.Popen | None = None
    last_slot: int | None = field(default=None)

    def slot(self, now: dt.datetime) -> int:
        """An integer that changes exactly when the job becomes due."""
        if self.every_s:
            return int((now.timestamp() - self.offset_s) // self.every_s)
        seconds = now.hour * 3600 + now.minute * 60 + now.second
        passed = sum(1 for t in self.daily_utc if seconds >= int(t[:2]) * 3600 + int(t[3:]) * 60)
        return now.toordinal() * 100 + passed


def default_jobs(bucket_configured: bool, ensemble_source: str = "openmeteo") -> list[Job]:
    jobs = [
        Job("catalog", ["catalog", "scan"], every_s=600),
        Job("nbm", ["nbm", "archive"], every_s=600, offset_s=180),
        Job("lamp", ["lamp", "archive"], every_s=600, offset_s=420),
        Job("ensemble", ["ensemble", "archive", "--source", ensemble_source], every_s=3600, offset_s=1200),
        Job("prices", ["prices", "archive"], daily_utc=("00:40",)),
        Job("check-settlement", ["check", "settlement"], daily_utc=("00:15", "12:15")),
        Job("check-gaps", ["check", "gaps"], daily_utc=("00:30",)),
        # Paper trading: the ledgers follow the recording a few minutes behind; the history
        # the nowcast needs is fetched at start and topped up once a day.
        Job("paper", ["paper", "run"], every_s=600, offset_s=540),
        Job("paper-prepare", ["paper", "prepare"], daily_utc=("03:40",), at_start=True),
        Job("paper-report", ["paper", "report"], daily_utc=("13:05",)),
    ]
    if bucket_configured:
        jobs.append(Job("offload", ["offload"], every_s=900, offset_s=60))  # also backs up the database
    else:
        jobs.append(Job("backup", ["backup"], daily_utc=("00:50",)))
    return jobs


class Supervisor:
    def __init__(self, settings: Settings, services: list[Service], jobs: list[Job], *, command: list[str] | None = None,
                 clock=lambda: dt.datetime.now(UTC)) -> None:
        self.settings = settings
        self.services = services
        self.jobs = jobs
        self.command = command or [sys.executable, "-m", "polyweather.cli"]
        self.clock = clock
        self.stopping = False
        self.webhook = os.environ.get("ALERT_WEBHOOK_URL") or None
        self._last_disk_alert = 0.0

    # ---- helpers ---------------------------------------------------------------

    def _spawn(self, args: list[str]) -> subprocess.Popen:
        return subprocess.Popen(self.command + args, **spawn_options())

    def alert(self, text: str) -> None:
        log.error("ALERT: %s", text)
        if not self.webhook:
            return
        try:
            httpx.post(self.webhook, json={"text": text, "content": text[:1900]}, timeout=15)
        except httpx.HTTPError as e:
            log.warning("alert webhook failed: %s", e)

    # ---- one scheduling pass ------------------------------------------------------

    def tick(self) -> None:
        now = self.clock()
        mono = time.monotonic()
        for svc in self.services:
            if svc.process is not None and svc.process.poll() is not None:
                code = svc.process.returncode
                ran = mono - svc.started
                svc.process = None
                if not self.stopping:
                    svc.restarts = 0 if ran > 300 else svc.restarts + 1
                    delay = min(60.0, 2.0 ** min(svc.restarts, 6))
                    svc.next_start = mono + delay
                    self.alert(f"polyweather {svc.name} exited with code {code} after {ran:.0f}s; restarting in {delay:.0f}s")
            if svc.process is None and not self.stopping and mono >= svc.next_start:
                svc.process = self._spawn(svc.args)
                svc.started = mono
                log.info("started %s (pid %d)", svc.name, svc.process.pid)
        for job in self.jobs:
            if job.process is not None and job.process.poll() is not None:
                code = job.process.returncode
                job.process = None
                # `check gaps` exits 2 when an archive is incomplete: that is a finding to report.
                if code != 0:
                    self.alert(f"polyweather job {job.name} failed with exit code {code}")
            slot = job.slot(now)
            if job.last_slot is None:
                job.last_slot = slot  # do not fire everything at start-up
                if job.at_start and not self.stopping:
                    job.process = self._spawn(job.args)
                    log.info("started job %s at start-up (pid %d)", job.name, job.process.pid)
                continue
            if slot != job.last_slot and not self.stopping:
                job.last_slot = slot
                if job.process is None:
                    job.process = self._spawn(job.args)
                    log.info("started job %s (pid %d)", job.name, job.process.pid)
                else:
                    log.warning("job %s is still running; skipped this run", job.name)
        if mono - self._last_disk_alert > 3600:
            usage = offload.disk_usage(self.settings)
            if usage["used_share"] >= DISK_ALERT_SHARE:
                self._last_disk_alert = mono
                self.alert(f"polyweather data disk is {usage['used_share']:.0%} full ({usage['used_gb']} of {usage['total_gb']} GB)")

    # ---- lifecycle ------------------------------------------------------------------

    def run_once(self, args: list[str], timeout: float) -> int:
        """Run a start-up command to completion."""
        log.info("start-up step: %s", " ".join(args))
        try:
            return subprocess.run(self.command + args, timeout=timeout).returncode
        except subprocess.TimeoutExpired:
            log.warning("start-up step timed out: %s", " ".join(args))
            return 1

    def stop(self, *_signal) -> None:
        self.stopping = True

    def shutdown(self, grace_s: float = 25.0) -> None:
        children = [p for p in [s.process for s in self.services] + [j.process for j in self.jobs] if p is not None]
        for p in children:
            if p.poll() is None:
                ask_to_stop(p)
        deadline = time.monotonic() + grace_s
        for p in children:
            try:
                p.wait(timeout=max(0.1, deadline - time.monotonic()))
            except subprocess.TimeoutExpired:
                p.kill()

    def run(self, duration_s: float | None = None) -> None:
        on_stop(self.stop)
        started = time.monotonic()
        try:
            while not self.stopping:
                self.tick()
                if duration_s is not None and time.monotonic() - started >= duration_s:
                    break
                time.sleep(TICK_SECONDS)
        finally:
            self.stopping = True
            self.shutdown()


def first_boot_steps(conn) -> list[tuple[list[str], float]]:
    """Commands to run before the collectors start, on an empty data directory only."""
    if conn.execute("SELECT COUNT(*) FROM events").fetchone()[0]:
        return []
    return [
        # Import mode: rule changes that already happened must not freeze anything today.
        (["catalog", "scan", "--history", "--closed-days", "3"], 900.0),
        # Recent METAR history, so the first settlement checks see complete days.
        (["metar", "backfill-awc", "--days", "4"], 1800.0),
    ]


def describe(jobs: list[Job]) -> str:
    return json.dumps([{"job": j.name, "every_s": j.every_s, "offset_s": j.offset_s, "daily_utc": j.daily_utc} for j in jobs])
