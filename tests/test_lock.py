import pytest

from polyweather.util import lock


def test_a_second_holder_is_turned_away_until_the_first_lets_go(tmp_path):
    assert lock.hold(tmp_path, "recorder") is True
    assert lock.hold(tmp_path, "recorder") is True          # the same process asking again already has it
    assert lock.hold(tmp_path, "paper") is True             # other names are other locks

    # a second holder: its own handle on the same file, as another process would have
    handle = open(tmp_path / "recorder.lock", "a+")
    with pytest.raises(OSError):
        lock._lock(handle)
    handle.close()

    lock.release(tmp_path, "recorder")
    handle = open(tmp_path / "recorder.lock", "a+")
    lock._lock(handle)                                       # free again once the holder is gone
    handle.close()
    lock.release(tmp_path, "paper")


def test_another_process_holding_the_lock_means_busy(tmp_path):
    import subprocess
    import sys

    code = ("import sys, time; from pathlib import Path; from polyweather.util import lock; "
            "lock.hold(Path(sys.argv[1]), 'run-all'); print('held', flush=True); time.sleep(30)")
    other = subprocess.Popen([sys.executable, "-c", code, str(tmp_path)], stdout=subprocess.PIPE, text=True)
    try:
        assert other.stdout.readline().strip() == "held"
        with pytest.raises(lock.Busy):
            lock.hold(tmp_path, "run-all")
    finally:
        other.kill()
        other.wait()
    assert lock.hold(tmp_path, "run-all") is True           # the lock died with its holder
    lock.release(tmp_path, "run-all")
