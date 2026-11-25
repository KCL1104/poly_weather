import os

import pytest

from polyweather.util import files


def test_move_into_place_tries_again_while_the_file_is_held(tmp_path, monkeypatch):
    src, dst = tmp_path / "a.tmp", tmp_path / "a.parquet"
    src.write_bytes(b"data")
    real, calls = os.replace, []

    def held_twice(a, b):
        calls.append(1)
        if len(calls) <= 2:
            raise PermissionError(13, "The process cannot access the file because it is being used by another process")
        real(a, b)

    monkeypatch.setattr(files.os, "replace", held_twice)
    files.move_into_place(src, dst, wait_s=0)
    assert len(calls) == 3 and dst.read_bytes() == b"data" and not src.exists()


def test_move_into_place_gives_up_when_the_file_stays_held(tmp_path, monkeypatch):
    src = tmp_path / "a.tmp"
    src.write_bytes(b"data")

    def always_held(a, b):
        raise PermissionError(13, "held")

    monkeypatch.setattr(files.os, "replace", always_held)
    with pytest.raises(PermissionError):
        files.move_into_place(src, tmp_path / "a.parquet", attempts=3, wait_s=0)
    assert src.exists()  # nothing was lost
