import json
from pathlib import Path

import pytest

from polyweather.storage import db

FIXTURES = Path(__file__).parent / "fixtures"


@pytest.fixture
def fixtures() -> Path:
    return FIXTURES


@pytest.fixture
def conn(tmp_path):
    c = db.connect(tmp_path / "t.sqlite")
    yield c
    c.close()


@pytest.fixture
def nyc_event() -> dict:
    return json.loads((FIXTURES / "gamma_event_nyc_2026-11-09.json").read_text("utf-8"))
