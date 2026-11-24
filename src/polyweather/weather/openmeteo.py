"""Open-Meteo Ensemble API source (the primary ensemble feed during paper research).

One request returns hourly 2 m temperature for every member of a model at several
locations. The free tier is for non-commercial use, under 10,000 calls a day, data
CC-BY 4.0; individual members can be looked back only about 3 days, so this must be
archived as it is published. Limits are counted per client IP address: on a shared
egress address the daily limit can already be exhausted by other users.

The API does not state the model run a response came from. ``run`` is therefore left
empty and the fetch time is what orders the versions; identical content is stored once.
"""

from __future__ import annotations

import datetime as dt
import logging
import re
from typing import Sequence

import httpx

from ..stations import Station
from ..util.http import request_with_retry
from ..util.timeutil import UTC
from .ensemble import EnsembleSample

log = logging.getLogger(__name__)

URL = "https://ensemble-api.open-meteo.com/v1/ensemble"
DEFAULT_MODELS = ("ecmwf_ifs025",)
_MEMBER = re.compile(r"^temperature_2m(?:_member(\d+))?$")


def parse_response(payload, stations: Sequence[Station], model: str) -> list[EnsembleSample]:
    """Samples from an API response (one object per location, or a single object)."""
    locations = payload if isinstance(payload, list) else [payload]
    if len(locations) != len(stations):
        raise ValueError(f"expected {len(stations)} locations, got {len(locations)}")
    out: list[EnsembleSample] = []
    for station, loc in zip(stations, locations):
        hourly = loc.get("hourly") or {}
        unit = (loc.get("hourly_units") or {}).get("temperature_2m", "°C")
        times = [dt.datetime.fromisoformat(t).replace(tzinfo=UTC) for t in hourly.get("time") or []]
        for key, values in hourly.items():
            m = _MEMBER.match(key)
            if not m:
                continue
            member = int(m.group(1)) if m.group(1) else 0
            for t, v in zip(times, values):
                if v is None:
                    continue
                c = (float(v) - 32.0) * 5.0 / 9.0 if "F" in unit else float(v)
                out.append(EnsembleSample(station.icao, model, None, t, member, "t2m", c))
    return out


class OpenMeteoSource:
    name = "openmeteo"

    def __init__(self, client: httpx.Client, models: Sequence[str] = DEFAULT_MODELS, forecast_days: int = 4) -> None:
        self.client = client
        self.models = tuple(models)
        self.forecast_days = forecast_days

    def fetch(self, stations: Sequence[Station]) -> list[EnsembleSample]:
        out: list[EnsembleSample] = []
        for model in self.models:
            params = {
                "latitude": ",".join(f"{s.lat:.4f}" for s in stations),
                "longitude": ",".join(f"{s.lon:.4f}" for s in stations),
                "hourly": "temperature_2m",
                "models": model,
                "timezone": "GMT",
                "forecast_days": self.forecast_days,
            }
            r = request_with_retry(self.client, "GET", URL, params=params, retries=2)
            out += parse_response(r.json(), stations, model)
        return out
