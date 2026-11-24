"""Station registry (see stations.toml)."""

from __future__ import annotations

import tomllib
from dataclasses import dataclass
from functools import lru_cache
from importlib import resources
from zoneinfo import ZoneInfo


@dataclass(frozen=True)
class Station:
    icao: str
    city: str
    tz: str
    lat: float
    lon: float
    elev_m: float | None
    name: str
    country: str
    us: bool

    @property
    def zone(self) -> ZoneInfo:
        return ZoneInfo(self.tz)

    @property
    def iem_id(self) -> str:
        """IEM ASOS identifier: 3-letter for CONUS K-stations, ICAO otherwise."""
        return self.icao[1:] if self.us else self.icao


@lru_cache(maxsize=1)
def registry() -> dict[str, Station]:
    raw = tomllib.loads(resources.files("polyweather").joinpath("stations.toml").read_text("utf-8"))
    return {
        icao: Station(
            icao=icao,
            city=v["city"],
            tz=v["tz"],
            lat=float(v["lat"]),
            lon=float(v["lon"]),
            elev_m=v.get("elev_m"),
            name=v.get("name", ""),
            country=v.get("country", ""),
            us=bool(v.get("us", False)),
        )
        for icao, v in raw.items()
    }


def get(icao: str) -> Station | None:
    return registry().get(icao)


def in_scope(scope: str) -> list[Station]:
    """Stations covered for a scope: "us" or "all"."""
    stations = sorted(registry().values(), key=lambda s: s.icao)
    return [s for s in stations if s.us] if scope == "us" else stations
