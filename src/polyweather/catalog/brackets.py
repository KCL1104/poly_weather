"""Parse a market's temperature range from its group title.

Titles seen in production: "49°F or below", "50-51°F", "68°F or higher", "11°C",
"-1°C", "-1°C or below". Bounds are inclusive whole degrees; None means open-ended.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Sequence

_N = r"([-−]?\d+)"
_UNIT = r"\s*°\s*([FC])"
_PATTERNS: list[tuple[re.Pattern[str], str]] = [
    (re.compile(rf"^{_N}{_UNIT} or (?:below|lower)$"), "below"),
    (re.compile(rf"^{_N}{_UNIT} or (?:higher|above)$"), "above"),
    (re.compile(rf"^{_N}\s*(?:-|–|to)\s*{_N}{_UNIT}$"), "range"),
    (re.compile(rf"^{_N}{_UNIT}$"), "single"),
]


@dataclass(frozen=True)
class Bracket:
    lo: int | None
    hi: int | None
    unit: str

    def contains(self, value: int) -> bool:
        return (self.lo is None or value >= self.lo) and (self.hi is None or value <= self.hi)

    @property
    def label(self) -> str:
        if self.lo is None:
            return f"<={self.hi}{self.unit}"
        if self.hi is None:
            return f">={self.lo}{self.unit}"
        return f"{self.lo}{self.unit}" if self.lo == self.hi else f"{self.lo}-{self.hi}{self.unit}"


def _int(s: str) -> int:
    return int(s.replace("−", "-"))


def parse_bracket(title: str) -> Bracket | None:
    t = " ".join((title or "").split())
    for pat, kind in _PATTERNS:
        m = pat.match(t)
        if not m:
            continue
        if kind == "below":
            return Bracket(None, _int(m.group(1)), m.group(2))
        if kind == "above":
            return Bracket(_int(m.group(1)), None, m.group(2))
        if kind == "single":
            v = _int(m.group(1))
            return Bracket(v, v, m.group(2))
        lo, hi = _int(m.group(1)), _int(m.group(2))
        if lo > hi:
            return None
        return Bracket(lo, hi, m.group(3))
    return None


def validate_ladder(brackets: Sequence[Bracket | None]) -> list[str]:
    """Check that an event's brackets, in display order, partition the integers.

    Returns a list of problems; empty means the ladder is sound.
    """
    problems: list[str] = []
    if not brackets:
        return ["no_brackets"]
    if any(b is None for b in brackets):
        return ["bracket_title_unparsed"]
    bs: list[Bracket] = list(brackets)  # type: ignore[arg-type]
    if len({b.unit for b in bs}) != 1:
        problems.append("mixed_units")
    if bs[0].lo is not None:
        problems.append("lowest_bracket_not_open")
    if bs[-1].hi is not None:
        problems.append("highest_bracket_not_open")
    for a, b in zip(bs, bs[1:]):
        if a.hi is None or b.lo is None or b.lo != a.hi + 1:
            problems.append(f"gap_or_overlap:{a.label}|{b.label}")
    return problems


def find_bracket(brackets: Sequence[Bracket], value: int) -> int | None:
    for i, b in enumerate(brackets):
        if b.contains(value):
            return i
    return None
