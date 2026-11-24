"""Read the fields of a raw METAR line that the market rules depend on.

The rules (quoted from the market text) are:

* "A report whose line begins with SPECI is a special observation and does not count.
  Every other report is routine, including one issued off the nominal hour and one
  that carries no METAR or SPECI label."
* "The hourly temperature remark is the group beginning with the letter T followed by
  exactly eight digits. After the T, one digit for sign (0 positive, 1 negative) and
  three for tenths of a degree Celsius give the temperature."
* "If a routine observation does not carry the hourly temperature remark, the
  temperature group in the body of that observation governs ... in whole degrees
  Celsius ... temperature and dew point separated by a slash, with M marking a
  negative value."
* "An observation that reports no temperature in either place is disregarded."

Temperatures are kept as integer tenths of a degree Celsius so that no rounding
happens before the rule's own rounding step.
"""

from __future__ import annotations

import re
from dataclasses import dataclass

_TIME = re.compile(r"(?<!\S)(\d{2})(\d{2})(\d{2})Z(?!\S)")
_TGROUP = re.compile(r"(?<!\S)T([01])(\d{3})[01]\d{3}(?!\S)")
# Dew point may be missing ("08/") or masked ("08///"); the temperature must be two digits.
_BODY_TEMP = re.compile(r"(?<!\S)(M?)(\d{2})/(?:M?\d{2}|//)?(?!\S)")
_STATION = re.compile(r"(?<!\S)([A-Z][A-Z0-9]{3})(?!\S)")


@dataclass(frozen=True)
class ParsedMetar:
    label: str | None  # 'METAR', 'SPECI' or None when the line carries no label
    station: str | None
    day: int | None
    hour: int | None
    minute: int | None
    corrected: bool
    temp_tenths_c: int | None
    temp_source: str | None  # 'tgroup' or 'body'

    @property
    def is_special(self) -> bool:
        return self.label == "SPECI"


def parse_metar(raw: str) -> ParsedMetar:
    text = " ".join(raw.replace("=", " ").split())
    tokens = text.split(" ")
    label = tokens[0] if tokens and tokens[0] in ("METAR", "SPECI") else None

    t = _TIME.search(text)
    day = hour = minute = None
    if t:
        day, hour, minute = int(t.group(1)), int(t.group(2)), int(t.group(3))

    station = None
    head = text[: t.start()] if t else text
    for tok in head.split(" "):
        if tok in ("METAR", "SPECI", "COR", "AUTO", ""):
            continue
        if _STATION.fullmatch(tok):
            station = tok
            break

    body, _, remarks = text.partition(" RMK ")
    corrected = " COR " in f" {body} "

    temp: int | None = None
    source: str | None = None
    tg = _TGROUP.search(remarks) if remarks else None
    if tg:
        temp = int(tg.group(2)) * (-1 if tg.group(1) == "1" else 1)
        source = "tgroup"
    else:
        # Skip the part before the time group so a station id can never match.
        search_from = t.end() if t else 0
        bt = _BODY_TEMP.search(body, search_from)
        if bt:
            temp = int(bt.group(2)) * 10 * (-1 if bt.group(1) else 1)
            source = "body"

    return ParsedMetar(label, station, day, hour, minute, corrected, temp, source)
