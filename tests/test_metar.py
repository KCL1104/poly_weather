import pytest

from polyweather.resolution.metar import parse_metar
from polyweather.resolution.settle import to_whole_unit


def test_tgroup_takes_precedence_over_body():
    p = parse_metar("METAR KLGA 092051Z 32018G27KT 10SM FEW045 14/01 A2988 RMK AO2 SLP118 T01440006 $")
    assert (p.label, p.station, p.day, p.hour, p.minute) == ("METAR", "KLGA", 9, 20, 51)
    assert (p.temp_tenths_c, p.temp_source) == (144, "tgroup")


def test_examples_quoted_in_the_market_rules():
    # "T01390133 is 13.9°C; T10171022 is −1.7°C."
    assert parse_metar("KXXX 010000Z 00000KT 10SM CLR 14/13 A3000 RMK AO2 T01390133").temp_tenths_c == 139
    assert parse_metar("KXXX 010000Z 00000KT 10SM CLR M02/M10 A3000 RMK AO2 T10171022").temp_tenths_c == -17
    # "18/12 is 18°C; M11/M17 is −11°C."
    assert parse_metar("EGLC 010020Z AUTO 09009KT 9999 BKN020 18/12 Q1019").temp_tenths_c == 180
    p = parse_metar("UUWW 010000Z 09009KT 9999 BKN020 M11/M17 Q1019 NOSIG")
    assert (p.temp_tenths_c, p.temp_source) == (-110, "body")


def test_speci_label_and_missing_label():
    assert parse_metar("SPECI KMIA 071912Z 10005KT 10SM BKN034 28/22 A2991 RMK AO2 T02780217").is_special
    p = parse_metar("KMIA 071953Z VRB03KT 10SM BKN042 29/20 A2987 RMK AO2 SLP116 T02940200")
    assert p.label is None and not p.is_special and p.station == "KMIA"


def test_tgroup_must_have_exactly_eight_digits():
    # A truncated group is not "the hourly temperature remark": the body value governs.
    p = parse_metar("METAR KLGA 092051Z 32018KT 10SM FEW045 14/01 A2988 RMK AO2 T0144")
    assert (p.temp_tenths_c, p.temp_source) == (140, "body")


def test_no_temperature_anywhere_is_disregarded():
    p = parse_metar("METAR KLGA 092051Z 32018KT 10SM FEW045 A2988 RMK AO2 SLP118")
    assert p.temp_tenths_c is None and p.temp_source is None


def test_body_temperature_not_confused_with_other_slash_groups():
    p = parse_metar("METAR KJFK 010051Z 04006KT 1 1/2SM R04R/2000V4000FT BR OVC002 06/05 A2992 RMK AO2")
    assert p.temp_tenths_c == 60
    assert parse_metar("LFPB 241200Z AUTO 21009KT 180V240 CAVOK 12/08 Q1017").temp_tenths_c == 120
    assert parse_metar("EDDM 010020Z AUTO 09009KT 9999 NCD M01/ Q1019").temp_tenths_c == -10


def test_correction_flag():
    assert parse_metar("METAR KLGA 092051Z COR 32018KT 10SM FEW045 14/01 A2988 RMK AO2 T01440006").corrected


@pytest.mark.parametrize(
    "tenths_c, unit, expected",
    [
        # Fahrenheit examples quoted in the rules.
        (183, "F", 65), (178, "F", 64), (190, "F", 66), (-225, "F", -8),
        # Celsius examples quoted in the rules.
        (139, "C", 14), (174, "C", 17), (165, "C", 17), (-17, "C", -2), (-85, "C", -8),
        # ASOS whole-degree-Fahrenheit readings survive the round trip through tenths °C.
        (78, "F", 46), (83, "F", 47), (89, "F", 48), (144, "F", 58),
    ],
)
def test_rounding_rule(tenths_c, unit, expected):
    assert to_whole_unit(tenths_c, unit) == expected
