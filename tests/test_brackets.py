import pytest

from polyweather.catalog.brackets import Bracket, find_bracket, parse_bracket, validate_ladder


@pytest.mark.parametrize(
    "title, expected",
    [
        ("49°F or below", Bracket(None, 49, "F")),
        ("50-51°F", Bracket(50, 51, "F")),
        ("68°F or higher", Bracket(68, None, "F")),
        ("11°C", Bracket(11, 11, "C")),
        ("-1°C", Bracket(-1, -1, "C")),
        ("-1°C or below", Bracket(None, -1, "C")),
        ("−3°C or higher", Bracket(-3, None, "C")),
        ("-4--3°F", Bracket(-4, -3, "F")),
        ("-2 to 1°F", Bracket(-2, 1, "F")),
    ],
)
def test_parse_bracket(title, expected):
    assert parse_bracket(title) == expected


@pytest.mark.parametrize("title", ["", "warm", "50-51", "51-50°F", "50°K"])
def test_unreadable_titles(title):
    assert parse_bracket(title) is None


def test_ladder_must_partition_the_integers():
    good = [parse_bracket(t) for t in ["49°F or below", "50-51°F", "52-53°F", "54°F or higher"]]
    assert validate_ladder(good) == []
    assert find_bracket(good, 49) == 0 and find_bracket(good, 50) == 1 and find_bracket(good, 99) == 3

    gap = [parse_bracket(t) for t in ["49°F or below", "52-53°F", "54°F or higher"]]
    assert any(p.startswith("gap_or_overlap") for p in validate_ladder(gap))
    assert "lowest_bracket_not_open" in validate_ladder(good[1:])
    assert "highest_bracket_not_open" in validate_ladder(good[:-1])
    assert validate_ladder([good[0], None]) == ["bracket_title_unparsed"]
    mixed = [parse_bracket("9°C or below"), parse_bracket("10°F or higher")]
    assert "mixed_units" in validate_ladder(mixed)
