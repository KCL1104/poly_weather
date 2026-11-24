import math

import pytest

from polyweather.catalog.brackets import parse_bracket
from polyweather.models.m0 import PercentileDistribution, bracket_probabilities

TITLES = ["49°F or below", "50-51°F", "52-53°F", "54-55°F", "56-57°F", "58-59°F", "60-61°F", "62-63°F",
          "64-65°F", "66-67°F", "68°F or higher"]
LADDER = [parse_bracket(t) for t in TITLES]


def test_passes_through_the_five_percentiles():
    d = PercentileDistribution.from_percentiles(52, 55, 58, 62, 65)
    for x, q in zip((52, 55, 58, 62, 65), (0.10, 0.25, 0.50, 0.75, 0.90)):
        assert d.cdf(x) == pytest.approx(q, abs=1e-12)
    xs = [40 + 0.1 * i for i in range(400)]
    cdf = [d.cdf(x) for x in xs]
    assert all(b >= a for a, b in zip(cdf, cdf[1:])) and cdf[0] < 1e-3 and cdf[-1] > 1 - 1e-3


def test_bracket_probabilities_sum_to_one_and_peak_at_the_median():
    d = PercentileDistribution.from_percentiles(54, 56, 58, 60, 62)
    p = bracket_probabilities(d, LADDER)
    assert sum(p) == pytest.approx(1.0, abs=1e-12)
    assert max(range(len(p)), key=p.__getitem__) == TITLES.index("58-59°F")
    # Symmetric input around a bracket edge puts equal mass on both sides of it.
    e = PercentileDistribution.from_percentiles(53.5, 55.5, 57.5, 59.5, 61.5)
    q = bracket_probabilities(e, LADDER)
    assert q[TITLES.index("56-57°F")] == pytest.approx(q[TITLES.index("58-59°F")], rel=1e-9)


def test_tied_percentiles_from_whole_degree_rounding_still_work():
    # A sharp short-range forecast: the bulletin reports 67 67 68 68 68.
    d = PercentileDistribution.from_percentiles(67, 67, 68, 68, 68)
    assert all(b > a for a, b in zip(d.knots, d.knots[1:]))
    assert d.median == 68
    p = bracket_probabilities(d, LADDER)
    assert sum(p) == pytest.approx(1.0) and p[-1] > 0.6  # "68°F or higher" starts at 67.5
    with pytest.raises(ValueError):
        PercentileDistribution.from_percentiles(60, 59, 61, 62, 63)


def test_calibration_shift_and_spread():
    d = PercentileDistribution.from_percentiles(54, 56, 58, 60, 62)
    colder = d.calibrated(shift=-2.0)
    assert colder.median == 56 and colder.cdf(56) == pytest.approx(0.5)
    wider = d.calibrated(spread=2.0)
    assert wider.knots == (50.0, 54.0, 58.0, 62.0, 66.0)
    centre = TITLES.index("58-59°F")
    assert bracket_probabilities(wider, LADDER)[centre] < bracket_probabilities(d, LADDER)[centre]
    with pytest.raises(ValueError):
        d.calibrated(spread=0)


def test_tails_are_gaussian_with_a_floor():
    d = PercentileDistribution.from_percentiles(57.75, 57.9, 58, 58.1, 58.25)  # absurdly sharp
    # With the 1-degree floor on tail sigma, 3 degrees out is still not impossible.
    assert 1e-6 < d.cdf(55) < 0.01
    assert math.isclose(d.cdf(58), 0.5)


def test_tail_scale_widens_only_the_tails():
    d = PercentileDistribution.from_percentiles(54, 56, 58, 60, 62)
    fat = d.calibrated(tail_scale=2.0)
    for x in (54, 56, 58, 60, 62, 57.3):  # nothing changes between the outer knots
        assert fat.cdf(x) == pytest.approx(d.cdf(x))
    assert fat.cdf(50) > 3 * d.cdf(50) and (1 - fat.cdf(66)) > 3 * (1 - d.cdf(66))
    assert sum(bracket_probabilities(fat, LADDER)) == pytest.approx(1.0)
    # The excess measure is zero at a knot, None inside, and independent of the scale in use.
    assert d.tail_excess_z(58) is None and d.tail_excess_z(62) == pytest.approx(0.0)
    assert fat.tail_excess_z(65) == pytest.approx(d.tail_excess_z(65)) and d.tail_excess_z(65) > 0
    assert fat.calibrated(shift=1.0).tail_scale == 2.0  # carried through further calibration
    with pytest.raises(ValueError):
        d.calibrated(tail_scale=0)


def test_gaussian_mean_tail_excess_constant():
    from polyweather.models.m0 import GAUSSIAN_MEAN_TAIL_EXCESS

    z0 = 1.2815515655446004
    pdf = math.exp(-z0 * z0 / 2) / math.sqrt(2 * math.pi)
    assert GAUSSIAN_MEAN_TAIL_EXCESS == pytest.approx(pdf / 0.10 - z0, abs=1e-3)

