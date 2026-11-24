"""Model M0: bracket probabilities from NBM probabilistic (NBP) temperature percentiles.

An NBP block gives the 10th, 25th, 50th, 75th and 90th percentile of the daily
maximum (or minimum) in whole degrees Fahrenheit. This module turns those five
numbers into a continuous distribution, applies a calibration (a shift and a spread
factor learned against past settlement values), and integrates it over each market
bracket.

The distribution is interpolated in normal-quantile space: between two percentile
knots the z-score is linear in temperature, and beyond the outer knots it continues
with the slope of the outermost segment. That passes exactly through the five
percentiles, never produces a kink in probability mass at a knot, and gives Gaussian
tails without assuming the whole forecast is Gaussian.

Thirteen months of history show the bulletin's central 80% interval is about right
but misses beyond it are larger than Gaussian tails allow. ``tail_scale`` widens only
the part outside the 10th and 90th percentiles and leaves the middle untouched.

Brackets are whole degrees of the settlement value, so the bracket "58-59" is the
interval from 57.5 to 59.5 on the continuous scale.
"""

from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Sequence

from ..catalog.brackets import Bracket

QUANTILES = (0.10, 0.25, 0.50, 0.75, 0.90)
_Z = (-1.2815515655446004, -0.6744897501960817, 0.0, 0.6744897501960817, 1.2815515655446004)
MIN_STEP = 0.25  # degrees between consecutive knots; the bulletin rounds to whole degrees
MIN_TAIL_SIGMA = 1.0  # floor for the Gaussian tails, in degrees
# E[z - z0 | z > z0] for a standard normal at z0 = the 90th percentile.
GAUSSIAN_MEAN_TAIL_EXCESS = 0.4733


def _phi(z: float) -> float:
    return 0.5 * math.erfc(-z / math.sqrt(2.0))


@dataclass(frozen=True)
class PercentileDistribution:
    """Continuous distribution through five percentile knots (degrees, ascending)."""

    knots: tuple[float, float, float, float, float]
    tail_scale: float = 1.0  # > 1 fattens the tails beyond the outer knots

    @staticmethod
    def from_percentiles(p10: float, p25: float, p50: float, p75: float, p90: float) -> "PercentileDistribution":
        xs = [float(p10), float(p25), float(p50), float(p75), float(p90)]
        if any(b < a for a, b in zip(xs, xs[1:])):
            raise ValueError(f"percentiles not ascending: {xs}")
        # Whole-degree rounding makes neighbours tie; spread ties out around the median
        # so the quantile function is strictly increasing.
        mid = xs[2]
        for i in (1, 0):
            xs[i] = min(xs[i], xs[i + 1] - MIN_STEP)
        for i in (3, 4):
            xs[i] = max(xs[i], xs[i - 1] + MIN_STEP)
        assert xs[2] == mid
        return PercentileDistribution(tuple(xs))  # type: ignore[arg-type]

    @property
    def median(self) -> float:
        return self.knots[2]

    def calibrated(self, shift: float = 0.0, spread: float = 1.0, tail_scale: float | None = None) -> "PercentileDistribution":
        """Move the median by ``shift`` degrees, scale distances from it by ``spread``,
        and optionally set how much wider than Gaussian the tails are."""
        if spread <= 0:
            raise ValueError("spread must be positive")
        if tail_scale is not None and tail_scale <= 0:
            raise ValueError("tail_scale must be positive")
        m = self.median
        knots = tuple(m + shift + spread * (x - m) for x in self.knots)
        return PercentileDistribution(knots, self.tail_scale if tail_scale is None else tail_scale)  # type: ignore[arg-type]

    def _z(self, x: float) -> float:
        k = self.knots
        lo_slope = (_Z[1] - _Z[0]) / max(k[1] - k[0], MIN_TAIL_SIGMA * (_Z[1] - _Z[0]))
        hi_slope = (_Z[4] - _Z[3]) / max(k[4] - k[3], MIN_TAIL_SIGMA * (_Z[4] - _Z[3]))
        if x <= k[0]:
            return _Z[0] + (x - k[0]) * lo_slope / self.tail_scale
        if x >= k[4]:
            return _Z[4] + (x - k[4]) * hi_slope / self.tail_scale
        for i in range(4):
            if x <= k[i + 1]:
                return _Z[i] + (x - k[i]) * (_Z[i + 1] - _Z[i]) / (k[i + 1] - k[i])
        raise AssertionError("unreachable")

    def tail_excess_z(self, x: float) -> float | None:
        """How far beyond an outer knot ``x`` lies, in z units of an unscaled tail.

        None when ``x`` is between the 10th and 90th percentile knots. For a Gaussian
        tail the mean of this quantity is ``GAUSSIAN_MEAN_TAIL_EXCESS``; the ratio of
        the observed mean to that is the tail scale the data ask for.
        """
        k = self.knots
        if k[0] < x < k[4]:
            return None
        z = PercentileDistribution(k, 1.0)._z(x)
        return abs(z) - _Z[4]

    def cdf(self, x: float) -> float:
        return _phi(self._z(x))

    def prob_between(self, lo: float | None, hi: float | None) -> float:
        """P(lo < X <= hi) on the continuous scale; None means unbounded."""
        upper = 1.0 if hi is None else self.cdf(hi)
        lower = 0.0 if lo is None else self.cdf(lo)
        return max(0.0, upper - lower)


def bracket_probabilities(dist: PercentileDistribution, brackets: Sequence[Bracket]) -> list[float]:
    """Probability of each whole-degree bracket; sums to 1 for a sound ladder."""
    out = []
    for b in brackets:
        lo = None if b.lo is None else b.lo - 0.5
        hi = None if b.hi is None else b.hi + 0.5
        out.append(dist.prob_between(lo, hi))
    return out
