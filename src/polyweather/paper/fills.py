"""Simulated taker fills, on purpose conservative.

An order is filled against the resting sell orders of the book as it stood two
seconds after the signal: level by level, cheapest first, never above the limit
price, and never more than a quarter of what is visible at a level (the engine does
not model its own effect on the price). The taker fee is the exchange's formula with
the market's own rate: shares x rate x p x (1 - p).

The optimistic variant, reported next to it and never used for a verdict, fills at the
signal instant and may take everything that is visible.
"""

from __future__ import annotations

import math
from dataclasses import dataclass, field

LATENCY_MS = 2_000
LEVEL_SHARE = 0.25
MIN_FILLABLE_USD = 5.0
SHARE_STEP = 0.01  # the exchange trades in hundredths of a share


def fee_per_share(price: float, rate: float) -> float:
    return rate * price * (1.0 - price)


def cost_per_share(price: float, rate: float) -> float:
    """What one share costs a taker at this price, fee included."""
    return price + fee_per_share(price, rate)


def limit_price(win_probability: float, min_edge: float, rate: float, tick: float = 0.001) -> float | None:
    """Highest price at which buying still leaves ``min_edge`` after the fee, on the tick grid.

    Edge per share is the probability of being paid 1 minus the cost of the share.
    Returns None when not even the lowest price qualifies.
    """
    budget = win_probability - min_edge  # the most a share may cost, fee included
    if budget <= 0:
        return None
    steps = int(math.floor(min(budget, 1.0 - tick) / tick + 1e-9))
    while steps > 0 and cost_per_share(steps * tick, rate) > budget + 1e-12:
        steps -= 1
    return round(steps * tick, 6) if steps > 0 else None


@dataclass
class Fill:
    shares: float = 0.0
    notional: float = 0.0  # sum of price x shares
    fee: float = 0.0
    levels: list[tuple[float, float]] = field(default_factory=list)  # (price, shares taken)
    fillable_usd: float = 0.0  # what could have been bought within the limit, by the same rule

    @property
    def spent(self) -> float:
        return self.notional + self.fee

    @property
    def avg_price(self) -> float | None:
        return self.notional / self.shares if self.shares else None

    @property
    def avg_cost(self) -> float | None:
        """Average cost of a share, fee included."""
        return self.spent / self.shares if self.shares else None


def take(
    asks: list[tuple[float, float]],
    *,
    limit: float,
    budget_usd: float,
    rate: float,
    level_share: float = LEVEL_SHARE,
) -> Fill:
    """Buy up to ``budget_usd`` (fee included) from ``asks`` (cheapest first) at prices up to ``limit``."""
    fill = Fill()
    left = budget_usd
    for price, size in asks:
        if price > limit + 1e-12:
            break
        available = size * level_share
        fill.fillable_usd += available * price
        if left <= 1e-9:
            continue
        shares = min(available, left / cost_per_share(price, rate))
        shares = math.floor(shares / SHARE_STEP + 1e-9) * SHARE_STEP
        if shares <= 0:
            continue
        fill.shares += shares
        fill.notional += shares * price
        fill.fee += shares * fee_per_share(price, rate)
        fill.levels.append((price, round(shares, 2)))
        left -= shares * cost_per_share(price, rate)
    fill.shares = round(fill.shares, 2)
    return fill
