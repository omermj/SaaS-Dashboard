import math


def safe_margin(numerator: float, denominator: float) -> float:
    """Calculate margin safely, returning 0.0 if denominator is 0."""

    if denominator == 0:
        return math.nan
    return numerator / denominator
