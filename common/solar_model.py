"""
Simple household solar-generation and consumption curves used by the meter simulator.

Kept pure (no I/O, no randomness seeding) so it is trivially unit-testable.
"""
import math
import random


def solar_generation_kwh(hour_of_day: float, peak_kw: float = 3.0, noise: float = 0.05) -> float:
    """Zero at night, bell-shaped, peaking near solar noon (12:00). ``hour_of_day``
    is a float in [0, 24)."""
    if hour_of_day < 6 or hour_of_day > 18:
        base = 0.0
    else:
        # Cosine-squared bump: 0 at 06:00 and 18:00, peak_kw at 12:00.
        base = peak_kw * math.cos((hour_of_day - 12) / 6 * (math.pi / 2)) ** 2
    jitter = random.uniform(-noise, noise) * peak_kw
    return max(0.0, round(base + jitter, 4))


def consumption_kwh(
    hour_of_day: float,
    base_load_kw: float = 0.4,
    evening_peak_kw: float = 1.2,
    noise: float = 0.05,
) -> float:
    """A steady base load plus a Gaussian evening peak centred around 20:00."""
    evening = evening_peak_kw * math.exp(-((hour_of_day - 20) ** 2) / (2 * 2.2 ** 2))
    jitter = random.uniform(-noise, noise) * (base_load_kw + evening_peak_kw)
    return max(0.0, round(base_load_kw + evening + jitter, 4))
