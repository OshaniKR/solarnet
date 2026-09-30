import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from common.solar_model import consumption_kwh, solar_generation_kwh


def test_solar_is_zero_at_night():
    assert solar_generation_kwh(hour_of_day=2, noise=0.0) == 0.0
    assert solar_generation_kwh(hour_of_day=22, noise=0.0) == 0.0


def test_solar_peaks_near_noon():
    midday = solar_generation_kwh(hour_of_day=12, noise=0.0)
    morning = solar_generation_kwh(hour_of_day=8, noise=0.0)
    assert midday > morning > 0


def test_consumption_has_evening_peak():
    evening = consumption_kwh(hour_of_day=20, noise=0.0)
    midday = consumption_kwh(hour_of_day=13, noise=0.0)
    assert evening > midday


def test_consumption_and_solar_never_negative():
    for h in range(0, 24):
        assert consumption_kwh(h) >= 0
        assert solar_generation_kwh(h) >= 0
