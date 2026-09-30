import os
import sys

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from common.billing import BillInputs, compute_bill


def test_import_only_household_pays_gross():
    result = compute_bill(BillInputs(
        household_id="HH001", imported_kwh=10, exported_kwh=0,
        import_tariff_rate=0.30, export_tariff_rate=0.10,
        subsidy_flag=False, billing_tier="residential_standard",
    ))
    assert result.gross_amount == pytest.approx(3.0)
    assert result.subsidy_amount == 0
    assert result.net_amount == pytest.approx(3.0)


def test_net_exporter_gets_a_credit():
    result = compute_bill(BillInputs(
        household_id="HH002", imported_kwh=2, exported_kwh=10,
        import_tariff_rate=0.30, export_tariff_rate=0.10,
        subsidy_flag=False, billing_tier="residential_solar",
    ))
    assert result.gross_amount == pytest.approx((2 * 0.30) - (10 * 0.10))
    assert result.net_amount < 0


def test_subsidy_only_applies_to_positive_bills():
    result = compute_bill(BillInputs(
        household_id="HH003", imported_kwh=10, exported_kwh=0,
        import_tariff_rate=0.30, export_tariff_rate=0.10,
        subsidy_flag=True, billing_tier="residential_standard",
    ))
    assert result.subsidy_amount == pytest.approx(3.0 * 0.10)
    assert result.net_amount == pytest.approx(3.0 - 0.3)


def test_subsidy_not_applied_when_net_exporter():
    result = compute_bill(BillInputs(
        household_id="HH004", imported_kwh=1, exported_kwh=10,
        import_tariff_rate=0.30, export_tariff_rate=0.10,
        subsidy_flag=True, billing_tier="residential_solar",
    ))
    assert result.subsidy_amount == 0


def test_negative_usage_rejected():
    with pytest.raises(ValueError):
        compute_bill(BillInputs(
            household_id="HH005", imported_kwh=-1, exported_kwh=0,
            import_tariff_rate=0.30, export_tariff_rate=0.10,
            subsidy_flag=False, billing_tier="residential_standard",
        ))
