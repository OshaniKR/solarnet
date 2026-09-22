"""
Net-billing calculation for SolarNet.

Kept as pure, dependency-free functions (no DB, no Spark, no Airflow) so it can be
unit tested in isolation and imported unchanged by the Airflow DAG.

Formula (matches SolarNet_Team_Project_Brief.pdf section 2):
    net_kwh   = consumption - solar_generation
    imported_kwh = sum of positive net_kwh over the billing period (drawn from grid)
    exported_kwh = sum of negative net_kwh over the billing period, made positive (sent to grid)
    gross_amount = imported_kwh * import_tariff_rate - exported_kwh * export_tariff_rate
    subsidy is then applied to gross_amount

ASSUMPTION (not specified by the brief -- state this in the report):
    The subsidy is modelled as a flat percentage discount applied ONLY when the
    household is a net importer for the period (gross_amount > 0) and subsidy_flag
    is true. A net exporter (gross_amount <= 0) already receives a net credit from
    the utility, so no subsidy is applied on top of that credit.
"""
from dataclasses import dataclass

SUBSIDY_RATE = 0.10  # 10% off a positive (import-side) bill -- a project assumption.


@dataclass
class BillInputs:
    household_id: str
    imported_kwh: float
    exported_kwh: float
    import_tariff_rate: float
    export_tariff_rate: float
    subsidy_flag: bool
    billing_tier: str


@dataclass
class BillResult:
    household_id: str
    imported_kwh: float
    exported_kwh: float
    gross_amount: float
    subsidy_amount: float
    net_amount: float
    billing_tier: str


def compute_bill(inputs: BillInputs) -> BillResult:
    if inputs.imported_kwh < 0 or inputs.exported_kwh < 0:
        raise ValueError("imported_kwh and exported_kwh must both be >= 0")

    gross = (inputs.imported_kwh * inputs.import_tariff_rate) - (
        inputs.exported_kwh * inputs.export_tariff_rate
    )

    subsidy = 0.0
    if inputs.subsidy_flag and gross > 0:
        subsidy = round(gross * SUBSIDY_RATE, 4)

    net = round(gross - subsidy, 4)

    return BillResult(
        household_id=inputs.household_id,
        imported_kwh=round(inputs.imported_kwh, 4),
        exported_kwh=round(inputs.exported_kwh, 4),
        gross_amount=round(gross, 4),
        subsidy_amount=subsidy,
        net_amount=net,
        billing_tier=inputs.billing_tier,
    )
