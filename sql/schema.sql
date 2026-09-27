-- SolarNet schema.

CREATE TABLE IF NOT EXISTS raw_readings (
    id                    BIGSERIAL PRIMARY KEY,
    meter_id              TEXT NOT NULL,
    household_id          TEXT NOT NULL,
    grid_zone             TEXT NOT NULL,
    power_consumption_kwh DOUBLE PRECISION NOT NULL,
    solar_generation_kwh  DOUBLE PRECISION NOT NULL,
    net_kwh               DOUBLE PRECISION NOT NULL,   -- consumption - solar
    is_exporting          BOOLEAN NOT NULL,            -- net_kwh < 0
    event_timestamp       TIMESTAMPTZ NOT NULL,        -- from the meter payload
    ingested_at           TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (meter_id, event_timestamp)                 -- de-dup safety net at the DB layer
);

CREATE INDEX IF NOT EXISTS idx_raw_readings_household_ts
    ON raw_readings (household_id, event_timestamp);
CREATE INDEX IF NOT EXISTS idx_raw_readings_zone_ts
    ON raw_readings (grid_zone, event_timestamp);

CREATE TABLE IF NOT EXISTS live_zone_metrics (
    grid_zone            TEXT NOT NULL,
    window_start         TIMESTAMPTZ NOT NULL,
    window_end           TIMESTAMPTZ NOT NULL,
    total_load_kwh       DOUBLE PRECISION NOT NULL,
    total_solar_kwh      DOUBLE PRECISION NOT NULL,
    solar_share_pct      DOUBLE PRECISION NOT NULL,
    exporting_households INTEGER NOT NULL,
    updated_at           TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (grid_zone, window_start)
);

CREATE TABLE IF NOT EXISTS tariffs (
    household_id       TEXT NOT NULL,
    tariff_date        DATE NOT NULL,
    import_tariff_rate DOUBLE PRECISION NOT NULL,
    export_tariff_rate DOUBLE PRECISION NOT NULL,
    billing_tier       TEXT NOT NULL,
    subsidy_flag       BOOLEAN NOT NULL DEFAULT false,
    PRIMARY KEY (household_id, tariff_date)
);

CREATE TABLE IF NOT EXISTS daily_bills (
    household_id       TEXT NOT NULL,
    bill_date          DATE NOT NULL,
    imported_kwh       DOUBLE PRECISION NOT NULL,
    exported_kwh       DOUBLE PRECISION NOT NULL,
    import_tariff_rate DOUBLE PRECISION NOT NULL,
    export_tariff_rate DOUBLE PRECISION NOT NULL,
    subsidy_flag       BOOLEAN NOT NULL DEFAULT false,
    gross_amount       DOUBLE PRECISION NOT NULL,
    subsidy_amount     DOUBLE PRECISION NOT NULL DEFAULT 0,
    net_amount         DOUBLE PRECISION NOT NULL,
    billing_tier       TEXT,
    created_at         TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (household_id, bill_date)
);

CREATE TABLE IF NOT EXISTS alerts (
    id           BIGSERIAL PRIMARY KEY,
    alert_type   TEXT NOT NULL,   -- NO_DATA | LOW_SOLAR_SHARE | DAG_FAILURE | INVALID_TARIFF_FILE
    severity     TEXT NOT NULL DEFAULT 'warning',
    message      TEXT NOT NULL,
    grid_zone    TEXT,
    household_id TEXT,
    source       TEXT NOT NULL,   -- api-monitor | airflow | spark
    triggered_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    resolved     BOOLEAN NOT NULL DEFAULT false
);

CREATE INDEX IF NOT EXISTS idx_alerts_triggered_at ON alerts (triggered_at DESC);
