-- Fact table: one row = one observed bus arrival at one stop.
-- Idempotent: CREATE TABLE IF NOT EXISTS.

CREATE TABLE IF NOT EXISTS gold.fact_stop_arrival (
    arrival_key         SERIAL PRIMARY KEY,
    date_key            INT  REFERENCES gold.dim_date(date_key),
    stop_key            INT  REFERENCES gold.dim_stop(stop_key),
    trip_key            INT  REFERENCES gold.dim_trip(trip_key),
    delay_causing_stop  INT  REFERENCES gold.dim_stop(stop_key),
    scheduled_arrival   TIMESTAMP NOT NULL,
    actual_arrival      TIMESTAMP NOT NULL,
    delay_minutes       INT NOT NULL,
    delay_severity      VARCHAR(20) NOT NULL
        CHECK (delay_severity IN ('on_time', 'moderate', 'severe')),
    loaded_at           TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS ix_fact_date       ON gold.fact_stop_arrival(date_key);
CREATE INDEX IF NOT EXISTS ix_fact_stop       ON gold.fact_stop_arrival(stop_key);
CREATE INDEX IF NOT EXISTS ix_fact_trip       ON gold.fact_stop_arrival(trip_key);
CREATE INDEX IF NOT EXISTS ix_fact_severity   ON gold.fact_stop_arrival(delay_severity);
CREATE INDEX IF NOT EXISTS ix_fact_delay_min  ON gold.fact_stop_arrival(delay_minutes);