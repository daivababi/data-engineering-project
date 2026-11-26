-- Dimension tables for the Tartu bus delays star schema.
-- Idempotent: CREATE TABLE IF NOT EXISTS.
-- stop_key / trip_key are INT (dbt generates them via row_number()).

CREATE SCHEMA IF NOT EXISTS gold;

-- ---------- dim_date (static) ----------
CREATE TABLE IF NOT EXISTS gold.dim_date (
    date_key     INT PRIMARY KEY,
    full_date    DATE NOT NULL UNIQUE,
    day_of_week  VARCHAR(10) NOT NULL
);

-- ---------- dim_stop (SCD Type 2) ----------
CREATE TABLE IF NOT EXISTS gold.dim_stop (
    stop_key             INT PRIMARY KEY,        -- dbt fills via row_number()
    stop_id              TEXT NOT NULL,          -- natural key from bronze.raw_stops
    stop_name            VARCHAR(150) NOT NULL,
    latitude             NUMERIC(9,6),
    longitude            NUMERIC(9,6),
    valid_from_date_key  INT  NOT NULL REFERENCES gold.dim_date(date_key),
    valid_to_date_key    INT  NULL     REFERENCES gold.dim_date(date_key),
    is_current           BOOLEAN NOT NULL DEFAULT TRUE,
    loaded_at            TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS ix_dim_stop_stop_id ON gold.dim_stop (stop_id);
CREATE INDEX IF NOT EXISTS ix_dim_stop_current ON gold.dim_stop (is_current) WHERE is_current = TRUE;

-- ---------- dim_trip ----------
CREATE TABLE IF NOT EXISTS gold.dim_trip (
    trip_key           INT PRIMARY KEY,           -- dbt fills via row_number()
    stop_key           INT REFERENCES gold.dim_stop(stop_key),
    line_no            VARCHAR(10) NOT NULL,
    route              VARCHAR(255),
    scheduled_arrival  TIMESTAMP NOT NULL
);

CREATE INDEX IF NOT EXISTS ix_dim_trip_line ON gold.dim_trip (line_no);
CREATE INDEX IF NOT EXISTS ix_dim_trip_stop ON gold.dim_trip (stop_key);
