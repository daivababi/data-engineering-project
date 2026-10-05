DROP TABLE IF EXISTS dim_date CASCADE;
DROP TABLE IF EXISTS dim_stop CASCADE;
DROP TABLE IF EXISTS dim_trip CASCADE;

-- ---------- dim_date ----------
CREATE TABLE dim_date (
    date_key      SERIAL PRIMARY KEY,
    full_date     DATE NOT NULL UNIQUE,
    day_of_week   VARCHAR(10) NOT NULL
);

-- ---------- dim_stop ----------
CREATE TABLE dim_stop (
    stop_key   SERIAL PRIMARY KEY,
    stop_name  VARCHAR(150) NOT NULL,
    latitude   NUMERIC(9,6),
    longitude  NUMERIC(9,6)
);

-- ---------- dim_trip ----------
CREATE TABLE dim_trip (
    trip_key            SERIAL PRIMARY KEY,
    stop_key            INT REFERENCES dim_stop(stop_key),
    line_no             VARCHAR(10) NOT NULL,
    route               VARCHAR(255),
    scheduled_arrival   TIMESTAMP NOT NULL
);

CREATE INDEX idx_dim_trip_line ON dim_trip(line_no);
