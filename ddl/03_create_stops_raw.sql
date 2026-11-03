-- Bronze layer: raw bus stops from stops.txt (GTFS)
-- Idempotent: safe to re-run (IF NOT EXISTS).

CREATE SCHEMA IF NOT EXISTS bronze;

CREATE TABLE IF NOT EXISTS bronze.raw_stops (
    stop_id             TEXT PRIMARY KEY,
    stop_code           TEXT,
    stop_name           TEXT NOT NULL,
    stop_lat            DOUBLE PRECISION,
    stop_lon            DOUBLE PRECISION,
    stop_desc           TEXT,
    wheelchair_boarding INTEGER,
    platform_code       TEXT,
    location_type       INTEGER,
    parent_station      TEXT,
    stop_timezone       TEXT,
    stop_url            TEXT,
    tts_stop_name       TEXT,
    zone_id             TEXT,
    alias               TEXT,
    stop_area           TEXT,
    authority           TEXT,
    lest_x              DOUBLE PRECISION,
    lest_y              DOUBLE PRECISION,
    loaded_at           TIMESTAMPTZ DEFAULT now()
);

CREATE INDEX IF NOT EXISTS ix_raw_stops_stop_area ON bronze.raw_stops (stop_area);
CREATE INDEX IF NOT EXISTS ix_raw_stops_name      ON bronze.raw_stops (stop_name);

COMMENT ON TABLE bronze.raw_stops IS
    'Raw GTFS bus stops loaded from data/raw/stops.txt';
