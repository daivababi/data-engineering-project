CREATE SCHEMA IF NOT EXISTS silver;

CREATE TABLE IF NOT EXISTS silver.vehicle_observation (
    source_poll_id      UUID NOT NULL,
    source_item_index   INTEGER NOT NULL CHECK (source_item_index >= 0),
    cleaned_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    device_id           TEXT,
    observed_at         TIMESTAMPTZ,
    service_date        DATE,
    ridango_trip_id     TEXT,
    gtfs_trip_id        TEXT,
    route_short_name    TEXT,
    latitude            DOUBLE PRECISION,
    longitude           DOUBLE PRECISION,
    is_valid            BOOLEAN NOT NULL,
    validation_error    TEXT,
    PRIMARY KEY (source_poll_id, source_item_index),
    CHECK (
        (is_valid AND validation_error IS NULL)
        OR (NOT is_valid AND validation_error IS NOT NULL)
    )
);

ALTER TABLE silver.vehicle_observation
    DROP COLUMN IF EXISTS trip_match_count;

ALTER TABLE silver.vehicle_observation
    ADD COLUMN IF NOT EXISTS service_date DATE;

COMMENT ON COLUMN silver.vehicle_observation.service_date IS
    'Ridango trip start date; combine with GTFS arrival offsets, which may exceed 24:00:00.';

CREATE INDEX IF NOT EXISTS ix_vehicle_observation_cleaned_at
    ON silver.vehicle_observation (cleaned_at DESC);

CREATE INDEX IF NOT EXISTS ix_vehicle_observation_gtfs_trip_id
    ON silver.vehicle_observation (gtfs_trip_id)
    WHERE is_valid;
