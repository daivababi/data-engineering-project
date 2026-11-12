CREATE SCHEMA IF NOT EXISTS gold;

CREATE TABLE IF NOT EXISTS gold.fact_observed_stop_arrival (
    arrival_id          BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    source_poll_id      UUID NOT NULL,
    source_item_index   INTEGER NOT NULL,
    device_id           TEXT NOT NULL,
    gtfs_trip_id        TEXT NOT NULL,
    line_no             TEXT,
    service_date        DATE NOT NULL,
    stop_id             TEXT NOT NULL,
    stop_name           TEXT,
    stop_sequence       INTEGER NOT NULL,
    scheduled_arrival   TIMESTAMPTZ NOT NULL,
    actual_arrival      TIMESTAMPTZ NOT NULL,
    delay_minutes       INTEGER NOT NULL CHECK (delay_minutes >= 0),
    distance_meters     DOUBLE PRECISION NOT NULL,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (gtfs_trip_id, service_date, stop_sequence),
    FOREIGN KEY (source_poll_id, source_item_index)
        REFERENCES silver.vehicle_observation (source_poll_id, source_item_index)
);

ALTER TABLE gold.fact_observed_stop_arrival
    ADD COLUMN IF NOT EXISTS delay_minutes INTEGER;

ALTER TABLE gold.fact_observed_stop_arrival
    ADD COLUMN IF NOT EXISTS line_no TEXT;

ALTER TABLE gold.fact_observed_stop_arrival
    ADD COLUMN IF NOT EXISTS stop_name TEXT;

UPDATE gold.fact_observed_stop_arrival AS fact
SET line_no = route.route_short_name
FROM silver.silver_trips AS trip
LEFT JOIN silver.silver_routes AS route
    ON route.route_id = trip.route_id
WHERE fact.gtfs_trip_id = trip.trip_id
  AND fact.line_no IS NULL;

UPDATE gold.fact_observed_stop_arrival AS fact
SET stop_name = stop.stop_name
FROM silver.silver_stops AS stop
WHERE fact.stop_id = stop.stop_id
  AND fact.stop_name IS NULL;

UPDATE gold.fact_observed_stop_arrival
SET delay_minutes = GREATEST(
    0,
    FLOOR(EXTRACT(EPOCH FROM (actual_arrival - scheduled_arrival)) / 60)::INTEGER
)
WHERE delay_minutes IS NULL;

ALTER TABLE gold.fact_observed_stop_arrival
    ALTER COLUMN delay_minutes SET NOT NULL;

ALTER TABLE gold.fact_observed_stop_arrival
    DROP COLUMN IF EXISTS delay_seconds;

CREATE INDEX IF NOT EXISTS ix_observed_stop_arrival_trip_date
    ON gold.fact_observed_stop_arrival (gtfs_trip_id, service_date, stop_sequence);

CREATE TABLE IF NOT EXISTS gold.stop_event_processing (
    source_poll_id      UUID NOT NULL,
    source_item_index   INTEGER NOT NULL,
    processed_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
    outcome             TEXT NOT NULL CHECK (outcome IN (
        'invalid_observation', 'inactive_service', 'no_stop_within_radius',
        'arrival_recorded', 'arrival_already_recorded'
    )),
    arrival_id          BIGINT REFERENCES gold.fact_observed_stop_arrival (arrival_id),
    PRIMARY KEY (source_poll_id, source_item_index),
    FOREIGN KEY (source_poll_id, source_item_index)
        REFERENCES silver.vehicle_observation (source_poll_id, source_item_index)
);

CREATE INDEX IF NOT EXISTS ix_vehicle_observation_unprocessed
    ON silver.vehicle_observation (observed_at, source_poll_id, source_item_index);
