CREATE SCHEMA IF NOT EXISTS bronze;

CREATE TABLE IF NOT EXISTS bronze.raw_ridango_poll_events (
    mongo_id       TEXT PRIMARY KEY,
    poll_id        TEXT NOT NULL UNIQUE,
    started_at     TIMESTAMPTZ,
    finished_at    TIMESTAMPTZ,
    status         TEXT NOT NULL,
    vehicle_count  INTEGER,
    raw_document   JSONB NOT NULL,
    ingested_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);