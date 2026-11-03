-- Load raw bus stops from stops.txt into bronze.raw_stops.
-- Idempotent: re-running will not duplicate rows (UPSERT by stop_id).

CREATE TEMP TABLE staging_stops (
    stop_id             TEXT,
    stop_code           TEXT,
    stop_name           TEXT,
    stop_lat            DOUBLE PRECISION,
    stop_lon            DOUBLE PRECISION,
    stop_desc           TEXT,
    wheelchair_boarding TEXT,
    platform_code       TEXT,
    location_type       TEXT,
    parent_station      TEXT,
    stop_timezone       TEXT,
    stop_url            TEXT,
    tts_stop_name       TEXT,
    zone_id             TEXT,
    alias               TEXT,
    stop_area           TEXT,
    authority           TEXT,
    lest_x              DOUBLE PRECISION,
    lest_y              DOUBLE PRECISION
);

COPY staging_stops
FROM '/docker-entrypoint-initdb.d/data/stops.txt'
WITH (
    FORMAT csv,
    HEADER true,
    DELIMITER ',',
    QUOTE '"',
    NULL ''
);

INSERT INTO bronze.raw_stops (
    stop_id, stop_code, stop_name, stop_lat, stop_lon, stop_desc,
    wheelchair_boarding, platform_code, location_type, parent_station,
    stop_timezone, stop_url, tts_stop_name, zone_id, alias, stop_area,
    authority, lest_x, lest_y
)
SELECT
    stop_id,
    NULLIF(stop_code, ''),
    TRIM(stop_name),
    stop_lat,
    stop_lon,
    NULLIF(stop_desc, ''),
    NULLIF(wheelchair_boarding, '')::INTEGER,
    NULLIF(platform_code, ''),
    NULLIF(location_type, '')::INTEGER,
    NULLIF(parent_station, ''),
    NULLIF(stop_timezone, ''),
    NULLIF(stop_url, ''),
    NULLIF(tts_stop_name, ''),
    NULLIF(zone_id, ''),
    NULLIF(alias, ''),
    NULLIF(stop_area, ''),
    NULLIF(authority, ''),
    lest_x,
    lest_y
FROM staging_stops
WHERE stop_id IS NOT NULL
  AND stop_name IS NOT NULL
  AND stop_lat BETWEEN 57.5 AND 59.7
  AND stop_lon BETWEEN 21.7 AND 28.3
ON CONFLICT (stop_id) DO UPDATE
    SET stop_name           = EXCLUDED.stop_name,
        stop_lat            = EXCLUDED.stop_lat,
        stop_lon            = EXCLUDED.stop_lon,
        stop_desc           = EXCLUDED.stop_desc,
        wheelchair_boarding = EXCLUDED.wheelchair_boarding,
        platform_code       = EXCLUDED.platform_code,
        location_type       = EXCLUDED.location_type,
        parent_station      = EXCLUDED.parent_station,
        stop_area           = EXCLUDED.stop_area,
        authority           = EXCLUDED.authority,
        lest_x              = EXCLUDED.lest_x,
        lest_y              = EXCLUDED.lest_y,
        loaded_at           = now();

DROP TABLE staging_stops;
