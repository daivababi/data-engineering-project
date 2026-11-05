{{ config(materialized='table', schema='silver') }}

SELECT
    trip_id,
    route_id,
    service_id,
    NULLIF(TRIM(direction_id), '')::smallint AS direction_id,
    NULLIF(TRIM(shape_id), '') AS shape_id,
    NULLIF(TRIM(trip_headsign), '') AS trip_headsign,
    NULLIF(TRIM(trip_headsign_code), '') AS trip_headsign_code,
    NULLIF(TRIM(trip_short_name), '') AS trip_short_name,
    NULLIF(TRIM(wheelchair_accessible), '')::smallint AS wheelchair_accessible,
    NULLIF(TRIM(bikes_allowed), '')::smallint AS bikes_allowed,
    NULLIF(TRIM(block_id), '') AS block_id,
    loaded_at
FROM {{ source('bronze', 'raw_trips') }}
WHERE NULLIF(TRIM(trip_id), '') IS NOT NULL
  AND NULLIF(TRIM(route_id), '') IS NOT NULL
  AND NULLIF(TRIM(service_id), '') IS NOT NULL
