{{ config(materialized='table', schema='silver') }}

WITH source AS (
    SELECT
        NULLIF(TRIM(trip_id), '') AS trip_id,
        NULLIF(TRIM(stop_id), '') AS stop_id,
        NULLIF(TRIM(stop_sequence), '')::integer AS stop_sequence,
        NULLIF(TRIM(arrival_time), '') AS arrival_time,
        loaded_at
    FROM {{ source('bronze', 'raw_stop_times') }}
)
SELECT
    trip_id,
    stop_id,
    stop_sequence,
    arrival_time,
    EXTRACT(EPOCH FROM arrival_time::interval)::integer AS arrival_offset_seconds,
    loaded_at
FROM source
WHERE trip_id IS NOT NULL
  AND stop_id IS NOT NULL
  AND stop_sequence IS NOT NULL
