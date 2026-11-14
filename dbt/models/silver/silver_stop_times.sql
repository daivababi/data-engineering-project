{{ config(
    materialized='table',
    schema='silver',
    indexes=[{'columns': ['trip_id', 'stop_sequence']}]
) }}

WITH source AS (
    SELECT
        NULLIF(TRIM(trip_id), '') AS trip_id,
        NULLIF(TRIM(stop_id), '') AS stop_id,
        NULLIF(TRIM(stop_sequence), '') AS stop_sequence_text,
        NULLIF(TRIM(arrival_time), '') AS arrival_time,
        loaded_at
    FROM {{ source('bronze', 'raw_stop_times') }}
),
typed AS (
    SELECT
        trip_id,
        stop_id,
        CASE
            WHEN stop_sequence_text ~ '^[0-9]+$'
                THEN stop_sequence_text::integer
        END AS stop_sequence,
        arrival_time,
        loaded_at
    FROM source
)
SELECT
    trip_id,
    stop_id,
    stop_sequence,
    arrival_time,
    split_part(arrival_time, ':', 1)::integer * 3600
        + split_part(arrival_time, ':', 2)::integer * 60
        + split_part(arrival_time, ':', 3)::integer AS arrival_offset_seconds,
    loaded_at
FROM typed
WHERE trip_id IS NOT NULL
  AND stop_id IS NOT NULL
  AND stop_sequence IS NOT NULL
  AND arrival_time ~ '^[0-9]+:[0-5][0-9]:[0-5][0-9]$'
