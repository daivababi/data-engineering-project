{{ config(severity='warn', store_failures=true) }}

SELECT DISTINCT
    stop_times.file_version,
    stop_times.stop_id,
    raw_stops.stop_name,
    raw_stops.stop_area
FROM {{ ref('silver_stop_times') }} AS stop_times
JOIN {{ source('bronze', 'raw_stops') }} AS raw_stops
    ON raw_stops.file_version = stop_times.file_version
    AND raw_stops.stop_id = stop_times.stop_id
LEFT JOIN {{ ref('silver_stops') }} AS silver_stops
    ON silver_stops.file_version = stop_times.file_version
    AND silver_stops.stop_id = stop_times.stop_id
WHERE silver_stops.stop_id IS NULL
