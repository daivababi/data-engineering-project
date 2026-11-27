SELECT DISTINCT
    stop_times.file_version,
    stop_times.stop_id
FROM {{ ref('silver_stop_times') }} AS stop_times
LEFT JOIN {{ source('bronze', 'raw_stops') }} AS raw_stops
    ON raw_stops.file_version = stop_times.file_version
    AND raw_stops.stop_id = stop_times.stop_id
WHERE raw_stops.stop_id IS NULL
