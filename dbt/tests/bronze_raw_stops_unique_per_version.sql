SELECT
    file_version,
    stop_id,
    COUNT(*) AS duplicate_count
FROM {{ source('bronze', 'raw_stops') }}
GROUP BY file_version, stop_id
HAVING COUNT(*) > 1
