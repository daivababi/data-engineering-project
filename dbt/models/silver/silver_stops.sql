{{ config(
    materialized='table',
    unique_key='stop_id',
    schema='silver',
    indexes=[{'columns': ['stop_id'], 'unique': true}]
) }}

WITH scheduled_stops AS (
    SELECT DISTINCT
        file_version,
        stop_id
    FROM {{ ref('silver_stop_times') }}
)
SELECT
    stops.file_version,
    stops.stop_id,
    stops.stop_name,
    stops.stop_lat::double precision AS stop_lat,
    stops.stop_lon::double precision AS stop_lon,
    stops.stop_area,
    stops.authority,
    stops.loaded_at
FROM {{ source('bronze', 'raw_stops') }} AS stops
LEFT JOIN scheduled_stops
    ON scheduled_stops.file_version = stops.file_version
    AND scheduled_stops.stop_id = stops.stop_id
WHERE stops.file_version = (SELECT file_version FROM {{ ref('silver_current_gtfs_version') }})
  AND stops.stop_id IS NOT NULL
  AND stops.stop_name IS NOT NULL
  AND stops.is_active
  AND (stops.stop_area = 'Tartu linn' OR scheduled_stops.stop_id IS NOT NULL)
