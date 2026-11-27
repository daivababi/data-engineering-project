{{ config(
    materialized='table',
    unique_key='stop_id',
    schema='silver',
    indexes=[{'columns': ['stop_id'], 'unique': true}]
) }}

SELECT
    stops.file_version,
    stop_id,
    stop_name,
    stop_lat::double precision AS stop_lat,
    stop_lon::double precision AS stop_lon,
    stop_area,
    authority,
    loaded_at
FROM {{ source('bronze', 'raw_stops') }} AS stops
WHERE stops.file_version = (SELECT file_version FROM {{ ref('silver_current_gtfs_version') }})
  AND stops.stop_id IS NOT NULL
  AND stops.stop_name IS NOT NULL
  AND stops.is_active
  AND stops.stop_area = 'Tartu linn'
