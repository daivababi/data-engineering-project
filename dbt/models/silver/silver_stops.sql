{{ config(
    materialized='table',
    unique_key='stop_id',
    schema='silver',
    indexes=[{'columns': ['stop_id'], 'unique': true}]
) }}

SELECT
    stop_id,
    stop_name,
    stop_lat::double precision AS stop_lat,
    stop_lon::double precision AS stop_lon,
    stop_area,
    authority,
    loaded_at
FROM {{ source('bronze', 'raw_stops') }}
WHERE stop_id   IS NOT NULL
  AND stop_name IS NOT NULL
  AND is_active
  --AND stop_area = 'Tartu linn'
