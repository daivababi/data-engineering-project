{% snapshot dim_stop_snapshot %}

{{ config(
    target_schema='gold',
    unique_key='stop_id',
    strategy='check',
    check_cols=['stop_name', 'stop_lat', 'stop_lon', 'stop_area', 'is_active']
) }}

SELECT
    stop_id,
    stop_name,
    stop_lat,
    stop_lon,
    stop_area,
    is_active
FROM {{ source('bronze', 'raw_stops') }}
WHERE stop_id IS NOT NULL
  AND stop_name IS NOT NULL

{% endsnapshot %}
