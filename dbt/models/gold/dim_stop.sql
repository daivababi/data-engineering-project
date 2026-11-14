SELECT
    dbt_scd_id AS stop_key,
    stop_id,
    stop_name,
    stop_lat AS latitude,
    stop_lon AS longitude,
    TO_CHAR(dbt_valid_from::date, 'YYYYMMDD')::integer AS valid_from_date_key,
    COALESCE(
        TO_CHAR((dbt_valid_to::date - 1), 'YYYYMMDD')::integer,
        99991231
    ) AS valid_to_date_key,
    dbt_valid_to IS NULL AND is_active AS is_current
FROM {{ ref('dim_stop_snapshot') }}
WHERE stop_area = 'Tartu linn'
  AND (is_active OR dbt_valid_to IS NOT NULL)
