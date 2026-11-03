{{ config(materialized='incremental', unique_key='stop_key') }}

WITH src AS (
    SELECT
        stop_id,
        stop_name,
        stop_lat AS latitude,
        stop_lon AS longitude
    FROM {{ ref('silver_stops') }}
)
SELECT
    md5(s.stop_id)                         AS stop_key,
    s.stop_id,
    s.stop_name,
    s.latitude,
    s.longitude,
    TO_CHAR(CURRENT_DATE, 'YYYYMMDD')::int AS valid_from_date_key,
    99991231                               AS valid_to_date_key,
    TRUE                                   AS is_current
FROM src s
{% if is_incremental() %}
LEFT JOIN {{ this }} e
  ON e.stop_id = s.stop_id AND e.is_current = TRUE
WHERE e.stop_id IS NULL
   OR e.stop_name <> s.stop_name
   OR e.latitude  <> s.latitude
   OR e.longitude <> s.longitude
{% endif %}
