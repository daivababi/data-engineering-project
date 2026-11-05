{{ config(materialized='table', schema='silver') }}

SELECT
    NULLIF(TRIM(service_id), '') AS service_id,
    TO_DATE(date, 'YYYYMMDD') AS service_date,
    NULLIF(TRIM(exception_type), '')::smallint AS exception_type,
    loaded_at
FROM {{ source('bronze', 'raw_calendar_dates') }}
WHERE NULLIF(TRIM(service_id), '') IS NOT NULL
  AND NULLIF(TRIM(date), '') IS NOT NULL
  AND NULLIF(TRIM(exception_type), '') IS NOT NULL
