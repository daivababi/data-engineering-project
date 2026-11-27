{{ config(materialized='table', schema='silver') }}

SELECT
    file_version,
    NULLIF(TRIM(service_id), '') AS service_id,
    TO_DATE(date, 'YYYYMMDD') AS service_date,
    NULLIF(TRIM(exception_type), '')::smallint AS exception_type,
    loaded_at
FROM {{ source('bronze', 'raw_calendar_dates') }}
WHERE file_version = (SELECT file_version FROM {{ ref('silver_current_gtfs_version') }})
  AND NULLIF(TRIM(service_id), '') IS NOT NULL
  AND NULLIF(TRIM(date), '') IS NOT NULL
  AND NULLIF(TRIM(exception_type), '') IS NOT NULL
