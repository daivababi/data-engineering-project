{{ config(materialized='table', schema='gold') }}

WITH service_dates AS (
    SELECT DISTINCT service_date AS full_date
    FROM {{ ref('silver_service_dates') }}
)
SELECT
    TO_CHAR(full_date, 'YYYYMMDD')::integer AS date_key,
    full_date,
    CASE EXTRACT(ISODOW FROM full_date)::integer
        WHEN 1 THEN 'Monday'
        WHEN 2 THEN 'Tuesday'
        WHEN 3 THEN 'Wednesday'
        WHEN 4 THEN 'Thursday'
        WHEN 5 THEN 'Friday'
        WHEN 6 THEN 'Saturday'
        WHEN 7 THEN 'Sunday'
    END AS day_of_week
FROM service_dates
