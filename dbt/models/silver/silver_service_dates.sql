{{ config(materialized='view', schema='silver') }}

WITH regular_service_dates AS (
    SELECT
        c.service_id,
        generated.service_datetime::date AS service_date
    FROM {{ ref('silver_calendar') }} AS c
    CROSS JOIN LATERAL generate_series(
        c.start_date::timestamp,
        c.end_date::timestamp,
        interval '1 day'
    ) AS generated(service_datetime)
    WHERE CASE EXTRACT(ISODOW FROM generated.service_datetime)::integer
        WHEN 1 THEN c.monday
        WHEN 2 THEN c.tuesday
        WHEN 3 THEN c.wednesday
        WHEN 4 THEN c.thursday
        WHEN 5 THEN c.friday
        WHEN 6 THEN c.saturday
        WHEN 7 THEN c.sunday
    END IS TRUE
      AND NOT EXISTS (
          SELECT 1
          FROM {{ ref('silver_calendar_dates') }} AS exception
          WHERE exception.service_id = c.service_id
            AND exception.service_date = generated.service_datetime::date
      )
),
added_service_dates AS (
    SELECT DISTINCT
        service_id,
        service_date
    FROM {{ ref('silver_calendar_dates') }}
    WHERE exception_type = 1
),
active_service_dates AS (
    SELECT service_id, service_date
    FROM regular_service_dates

    UNION

    SELECT service_id, service_date
    FROM added_service_dates
)
SELECT
    md5(service_id || ':' || service_date::text) AS service_date_key,
    service_id,
    service_date
FROM active_service_dates