{{ config(materialized='table', schema='silver') }}

WITH typed AS (
    SELECT
        NULLIF(TRIM(service_id), '') AS service_id,
        TO_DATE(start_date, 'YYYYMMDD') AS start_date,
        TO_DATE(end_date, 'YYYYMMDD') AS end_date,
        NULLIF(TRIM(monday), '')::integer = 1 AS monday,
        NULLIF(TRIM(tuesday), '')::integer = 1 AS tuesday,
        NULLIF(TRIM(wednesday), '')::integer = 1 AS wednesday,
        NULLIF(TRIM(thursday), '')::integer = 1 AS thursday,
        NULLIF(TRIM(friday), '')::integer = 1 AS friday,
        NULLIF(TRIM(saturday), '')::integer = 1 AS saturday,
        NULLIF(TRIM(sunday), '')::integer = 1 AS sunday
    FROM {{ source('bronze', 'raw_calendar') }}
)
SELECT
    service_id,
    start_date,
    end_date,
    monday,
    tuesday,
    wednesday,
    thursday,
    friday,
    saturday,
    sunday,
    NULLIF(
        CONCAT_WS(
            '',
            CASE WHEN monday THEN 'Mo' END,
            CASE WHEN tuesday THEN 'Tu' END,
            CASE WHEN wednesday THEN 'We' END,
            CASE WHEN thursday THEN 'Th' END,
            CASE WHEN friday THEN 'Fr' END,
            CASE WHEN saturday THEN 'Sa' END,
            CASE WHEN sunday THEN 'Su' END
        ),
        ''
    ) AS day_type
FROM typed
WHERE service_id IS NOT NULL
