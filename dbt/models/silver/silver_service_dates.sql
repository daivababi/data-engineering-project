{{ config(materialized='table', schema='silver') }}

WITH recurring_dates AS (
    SELECT
        c.service_id,
        generated.service_date
    FROM {{ ref('silver_calendar') }} AS c
    CROSS JOIN LATERAL generate_series(
        c.start_date,
        c.end_date,
        interval '1 day'
    ) AS generated(service_date)
    WHERE CASE EXTRACT(ISODOW FROM generated.service_date)::integer
        WHEN 1 THEN c.monday
        WHEN 2 THEN c.tuesday
        WHEN 3 THEN c.wednesday
        WHEN 4 THEN c.thursday
        WHEN 5 THEN c.friday
        WHEN 6 THEN c.saturday
        WHEN 7 THEN c.sunday
    END
),
active_dates AS (
    SELECT
        recurring.service_id,
        recurring.service_date,
        CASE
            WHEN exceptions.exception_type = 1 THEN 'added_exception'
            ELSE 'regular_calendar'
        END AS service_date_source
    FROM recurring_dates AS recurring
    LEFT JOIN {{ ref('silver_calendar_dates') }} AS exceptions
        ON recurring.service_id = exceptions.service_id
        AND recurring.service_date = exceptions.service_date
    WHERE exceptions.exception_type IS DISTINCT FROM 2

    UNION

    SELECT
        service_id,
        service_date,
        'added_exception' AS service_date_source
    FROM {{ ref('silver_calendar_dates') }}
    WHERE exception_type = 1
)
SELECT
    md5(service_id || ':' || service_date::text) AS service_date_key,
    service_id,
    service_date,
    service_date_source
FROM active_dates
