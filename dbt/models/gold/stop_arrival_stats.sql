{{
    config(
        materialized='table',
        schema='gold'
    )
}}

WITH observed AS (
    SELECT
        line_no,
        stop_id,
        stop_name,
        service_date,
        EXTRACT(HOUR FROM scheduled_arrival) AS hour_of_day,
        delay_minutes,
        scheduled_arrival,
        actual_arrival
    FROM {{ source('silver', 'observed_stop_arrival') }}
    WHERE delay_minutes >= 0
      AND line_no IS NOT NULL
      AND stop_id IS NOT NULL
),

aggregated AS (
    SELECT
        line_no,
        stop_id,
        stop_name,
        service_date AS date,
        hour_of_day,
        COUNT(*) AS total_arrivals,
        ROUND(AVG(delay_minutes), 2) AS avg_delay_minutes,
        ROUND(
            100.0 * SUM(CASE WHEN delay_minutes <= 1 THEN 1 ELSE 0 END) / COUNT(*), 2) 
            AS on_time_pct,
        ROUND(
            100.0 * SUM(CASE WHEN delay_minutes BETWEEN 2 AND 5 THEN 1 ELSE 0 END) / COUNT(*), 2) 
            AS moderate_delay_pct,
        ROUND(
            100.00
            - ROUND(100.0 * SUM(CASE WHEN delay_minutes <= 1 THEN 1 ELSE 0 END) / COUNT(*), 2)
            - ROUND(100.0 * SUM(CASE WHEN delay_minutes BETWEEN 2 AND 5 THEN 1 ELSE 0 END) / COUNT(*), 2),2) 
            AS severe_delay_pct
    FROM observed
    GROUP BY line_no, stop_id, stop_name, service_date, hour_of_day
)

SELECT * FROM aggregated