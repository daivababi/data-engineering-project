{{ config(materialized='view', schema='gold') }}

SELECT
    md5(schedule.schedule_key || ':' || active.service_date::text) AS scheduled_arrival_key,
    active.service_date,
    schedule.schedule_key,
    schedule.trip_key,
    schedule.trip_id,
    schedule.stop_key,
    schedule.stop_id,
    schedule.stop_name,
    schedule.route_id,
    schedule.line_no,
    schedule.route_name,
    schedule.direction_id,
    schedule.service_id,
    schedule.day_type,
    active.service_date_source,
    schedule.stop_sequence,
    schedule.arrival_time,
    schedule.arrival_offset_seconds
FROM {{ ref('schedule_stop_arrivals') }} AS schedule
JOIN {{ ref('silver_service_dates') }} AS active
    ON schedule.service_id = active.service_id
