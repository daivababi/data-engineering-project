{{ config(materialized='view', schema='silver') }}

SELECT
    md5(
        schedule.service_id || ':' ||
        schedule.trip_id || ':' ||
        schedule.stop_sequence::text || ':' ||
        service_dates.service_date::text
    ) AS scheduled_arrival_key,
    schedule.service_id,
    service_dates.service_date,
    schedule.trip_id,
    schedule.route_id,
    schedule.line_no,
    schedule.route_name,
    schedule.stop_id,
    schedule.stop_name,
    schedule.stop_sequence,
    schedule.scheduled_arrival_gtfs,
    service_dates.service_date + schedule.scheduled_arrival_offset
        AS scheduled_arrival
FROM {{ ref('silver_trip_stop_schedule') }} AS schedule
JOIN {{ ref('silver_service_dates') }} AS service_dates
    ON schedule.service_id = service_dates.service_id