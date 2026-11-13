{{ config(materialized='table', schema='gold') }}

SELECT
    scheduled.scheduled_arrival_key AS arrival_key,
    date_dim.date_key,
    stop_dim.stop_key,
    trip_dim.trip_key,
    scheduled.service_date,
    scheduled.service_id,
    scheduled.trip_id,
    scheduled.route_id,
    scheduled.line_no,
    scheduled.route_name,
    scheduled.stop_id,
    scheduled.stop_name,
    scheduled.stop_sequence,
    scheduled.scheduled_arrival,
    split_part(scheduled.scheduled_arrival_gtfs, ':', 1)
        || ':' || split_part(scheduled.scheduled_arrival_gtfs, ':', 2)
        AS scheduled_arrival_hhmm
FROM {{ ref('silver_scheduled_arrivals') }} AS scheduled
JOIN {{ ref('dim_date') }} AS date_dim
    ON scheduled.service_date = date_dim.full_date
JOIN {{ ref('dim_stop') }} AS stop_dim
    ON scheduled.stop_id = stop_dim.stop_id
    AND scheduled.service_date >= TO_DATE(stop_dim.valid_from_date_key::text, 'YYYYMMDD')
    AND scheduled.service_date <= TO_DATE(stop_dim.valid_to_date_key::text, 'YYYYMMDD')
JOIN {{ ref('dim_trip') }} AS trip_dim
    ON scheduled.trip_id = trip_dim.trip_id
