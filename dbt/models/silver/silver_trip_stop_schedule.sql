{{ config(materialized='view', schema='silver') }}

SELECT
    md5(st.trip_id || ':' || st.stop_sequence::text) AS trip_stop_key,
    st.trip_id,
    t.service_id,
    t.route_id,
    r.route_short_name AS line_no,
    r.route_long_name AS route_name,
    t.direction_id,
    st.stop_id,
    s.stop_name,
    st.stop_sequence,
    st.arrival_time AS scheduled_arrival_gtfs,
    st.arrival_offset_seconds,
    st.arrival_offset_seconds * interval '1 second'
        AS scheduled_arrival_offset
FROM {{ ref('silver_stop_times') }} AS st
JOIN {{ ref('silver_trips') }} AS t
    ON st.trip_id = t.trip_id
JOIN {{ ref('silver_stops') }} AS s
    ON st.stop_id = s.stop_id
LEFT JOIN {{ ref('silver_routes') }} AS r
    ON t.route_id = r.route_id