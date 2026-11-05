{{ config(materialized='table', schema='gold') }}

SELECT
    md5(st.trip_id || ':' || st.stop_sequence::text) AS schedule_key,
    t.trip_key,
    st.trip_id,
    s.stop_key,
    st.stop_id,
    s.stop_name,
    t.route_id,
    t.line_no,
    t.route_name,
    t.direction_id,
    t.service_id,
    c.day_type,
    c.start_date AS service_start_date,
    c.end_date AS service_end_date,
    st.stop_sequence,
    st.arrival_time,
    st.arrival_offset_seconds
FROM {{ ref('silver_stop_times') }} AS st
JOIN {{ ref('dim_trip') }} AS t
    ON st.trip_id = t.trip_id
JOIN {{ ref('dim_stop') }} AS s
    ON st.stop_id = s.stop_id
LEFT JOIN {{ ref('silver_calendar') }} AS c
    ON t.service_id = c.service_id
