{{ config(materialized='table', schema='gold') }}

SELECT
    md5(t.trip_id) AS trip_key,
    t.trip_id,
    t.route_id,
    r.route_short_name AS line_no,
    r.route_long_name AS route_name,
    t.service_id,
    t.direction_id,
    t.trip_headsign,
    t.trip_headsign_code,
    t.trip_short_name,
    t.shape_id,
    t.wheelchair_accessible,
    t.bikes_allowed,
    t.block_id
FROM {{ ref('silver_trips') }} AS t
LEFT JOIN {{ ref('silver_routes') }} AS r
    ON t.route_id = r.route_id
