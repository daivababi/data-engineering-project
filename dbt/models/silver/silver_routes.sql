{{ config(materialized='table', schema='silver') }}

SELECT
    route_id,
    NULLIF(TRIM(agency_id), '') AS agency_id,
    NULLIF(TRIM(route_short_name), '') AS route_short_name,
    NULLIF(TRIM(route_long_name), '') AS route_long_name,
    NULLIF(TRIM(route_desc), '') AS route_desc,
    NULLIF(TRIM(route_type), '')::integer AS route_type,
    NULLIF(TRIM(route_color), '') AS route_color,
    NULLIF(TRIM(route_text_color), '') AS route_text_color,
    NULLIF(TRIM(competent_authority), '') AS competent_authority,
    loaded_at
FROM {{ source('bronze', 'raw_routes') }}
WHERE NULLIF(TRIM(route_id), '') IS NOT NULL
