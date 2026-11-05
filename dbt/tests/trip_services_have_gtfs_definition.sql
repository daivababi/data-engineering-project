SELECT
    trips.trip_id,
    trips.service_id
FROM {{ ref('silver_trips') }} AS trips
LEFT JOIN {{ ref('silver_calendar') }} AS calendar
    ON trips.service_id = calendar.service_id
LEFT JOIN {{ ref('silver_calendar_dates') }} AS exceptions
    ON trips.service_id = exceptions.service_id
WHERE calendar.service_id IS NULL
  AND exceptions.service_id IS NULL
