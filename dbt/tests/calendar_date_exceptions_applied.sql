SELECT
    exceptions.service_id,
    exceptions.service_date,
    exceptions.exception_type
FROM {{ ref('silver_calendar_dates') }} AS exceptions
LEFT JOIN {{ ref('silver_service_dates') }} AS active
    ON exceptions.service_id = active.service_id
    AND exceptions.service_date = active.service_date
WHERE (exceptions.exception_type = 1 AND active.service_id IS NULL)
   OR (exceptions.exception_type = 2 AND active.service_id IS NOT NULL)
