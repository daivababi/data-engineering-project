SELECT
    active.service_id,
    active.service_date,
    schedule.schedule_key
FROM {{ ref('silver_service_dates') }} AS active
JOIN {{ ref('schedule_stop_arrivals') }} AS schedule
    ON active.service_id = schedule.service_id
LEFT JOIN {{ ref('schedule_stop_arrivals_by_date') }} AS dated_schedule
    ON schedule.schedule_key = dated_schedule.schedule_key
    AND active.service_date = dated_schedule.service_date
WHERE active.service_date_source = 'added_exception'
  AND dated_schedule.scheduled_arrival_key IS NULL
