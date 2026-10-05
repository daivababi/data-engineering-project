-- Each row = one stop arrival event

INSERT INTO fact_stop_arrival (
    date_key, stop_key, trip_key, delay_causing_stop,
    scheduled_arrival, actual_arrival,
    delay_minutes, delay_severity
) VALUES
    -- On time (0 min delay)
    (1, 1, 1, NULL, '2025-10-06 08:15:00', '2025-10-06 08:15:40', 0, 'on_time'),

    -- Moderate delay (4 min)
    (1, 2, 2, 2,    '2025-10-06 08:30:00', '2025-10-06 08:34:20', 4, 'moderate'),

    -- Severe delay (8 min)
    (1, 3, 3, 3,    '2025-10-06 09:00:00', '2025-10-06 09:08:10', 8, 'severe'),

    -- Severe delay (7 min) on a different day
    (2, 4, 4, 4,    '2025-10-07 17:45:00', '2025-10-07 17:52:00', 7, 'severe'),

    -- On time again
    (2, 5, 5, NULL, '2025-10-07 18:00:00', '2025-10-07 18:00:50', 0, 'on_time');
