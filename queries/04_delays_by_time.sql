-- Business Question 4:
-- At what time of the day and day of the week are delays most frequent?

SELECT
    d.day_of_week,
    EXTRACT(HOUR FROM f.scheduled_arrival) AS hour_of_day,
    COUNT(*) AS total_arrivals,
    SUM(CASE WHEN f.delay_minutes >= 2 THEN 1 ELSE 0 END) AS delayed_arrivals,
    ROUND(
        100.0 * SUM(CASE WHEN f.delay_minutes >= 2 THEN 1 ELSE 0 END)
        / COUNT(*),
        2
    ) AS delay_percentage
FROM fact_stop_arrival f
JOIN dim_date d ON f.date_key = d.date_key
GROUP BY d.day_of_week, EXTRACT(HOUR FROM f.scheduled_arrival)
HAVING COUNT(*) > 5
ORDER BY delay_percentage DESC
LIMIT 20;
