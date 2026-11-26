-- Business Question 4:
-- At what time of the day and day of the week are delays most frequent?

SELECT
    d.day_of_week,
    s.hour_of_day,
    s.line_no,
    s.stop_name,
    SUM(s.total_arrivals) AS total_arrivals,
    ROUND(
        SUM(s.total_arrivals * (100.0 - s.on_time_pct)) / SUM(s.total_arrivals),
        2
    ) AS delay_percentage
FROM gold.stop_arrival_stats s
JOIN gold.dim_date d ON s.date = d.full_date
GROUP BY d.day_of_week, s.hour_of_day, s.line_no, s.stop_name
HAVING SUM(s.total_arrivals) > 5
ORDER BY delay_percentage DESC
LIMIT 20;