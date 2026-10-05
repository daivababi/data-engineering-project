-- Business Question 2:
-- Which bus lines have the highest proportion of delays?

SELECT
    t.line_no,
    COUNT(*) AS total_arrivals,
    SUM(CASE WHEN f.delay_minutes >= 2 THEN 1 ELSE 0 END) AS delayed_arrivals,
    ROUND(
        100.0 * SUM(CASE WHEN f.delay_minutes >= 2 THEN 1 ELSE 0 END)
        / COUNT(*),
        2
    ) AS delay_percentage
FROM fact_stop_arrival f
JOIN dim_trip t ON f.trip_key = t.trip_key
GROUP BY t.line_no
HAVING COUNT(*) > 10
ORDER BY delay_percentage DESC;
