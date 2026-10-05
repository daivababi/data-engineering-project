-- Business Question 3:
-- Which bus stops have the highest proportion of delays?

SELECT
    s.stop_name,
    COUNT(*) AS total_arrivals,
    SUM(CASE WHEN f.delay_minutes >= 2 THEN 1 ELSE 0 END) AS delayed_arrivals,
    ROUND(
        100.0 * SUM(CASE WHEN f.delay_minutes >= 2 THEN 1 ELSE 0 END)
        / COUNT(*),
        2
    ) AS delay_percentage
FROM fact_stop_arrival f
JOIN dim_stop s ON f.stop_key = s.stop_key
GROUP BY s.stop_name
HAVING COUNT(*) > 20
ORDER BY delay_percentage DESC
LIMIT 20;
