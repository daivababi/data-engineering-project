-- Business Question 5:
-- How does the reliability of the service change over time?
-- Monthly trend with all three delay categories.

SELECT
    EXTRACT(YEAR  FROM d.full_date) AS year,
    EXTRACT(MONTH FROM d.full_date) AS month,
    COUNT(*) AS total_arrivals,
    ROUND(AVG(f.delay_minutes), 2) AS avg_delay_minutes,
    ROUND(100.0 * SUM(CASE WHEN f.delay_minutes <= 1 THEN 1 ELSE 0 END)
          / COUNT(*), 2) AS on_time_pct,
    ROUND(100.0 * SUM(CASE WHEN f.delay_minutes BETWEEN 2 AND 5 THEN 1 ELSE 0 END)
          / COUNT(*), 2) AS moderate_delay_pct,
    ROUND(100.0 * SUM(CASE WHEN f.delay_minutes >= 6 THEN 1 ELSE 0 END)
          / COUNT(*), 2) AS severe_delay_pct
FROM fact_stop_arrival f
JOIN dim_date d ON f.date_key = d.date_key
GROUP BY EXTRACT(YEAR FROM d.full_date), EXTRACT(MONTH FROM d.full_date)
ORDER BY year, month;
