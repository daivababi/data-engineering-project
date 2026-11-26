-- Business Question 5:
-- How does the reliability of the service change over time?
-- Monthly trend with all three delay categories.

SELECT
    EXTRACT(YEAR  FROM date) AS year,
    EXTRACT(MONTH FROM date) AS month,
    SUM(total_arrivals) AS total_arrivals,
    ROUND(
        SUM(total_arrivals * avg_delay_minutes) / SUM(total_arrivals),
        2
    ) AS avg_delay_minutes,
    ROUND(
        SUM(total_arrivals * on_time_pct) / SUM(total_arrivals),
        2
    ) AS on_time_pct,
    ROUND(
        SUM(total_arrivals * moderate_delay_pct) / SUM(total_arrivals),
        2
    ) AS moderate_delay_pct,
    ROUND(
        SUM(total_arrivals * severe_delay_pct) / SUM(total_arrivals),
        2
    ) AS severe_delay_pct
FROM gold.stop_arrival_stats
GROUP BY EXTRACT(YEAR FROM date), EXTRACT(MONTH FROM date)
ORDER BY year, month;