-- Business Question 1:
-- What percentage of arrivals to a stop are either on time (up to 1 min delay),
--  moderately delayed (2-5 min) or severely delayed (6+ min)?

WITH totals AS (
    SELECT
        SUM(total_arrivals) AS grand_total,
        SUM(total_arrivals * on_time_pct) AS on_time_weighted,
        SUM(total_arrivals * moderate_delay_pct) AS moderate_weighted,
        SUM(total_arrivals * severe_delay_pct) AS severe_weighted
    FROM gold.stop_arrival_stats
)
SELECT 'on_time' AS delay_severity,
       ROUND(on_time_weighted / grand_total, 2) AS percentage
FROM totals
UNION ALL
SELECT 'moderate',
       ROUND(moderate_weighted / grand_total, 2)
FROM totals
UNION ALL
SELECT 'severe',
       ROUND(severe_weighted / grand_total, 2)
FROM totals
ORDER BY percentage DESC;