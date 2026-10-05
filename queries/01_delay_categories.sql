-- Business Question 1:
-- What percentage of arrivals to a stop are either on time
-- (up to 1 min delay), moderately delayed (2-5 min) or
-- severely delayed (6+ min)?

SELECT
    delay_severity,
    COUNT(*) AS arrival_count,
    ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) AS percentage
FROM fact_stop_arrival
GROUP BY delay_severity
ORDER BY percentage DESC;
