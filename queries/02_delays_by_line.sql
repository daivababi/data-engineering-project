-- Business Question 2:
-- Which bus lines have the highest proportion of delays?

SELECT
    line_no,
    SUM(total_arrivals) AS total_arrivals,
    ROUND(
        SUM(total_arrivals * (100.0 - on_time_pct)) / SUM(total_arrivals),
        2
    ) AS delay_percentage
FROM gold.stop_arrival_stats
GROUP BY line_no
HAVING SUM(total_arrivals) > 10
ORDER BY delay_percentage DESC;