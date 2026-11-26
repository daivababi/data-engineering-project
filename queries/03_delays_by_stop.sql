-- Business Question 3:
-- Which bus stops have the highest proportion of delays?

SELECT
    stop_name,
    SUM(total_arrivals) AS total_arrivals,
    ROUND(
        SUM(total_arrivals * (100.0 - on_time_pct)) / SUM(total_arrivals),
        2
    ) AS delay_percentage
FROM gold.stop_arrival_stats
GROUP BY stop_name
HAVING SUM(total_arrivals) > 20
ORDER BY delay_percentage DESC
LIMIT 20;