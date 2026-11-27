# Tartu Bus Delays — Data Engineering Project

**Group 18** — Data Architecture & Modeling

## Current GTFS/dbt pipeline

The `update_gtfs` Airflow DAG downloads a GTFS feed, assigns a content-based
version, and passes the same version and feed path to `ingest_gtfs` and
`ingest_stops`. Bronze keeps rows for different `file_version` values; reloading
the same version replaces or upserts that version's rows without removing other
versions. For manual ingestion, `ingest_gtfs` reads `.txt` files from `data/raw`
by default, while `ingest_stops` loads `stops.txt` separately.

The version-aware Silver timetable models use the version with the latest
`bronze.raw_stops.loaded_at` as current and retain `file_version` for lineage.
This assumes the orchestrated feed load completes the stops ingestion after
the other GTFS tables. Rebuild and test these models with:

```bash
docker compose exec dbt dbt build --select path:models/silver
```

Schedule stop IDs missing from the current Bronze stops feed fail a dbt test.
`silver_stops` retains active Tartu stops and active stops outside Tartu that
are referenced by the current timetable, including the 55 stops identified
above. Scheduled stops that still cannot join to a Silver stop are reported by
a warning test.

`dim_trip` has one row per GTFS `trip_id` and stores trip-level details such
as route, service, direction, and headsign. Scheduled stop times are held in
`silver.silver_scheduled_arrivals`. The persistent Ridango services write
inferred stop events to `silver.observed_stop_arrival`; this is staging data
for the final dbt-managed analytical fact table.

### GTFS history work status

The Silver current-version filtering and `file_version` lineage are implemented
and tested. SCD2 history is not yet complete. The existing
`dbt/snapshots/dim_stop_snapshot.sql` remains unchanged; its canonical ownership,
the entities to track, and the policy for records removed from a later feed
still need agreement with Daiva. Snapshot execution and the full post-ingestion
refresh workflow also remain to be finalized. Do not treat stop-history handling
as complete until those decisions and changes are made.

## Demo Queries

### 1. What percentage of arrivals to a stop are either on time (up to 1 min delay), moderately delayed (2–5 min) or severely delayed (6+ min)?

```sql
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
```

### 2. Which bus lines have the highest proportion of delays?

```sql
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
```

### 3. Which bus stops have the highest proportion of delays?

```sql
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
```

### 4. At what time of the day and day of the week are delays most frequent?

```sql
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
FROM gold.stop_arrival_stats AS s
JOIN gold.dim_date AS d ON s.date = d.full_date
GROUP BY d.day_of_week, s.hour_of_day, s.line_no, s.stop_name
HAVING SUM(s.total_arrivals) > 5
ORDER BY delay_percentage DESC
LIMIT 20;
```

### 5. How does the reliability of the service change over time? (monthly trend)

```sql
SELECT
    EXTRACT(YEAR FROM date) AS year,
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
```

## Repository Structure

```text
.
├── queries/                            # SQL answering the 5 business questions
│   ├── 01_delay_categories.sql         # Q1: distribution of arrivals by delay category
│   ├── 02_delays_by_line.sql           # Q2: delays by bus line
│   ├── 03_delays_by_stop.sql           # Q3: delays by bus stop
│   ├── 04_delays_by_time.sql           # Q4: delays by time of day / weekday
│   └── 05_reliability_over_time.sql    # Q5: reliability trend over time
├── dags/                               # Airflow ingestion DAGs
├── dbt/models/
│   ├── silver/                         # Cleaned GTFS models
│   └── gold/                           # Analytics dimensions
├── data/raw/                           # Local GTFS feed files
└── docs/                               # Architecture and schema diagrams
```
## Run the pipeline

1. Start the services with `docker compose up -d`.
2. Trigger the `update_gtfs` DAG in Airflow to ingest the current feed.
3. Build and test the version-aware Silver models:

   ```bash
   docker compose exec dbt dbt build --select path:models/silver
   ```

4. Run the five SQL files in `queries/` against the `tartu_bus` database. They
   query the current dbt-managed `gold.stop_arrival_stats` model.

## Load the GTFS feed into Bronze

The `ingest_gtfs` Airflow DAG loads each `.txt` file in `data/raw` except
`stops.txt` into a `bronze.raw_<filename>` table. Source columns are stored as
text, with `file_version` and `loaded_at` for version lineage and load tracking.
It replaces rows only for the run's version; other versions remain. Stops are
loaded separately by `ingest_stops`.

For normal updates, trigger `update_gtfs` in Airflow; it coordinates both
ingestion DAGs. After both succeed, the version-aware Silver models can be
built with the command above. If a GTFS file is absent, its previous Bronze
table is not automatically removed.
