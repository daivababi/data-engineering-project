# Tartu Bus Delays — Data Engineering Project

**Group 18** — Data Architecture & Modeling

## Data Dictionary

### fact_stop_arrival

Stores all events of a bus reaching a stop on its route.

| Column | Data Type | Description |
|---|---|---|
| arrival_key | INT | PK, identifies an arrival |
| arrival_date_key | INT | FK to dim_date |
| stop_key | INT | FK to dim_stop |
| trip_key | INT | FK to dim_trip |
| delay_causing_stop | INT | FK to dim_stop, identifying the stop that caused the first delay of this trip |
| scheduled_arrival | TIME | Time of scheduled arrival to this stop |
| actual_arrival | TIME | Time of actual arrival to this stop |
| delay_minutes | INT | Difference in minutes between the scheduled and actual arrival times |
| delay_severity | VARCHAR(20) | Delay severity category |

### dim_trip

Stores the bus schedule.

| Column | Data Type | Description |
|---|---|---|
| trip_key | INT | PK, identifies a trip stop |
| stop_key | INT | FK to dim_stop |
| valid_from_date_key | INT | FK to dim_date, identifying from which date the trip schedule is valid |
| valid_to_date_key | INT | FK to dim_date, identifying until which date the trip schedule is valid |
| scheduled_arrival | TIME | Scheduled arrival at the stop |
| line_no | VARCHAR(20) | Bus line associated with the trip |
| route | VARCHAR(100) | Route associated with the trip |

### dim_stop

Stores the bus stops' name and location.

| Column | Data Type | Description |
|---|---|---|
| stop_key | INT | PK, identifies a trip stop |
| valid_from_date_key | INT | FK to dim_date, identifying from which date the stop information is valid |
| valid_to_date_key | INT | FK to dim_date, identifying until which date the stop information is valid |
| stop_name | VARCHAR(100) | Name of the bus stop |
| longitude | DECIMAL(9,6) | Longitude coordinate of the bus stop |
| latitude | DECIMAL(8,6) | Latitude coordinate of the bus stop |

### dim_date

Stores date information.

| Column | Data Type | Description |
|---|---|---|
| date_key | INT | PK, identifies a calendar date |
| full_date | DATE | Full calendar date |
| day_of_week | VARCHAR(10) | Name of the weekday |

## Demo Queries

### 1. What percentage of arrivals to a stop are either on time (up to 1 min delay), moderately delayed (2–5 min) or severely delayed (6+ min)?

```sql
SELECT
    delay_severity,
    COUNT(*) AS arrival_count,
    ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) AS percentage
FROM fact_stop_arrival
GROUP BY delay_severity
ORDER BY percentage DESC;
```

### 2. Which bus lines have the highest proportion of delays?

```sql
SELECT
    t.line_no,
    COUNT(*) AS total_arrivals,
    SUM(CASE WHEN f.delay_minutes >= 2 THEN 1 ELSE 0 END) AS delayed_arrivals,
    ROUND(
        100.0 * SUM(CASE WHEN f.delay_minutes >= 2 THEN 1 ELSE 0 END) / COUNT(*),
        2
    ) AS delay_percentage
FROM fact_stop_arrival f
JOIN dim_trip t ON f.trip_key = t.trip_key
GROUP BY t.line_no
HAVING COUNT(*) > 10
ORDER BY delay_percentage DESC;
```

### 3. Which bus stops have the highest proportion of delays?

```sql
SELECT
    s.stop_name,
    COUNT(*) AS total_arrivals,
    SUM(CASE WHEN f.delay_minutes >= 2 THEN 1 ELSE 0 END) AS delayed_arrivals,
    ROUND(
        100.0 * SUM(CASE WHEN f.delay_minutes >= 2 THEN 1 ELSE 0 END) / COUNT(*),
        2
    ) AS delay_percentage
FROM fact_stop_arrival f
JOIN dim_stop s ON f.stop_key = s.stop_key
GROUP BY s.stop_name
HAVING COUNT(*) > 20
ORDER BY delay_percentage DESC
LIMIT 20;
```

### 4. At what time of the day and day of the week are delays most frequent?

```sql
SELECT
    d.day_of_week,
    EXTRACT(HOUR FROM f.scheduled_arrival) AS hour_of_day,
    COUNT(*) AS total_arrivals,
    SUM(CASE WHEN f.delay_minutes >= 2 THEN 1 ELSE 0 END) AS delayed_arrivals,
    ROUND(
        100.0 * SUM(CASE WHEN f.delay_minutes >= 2 THEN 1 ELSE 0 END) / COUNT(*),
        2
    ) AS delay_percentage
FROM fact_stop_arrival f
JOIN dim_date d ON f.date_key = d.date_key
GROUP BY d.day_of_week, EXTRACT(HOUR FROM f.scheduled_arrival)
HAVING COUNT(*) > 5
ORDER BY delay_percentage DESC;
```

### 5. How does the reliability of the service change over time? (monthly trend)

```sql
SELECT
    EXTRACT(YEAR FROM d.full_date) AS year,
    EXTRACT(MONTH FROM d.full_date) AS month,
    COUNT(*) AS total_arrivals,
    ROUND(AVG(f.delay_minutes), 2) AS avg_delay_minutes,
    ROUND(100.0 * SUM(CASE WHEN f.delay_minutes <= 1 THEN 1 ELSE 0 END) / COUNT(*), 2) AS on_time_pct,
    ROUND(100.0 * SUM(CASE WHEN f.delay_minutes BETWEEN 2 AND 5 THEN 1 ELSE 0 END) / COUNT(*), 2) AS moderate_delay_pct,
    ROUND(100.0 * SUM(CASE WHEN f.delay_minutes >= 6 THEN 1 ELSE 0 END) / COUNT(*), 2) AS severe_delay_pct
FROM fact_stop_arrival f
JOIN dim_date d ON f.date_key = d.date_key
GROUP BY
    EXTRACT(YEAR FROM d.full_date),
    EXTRACT(MONTH FROM d.full_date)
ORDER BY year, month;
```
```

## Repository Structure

```text
.
├── ddl/                                # DDL scripts — star schema creation
│   ├── 01_create_dim_tables.sql        # Dimension tables: dim_date, dim_stop, dim_trip
│   └── 02_create_fact_table.sql        # Fact table: fact_stop_arrival with indexes
├── dml/                                # DML scripts — sample data
│   ├── 01_sample_dim_data.sql          # Sample rows for dimension tables
│   └── 02_sample_fact_data.sql         # Sample rows for fact table
├── queries/                            # SQL answering the 5 business questions
│   ├── 01_delay_categories.sql         # Q1: distribution of arrivals by delay category
│   ├── 02_delays_by_line.sql           # Q2: delays by bus line
│   ├── 03_delays_by_stop.sql           # Q3: delays by bus stop
│   ├── 04_delays_by_time.sql           # Q4: delays by time of day / weekday
│   └── 05_reliability_over_time.sql    # Q5: reliability trend over time
└── docs/                               # Architecture and schema diagrams
```
## How to Run

```bash
psql -d tartu_bus -f ddl/01_create_dim_tables.sql
psql -d tartu_bus -f ddl/02_create_fact_table.sql
psql -d tartu_bus -f dml/01_sample_dim_data.sql
psql -d tartu_bus -f dml/02_sample_fact_data.sql
psql -d tartu_bus -f queries/01_delay_categories.sql
```
