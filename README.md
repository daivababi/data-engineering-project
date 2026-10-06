# Tartu Bus Delays — Data Engineering Project

**Group 18** — Data Architecture & Modeling

## Project Member Roles and Contributions

- Daiva Babi — attended the project plan discussion meeting (25%)
- Peep Kolberg — attended the project plan discussion meeting (25%)
- Inga Tallinn — attended the project plan discussion meeting (25%)
- Tiina Uuk — attended the project plan discussion meeting (25%)

## Business Brief

The goal of the project is to build a dashboard for monitoring real-time bus operations in Tartu and analyze delays in the bus service.

The main stakeholders of the project are the **City of Tartu**, **public transportation operators**, and **public transportation users**.

## KPIs

1. Average schedule delay (minutes), aggregated by bus line, date, and hour of day
2. Percentage of bus trips with a severe delay (6+ min), by bus line
3. Percentage of stop arrivals on time (up to 1 min delay), by bus line

## Business Questions

Our model answers the following business questions:

1. What percentage of arrivals to a stop are either on time (up to 1 min delay), moderately delayed (2–5 min) or severely delayed (6+ min)?
2. Which bus lines have the highest proportion of delays?
3. Which bus stops have the highest proportion of delays?
4. At what time of the day and day of the week are delays most frequent?
5. How does the reliability of the service change over time?

## Datasets

We use three external data sources and construct one derived dataset:

1. **Tartu bus timetable** (122k rows, 11 columns). Contains scheduled trips and stop arrival times. Provides the expected service schedule against which observed bus operations will be compared.
2. **Tartu bus stops**. Contains bus-stop identifiers, names and coordinates. Coordinates are used together with GPS observations to detect bus arrivals at stops.
3. **Ridango real-time bus data**. The Ridango API provides near-real-time observations of buses currently operating in Tartu, including bus line, bus and trip IDs, trip start time and date, observation time, and GPS coordinates. The API is queried every 15 seconds and observations are stored to build a dataset of observations.
4. **Derived stop-arrival dataset** (will grow to 1000+ rows, 8 columns). Real-time observations are combined with the timetable and stop locations to detect stop-arrival events. Each event contains line, trip, stop, scheduled arrival, actual arrival, calculated schedule delay, delay severity, and delay-causing stop. This dataset forms the basis of the analytical fact table and dashboard metrics.

## Tooling

- **Airflow** — orchestrates the data pipeline, periodically retrieves Ridango API data and triggers downstream processing tasks.
- **PostgreSQL** — stores collected data and implements the analytical star schema.
- **dbt** — transforms raw data into the dimensional model and performs data quality tests.
- **Streamlit or Superset** — final dashboard and visualization of the KPIs (decision to be made after the respective practice session).
- **Docker** — everything runs in Docker for reproducibility.

## Data Architecture

![Data Architecture](docs/architecture.png)

Pipeline stages:

1. **Data sources** — Ridango API, Bus timetable, Bus stops
2. **Ingestion methods** — API poll every 15 seconds; load file for timetable and stops
3. **Storage** — Raw PostgreSQL (Ridango), PostgreSQL (timetable, stops)
4. **Transformation** — dbt (with quality checks, e.g. verify that bus coordinates reported by Ridango are within Tartu city limits)
5. **Warehouse** — Processed PostgreSQL
6. **Reporting** — Dashboard (Streamlit / Superset)

## Data Model

Star schema with one fact table and three dimension tables:

- **fact_stop_arrival** — `arrival_key`, `date_key`, `stop_key`, `trip_key`, `delay_causing_stop`, `scheduled_arrival`, `actual_arrival`, `delay_minutes`, `delay_severity`
- **dim_date** — `date_key`, `full_date`, `day_of_week`
- **dim_stop** — `stop_key`, `stop_name`, `latitude`, `longitude`
- **dim_trip** — `trip_key`, `stop_key`, `line_no`, `route`, `scheduled_arrival`

![Star Schema](docs/schema.png)

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
