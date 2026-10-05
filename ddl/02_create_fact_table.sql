DROP TABLE IF EXISTS fact_stop_arrival CASCADE;

CREATE TABLE fact_stop_arrival (
    arrival_key          SERIAL PRIMARY KEY,
    date_key             INT REFERENCES dim_date(date_key),
    stop_key             INT REFERENCES dim_stop(stop_key),
    trip_key             INT REFERENCES dim_trip(trip_key),
    delay_causing_stop   INT REFERENCES dim_stop(stop_key),
    scheduled_arrival    TIMESTAMP NOT NULL,
    actual_arrival       TIMESTAMP NOT NULL,
    delay_minutes        INT NOT NULL,
    delay_severity       VARCHAR(20) NOT NULL
        CHECK (delay_severity IN ('on_time', 'moderate', 'severe'))
);

-- Indexes for faster analytical queries
CREATE INDEX idx_fact_date        ON fact_stop_arrival(date_key);
CREATE INDEX idx_fact_stop        ON fact_stop_arrival(stop_key);
CREATE INDEX idx_fact_trip        ON fact_stop_arrival(trip_key);
CREATE INDEX idx_fact_severity    ON fact_stop_arrival(delay_severity);
CREATE INDEX idx_fact_delay_min   ON fact_stop_arrival(delay_minutes);
