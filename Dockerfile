FROM postgres:16.4

# Set up the environment for running the Ridango API poll script
RUN apt-get update \
    && apt-get install -y --no-install-recommends python3 python3-venv \
    && rm -rf /var/lib/apt/lists/* \
    && python3 -m venv /opt/venv
ENV PATH="/opt/venv/bin:$PATH"
WORKDIR /app
COPY requirements-poller.txt /tmp/requirements-poller.txt
RUN pip install --no-cache-dir -r /tmp/requirements-poller.txt
COPY src/poll_ridango.py src/clean_ridango.py src/generate_stop_events.py /app/src/

RUN mkdir -p /app/data/raw
COPY data/raw/ /app/data/raw/

COPY ddl/01_create_dim_tables.sql /docker-entrypoint-initdb.d/01_create_dim_tables.sql
COPY dml/01_sample_dim_data.sql /docker-entrypoint-initdb.d/01_sample_dim_data.sql
COPY ddl/02_create_fact_table.sql /docker-entrypoint-initdb.d/02_create_fact_table.sql
COPY dml/02_sample_fact_data.sql /docker-entrypoint-initdb.d/02_sample_fact_data.sql
COPY ddl/04_create_vehicle_observation.sql /docker-entrypoint-initdb.d/04_create_vehicle_observation.sql
COPY ddl/05_create_observed_stop_arrivals.sql /docker-entrypoint-initdb.d/05_create_observed_stop_arrivals.sql
