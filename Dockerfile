FROM postgres:16-alpine

# Set up the environment for running the Ridango API poll script
RUN apk add --no-cache python3 py3-pip py3-virtualenv tini \
    && virtualenv /opt/venv
ENV PATH="/opt/venv/bin:$PATH" \
    PYTHONUNBUFFERED=1 \
    POLL_INTERVAL_SECONDS=15 \
    RIDANGO_OUTPUT_DIR=/app/data/ridango_responses
WORKDIR /app
COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt
COPY src/poll_ridango.py /app/src/poll_ridango.py
COPY docker-entrypoint-with-poller.sh /usr/local/bin/docker-entrypoint-with-poller
RUN chmod +x /usr/local/bin/docker-entrypoint-with-poller

# 
COPY ddl/01_create_dim_tables.sql /docker-entrypoint-initdb.d/01_create_dim_tables.sql
COPY dml/01_sample_dim_data.sql /docker-entrypoint-initdb.d/01_sample_dim_data.sql
COPY ddl/02_create_fact_table.sql /docker-entrypoint-initdb.d/02_create_fact_table.sql
COPY dml/02_sample_fact_data.sql /docker-entrypoint-initdb.d/02_sample_fact_data.sql

ENTRYPOINT ["/sbin/tini", "-g", "--", "/usr/local/bin/docker-entrypoint-with-poller"]
CMD ["postgres"]
