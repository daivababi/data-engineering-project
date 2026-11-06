FROM apache/airflow:2.8.1

COPY requirements-dbt.txt requirements-airflow.txt /tmp/
RUN pip install --no-cache-dir \
    -r /tmp/requirements-dbt.txt \
    -r /tmp/requirements-airflow.txt
