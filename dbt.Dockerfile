FROM python:3.11.18-slim-bookworm

COPY requirements-dbt.txt /tmp/requirements-dbt.txt
RUN pip install --no-cache-dir -r /tmp/requirements-dbt.txt

WORKDIR /dbt

CMD ["tail", "-f", "/dev/null"]
