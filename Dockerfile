FROM postgres:16-alpine

COPY ddl/01_create_dim_tables.sql /docker-entrypoint-initdb.d/01_create_dim_tables.sql
COPY dml/01_sample_dim_data.sql /docker-entrypoint-initdb.d/01_sample_dim_data.sql
COPY ddl/02_create_fact_table.sql /docker-entrypoint-initdb.d/02_create_fact_table.sql
COPY dml/02_sample_fact_data.sql /docker-entrypoint-initdb.d/02_sample_fact_data.sql
