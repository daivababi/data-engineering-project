from __future__ import annotations

import csv
import os
import re
from pathlib import Path

import psycopg2
from airflow.decorators import dag, task
from psycopg2 import sql
from airflow.operators.python import get_current_context

GTFS_DIR = Path(
    os.getenv("GTFS_DIR", "/opt/airflow/data/raw")
)
IDENTIFIER_PATTERN = re.compile(r"^[a-z][a-z0-9_]*$")


@dag(
    dag_id="ingest_gtfs",
    schedule=None,
    catchup=False,
    tags=["bronze", "gtfs"],
)
def ingest_gtfs():
    @task
    def load_gtfs() -> None:
        context = get_current_context()

        dag_run = context.get("dag_run")
        conf = dag_run.conf if dag_run and dag_run.conf else {}

        gtfs_version = conf.get("gtfs_version", "manual")
        gtfs_path = conf.get("gtfs_path")

        if gtfs_path:
            gtfs_dir = Path(gtfs_path)
        else:
            gtfs_dir = GTFS_DIR

        files = sorted(path for path in gtfs_dir.glob("*.txt") if path.name != "stops.txt")
        if not files:
            raise FileNotFoundError(f"No GTFS .txt files found in {gtfs_dir}")

        conn = psycopg2.connect(
            host=os.environ["GTFS_DB_HOST"],
            port=os.environ["GTFS_DB_PORT"],
            dbname=os.environ["GTFS_DB_NAME"],
            user=os.environ["GTFS_DB_USER"],
            password=os.environ["GTFS_DB_PASSWORD"],
        )
        try:
            with conn, conn.cursor() as cur:
                cur.execute("CREATE SCHEMA IF NOT EXISTS bronze")

                for path in files:
                    table_name = f"raw_{path.stem}"
                    if not IDENTIFIER_PATTERN.fullmatch(table_name):
                        raise ValueError(f"Invalid GTFS table name from file: {path.name}")

                    with path.open(encoding="utf-8-sig", newline="") as source:
                        reader = csv.reader(source)
                        columns = next(reader, None)
                        if not columns or any(
                            not column or not IDENTIFIER_PATTERN.fullmatch(column)
                            for column in columns
                        ):
                            raise ValueError(f"Invalid or missing CSV headers in {path.name}")
                        if len(columns) != len(set(columns)):
                            raise ValueError(f"Duplicate CSV headers in {path.name}")

                    table = sql.Identifier("bronze", table_name)
                    stage = sql.Identifier(f"stage_{table_name}")

                    column_definitions = sql.SQL(", ").join(
                        sql.SQL("{} TEXT").format(sql.Identifier(column)) for column in columns
                    )
                    cur.execute(
                        sql.SQL("CREATE TABLE IF NOT EXISTS {} ({})").format(
                            table, column_definitions
                        )
                    )

                    for column in columns:
                        cur.execute(
                            sql.SQL("ALTER TABLE {} ADD COLUMN IF NOT EXISTS {} TEXT").format(
                                table, sql.Identifier(column)
                            )
                        )
                    cur.execute(
                        sql.SQL(
                            "ALTER TABLE {} ADD COLUMN IF NOT EXISTS "
                            "file_version TEXT"
                        ).format(table)
                    )
                    cur.execute(
                        sql.SQL(
                            "CREATE TEMP TABLE {} ({}) ON COMMIT DROP"
                        ).format(stage, column_definitions)
                    )
                    copy_statement = sql.SQL(
                        "COPY {} ({}) FROM STDIN WITH (FORMAT CSV, HEADER TRUE)"
                    ).format(
                        stage,
                        sql.SQL(", ").join(
                            sql.Identifier(column) for column in columns
                        ),
                    )
                    with path.open(
                        encoding="utf-8-sig", newline=""
                    ) as source:
                        cur.copy_expert(
                            copy_statement.as_string(conn), source
                        )
                    cur.execute(
                        sql.SQL(
                            "DELETE FROM {} WHERE file_version = %s"
                        ).format(table),
                        (gtfs_version,),
                    )
                    insert_columns = sql.SQL(", ").join(
                        [sql.Identifier(column) for column in columns]
                        + [sql.Identifier("file_version")]
                    )

                    select_columns = sql.SQL(", ").join(
                        sql.Identifier(column) for column in columns
                    )

                    insert_statement = sql.SQL(
                        "INSERT INTO {} ({}) SELECT {}, %s FROM {}"
                    ).format(
                        table,
                        insert_columns,
                        select_columns,
                        stage,
                    )

                    cur.execute(insert_statement, (gtfs_version,))

                    print(f"Loaded {cur.rowcount} rows into bronze.{table_name}, version {gtfs_version}")
        finally:
            conn.close()

    load_gtfs()

ingest_gtfs()
