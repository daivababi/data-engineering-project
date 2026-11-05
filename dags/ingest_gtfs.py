from __future__ import annotations

import csv
import os
import re
from pathlib import Path

import psycopg2
from airflow.decorators import dag, task
from psycopg2 import sql

GTFS_DIR = Path("/opt/airflow/data/raw")
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
        files = sorted(GTFS_DIR.glob("*.txt"))
        if not files:
            raise FileNotFoundError(f"No GTFS .txt files found in {GTFS_DIR}")

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
                    cur.execute(
                        sql.SQL("CREATE TABLE IF NOT EXISTS {} ({})").format(
                            table,
                            sql.SQL(", ").join(
                                sql.SQL("{} TEXT").format(sql.Identifier(column))
                                for column in columns
                            ),
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
                            "loaded_at TIMESTAMPTZ NOT NULL DEFAULT now()"
                        ).format(table)
                    )

                    cur.execute(
                        """
                        SELECT column_name, data_type
                        FROM information_schema.columns
                        WHERE table_schema = 'bronze' AND table_name = %s
                        """,
                        (table_name,),
                    )
                    existing_types = dict(cur.fetchall())
                    for column in columns:
                        if existing_types[column] != "text":
                            cur.execute(
                                sql.SQL(
                                    "ALTER TABLE {} ALTER COLUMN {} TYPE TEXT USING {}::TEXT"
                                ).format(
                                    table,
                                    sql.Identifier(column),
                                    sql.Identifier(column),
                                )
                            )

                    cur.execute(sql.SQL("TRUNCATE TABLE {}").format(table))
                    copy_statement = sql.SQL(
                        "COPY {} ({}) FROM STDIN WITH (FORMAT CSV, HEADER TRUE)"
                    ).format(
                        table,
                        sql.SQL(", ").join(sql.Identifier(column) for column in columns),
                    )
                    with path.open(encoding="utf-8-sig", newline="") as source:
                        cur.copy_expert(copy_statement.as_string(conn), source)
                    print(f"Loaded {cur.rowcount} rows into bronze.{table_name}")
        finally:
            conn.close()

    load_gtfs()

ingest_gtfs()
