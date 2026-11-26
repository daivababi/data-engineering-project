from __future__ import annotations

import csv
import os
from datetime import datetime, timedelta
from pathlib import Path

import psycopg2
from airflow.decorators import dag, task
from airflow.models.param import Param
from airflow.operators.python import get_current_context

STOPS_FILE = os.getenv(
    "STOPS_FILE",
    "/opt/airflow/data/raw/stops.txt"
)
REQUIRED_COLUMNS = {"stop_id", "stop_name", "stop_lat", "stop_lon"}
BATCH_SIZE = 1000


def is_in_estonia(lat: float, lon: float) -> bool:
    return 57.5 <= lat <= 59.7 and 21.7 <= lon <= 28.3


@dag(
    dag_id="ingest_stops",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    params={
        "file_path": Param(STOPS_FILE, type="string"),
        "file_version": Param("v1", type="string"),
    },
    default_args={"retries": 1, "retry_delay": timedelta(minutes=5)},
    tags=["bronze", "stops"],
)
def ingest_stops():
    @task
    def extract(**context) -> str:
        ctx = get_current_context()

        gtfs_path = None

        if ctx.get("dag_run") and ctx["dag_run"].conf:
            gtfs_path = ctx["dag_run"].conf.get("gtfs_path")

        if gtfs_path:
            path = Path(gtfs_path) / "stops.txt"
        else:
            path = Path(context["params"]["file_path"])
        if not path.exists():
            raise FileNotFoundError(path)
        return str(path)

    @task
    def quality_check(file_path: str) -> int:
        row_count = 0
        skipped_count = 0
        seen_ids = set()
        with Path(file_path).open(encoding="utf-8-sig", newline="") as source:
            reader = csv.DictReader(source)
            if not reader.fieldnames or not REQUIRED_COLUMNS.issubset(reader.fieldnames):
                raise ValueError(
                    "Missing required stops.txt columns: "
                    f"{sorted(REQUIRED_COLUMNS - set(reader.fieldnames or []))}"
                )

            for row_number, row in enumerate(reader, start=2):
                stop_id = (row.get("stop_id") or "").strip()
                stop_name = (row.get("stop_name") or "").strip()
                if not stop_id:
                    raise ValueError(f"Null stop_id at CSV row {row_number}")
                if not stop_name:
                    raise ValueError(f"Null stop_name at CSV row {row_number}")
                if stop_id in seen_ids:
                    raise ValueError(f"Duplicate stop_id at CSV row {row_number}: {stop_id}")
                seen_ids.add(stop_id)
                try:
                    lat = float(row["stop_lat"])
                    lon = float(row["stop_lon"])
                except (TypeError, ValueError) as error:
                    raise ValueError(
                        f"Invalid coordinates at CSV row {row_number}: {stop_id}"
                    ) from error
                if not is_in_estonia(lat, lon):
                    skipped_count += 1
                    continue
                row_count += 1

        if not row_count:
            raise ValueError("No valid rows extracted")
        print(f"Quality check passed for {row_count} rows")
        if skipped_count:
            print(f"Skipped {skipped_count} rows with coordinates outside Estonia")
        return row_count

    @task
    def load(file_path: str, expected_count: int, **context) -> None:
        ctx = get_current_context()

        version = None

        if ctx.get("dag_run") and ctx["dag_run"].conf:
            version = ctx["dag_run"].conf.get("gtfs_version")

        if not version:
            version = context["params"]["file_version"]

        conn = psycopg2.connect(
            host=os.environ["GTFS_DB_HOST"],
            port=os.environ["GTFS_DB_PORT"],
            dbname=os.environ["GTFS_DB_NAME"],
            user=os.environ["GTFS_DB_USER"],
            password=os.environ["GTFS_DB_PASSWORD"],
        )
        insert_sql = """
            INSERT INTO bronze.raw_stops
                (stop_id, stop_name, stop_lat, stop_lon, stop_area, authority, file_version)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (stop_id, file_version) DO UPDATE SET
                stop_name    = EXCLUDED.stop_name,
                stop_lat     = EXCLUDED.stop_lat,
                stop_lon     = EXCLUDED.stop_lon,
                stop_area    = EXCLUDED.stop_area,
                authority    = EXCLUDED.authority,
                is_active    = TRUE,
                loaded_at    = now();
        """
        loaded_count = 0
        skipped_count = 0
        try:
            with conn, conn.cursor() as cur:
                cur.execute("""
                    CREATE SCHEMA IF NOT EXISTS bronze;
                    CREATE TABLE IF NOT EXISTS bronze.raw_stops (
                        stop_id      TEXT NOT NULL,
                        stop_name    TEXT NOT NULL,
                        stop_lat     DOUBLE PRECISION,
                        stop_lon     DOUBLE PRECISION,
                        stop_area    TEXT,
                        authority    TEXT,
                        file_version TEXT NOT NULL,
                        is_active    BOOLEAN NOT NULL DEFAULT TRUE,
                        loaded_at    TIMESTAMPTZ DEFAULT now(),
                        PRIMARY KEY (stop_id, file_version)
                    );
                """)
                cur.execute(
                    "ALTER TABLE bronze.raw_stops "
                    "ADD COLUMN IF NOT EXISTS file_version TEXT"
                )
                cur.execute(
                    "ALTER TABLE bronze.raw_stops "
                    "ADD COLUMN IF NOT EXISTS is_active BOOLEAN NOT NULL DEFAULT TRUE"
                )
        
                with Path(file_path).open(
                    encoding="utf-8-sig", newline=""
                ) as source:
                    reader = csv.DictReader(source)
                    batch = []
                    for row in reader:
                        lat = float(row["stop_lat"])
                        lon = float(row["stop_lon"])
                        if not is_in_estonia(lat, lon):
                            skipped_count += 1
                            continue
                        batch.append((
                            row["stop_id"],
                            row["stop_name"],
                            lat,
                            lon,
                            row.get("stop_area"),
                            row.get("authority"),
                            version,
                        ))
                        if len(batch) == BATCH_SIZE:
                            cur.executemany(insert_sql, batch)
                        
                            loaded_count += len(batch)
                            batch.clear()
                    if batch:
                        cur.executemany(insert_sql, batch)
                        
                        loaded_count += len(batch)
                if loaded_count != expected_count:
                    raise ValueError(
                        f"CSV changed after quality check: expected "
                        f"{expected_count} rows, loaded {loaded_count}"
                    )
                
              
        finally:
            conn.close()
        print(
            f"Loaded {loaded_count} rows "
            f"and skipped {skipped_count} out-of-bounds rows "
            f"(version={version})"
        )

    file_path = extract()
    row_count = quality_check(file_path)
    load(file_path, row_count)


ingest_stops()
