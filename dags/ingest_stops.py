from __future__ import annotations

import csv
from datetime import datetime, timedelta
from pathlib import Path

import psycopg2
from airflow.decorators import dag, task
from airflow.models.param import Param

STOPS_FILE = "/opt/airflow/data/raw/stops.txt"
PG_CONN = "postgresql://postgres:postgres@postgres:5432/tartucitybuses"


@dag(
    dag_id="ingest_stops",
    start_date=datetime(2026, 1, 1),
    schedule="@daily",
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
    def extract(**context) -> list[dict]:
        path = Path(context["params"]["file_path"])
        if not path.exists():
            raise FileNotFoundError(path)
        with path.open(encoding="utf-8") as f:
            return list(csv.DictReader(f))

    @task
    def quality_check(rows: list[dict]) -> None:
        if not rows:
            raise ValueError("No rows extracted")
        ids = [r["stop_id"] for r in rows]
        if any(not r["stop_id"] for r in rows):
            raise ValueError("Null stop_id")
        if any(not r["stop_name"] for r in rows):
            raise ValueError("Null stop_name")
        if len(ids) != len(set(ids)):
            raise ValueError("Duplicate stop_id")
        for r in rows:
            lat, lon = float(r["stop_lat"]), float(r["stop_lon"])
            if not (57.5 <= lat <= 59.7 and 21.7 <= lon <= 28.3):
                raise ValueError(f"Coordinates out of Estonia bbox: {r['stop_id']}")
        print(f"Quality check passed for {len(rows)} rows")

    @task
    def load(rows: list[dict], **context) -> None:
        version = context["params"]["file_version"]
        conn = psycopg2.connect(PG_CONN)
        try:
            with conn, conn.cursor() as cur:
                cur.execute("""
                    CREATE SCHEMA IF NOT EXISTS bronze;
                    CREATE TABLE IF NOT EXISTS bronze.raw_stops (
                        stop_id      TEXT PRIMARY KEY,
                        stop_name    TEXT NOT NULL,
                        stop_lat     DOUBLE PRECISION,
                        stop_lon     DOUBLE PRECISION,
                        stop_area    TEXT,
                        authority    TEXT,
                        file_version TEXT,
                        loaded_at    TIMESTAMPTZ DEFAULT now()
                    );
                """)
                cur.executemany("""
                    INSERT INTO bronze.raw_stops
                        (stop_id, stop_name, stop_lat, stop_lon, stop_area, authority, file_version)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (stop_id) DO UPDATE SET
                        stop_name    = EXCLUDED.stop_name,
                        stop_lat     = EXCLUDED.stop_lat,
                        stop_lon     = EXCLUDED.stop_lon,
                        stop_area    = EXCLUDED.stop_area,
                        authority    = EXCLUDED.authority,
                        file_version = EXCLUDED.file_version,
                        loaded_at    = now();
                """, [
                    (r["stop_id"], r["stop_name"], r["stop_lat"], r["stop_lon"],
                     r.get("stop_area"), r.get("authority"), version)
                    for r in rows
                ])
        finally:
            conn.close()
        print(f"Loaded {len(rows)} rows (version={version})")

    rows = extract()
    quality_check(rows)
    load(rows)


ingest_stops()
