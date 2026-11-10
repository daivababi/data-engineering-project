from __future__ import annotations

import logging
import os
from collections.abc import Iterator
from datetime import datetime, timezone
from typing import Any

from airflow.decorators import dag, task
from airflow.hooks.base import BaseHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from bson import json_util
from bson.objectid import ObjectId
from psycopg2.extras import execute_values
from pymongo import ASCENDING, MongoClient

LOGGER = logging.getLogger(__name__)
BATCH_SIZE = 500

CREATE_RAW_TABLE_SQL = """
    CREATE SCHEMA IF NOT EXISTS bronze;
    CREATE TABLE IF NOT EXISTS bronze.raw_ridango_poll_events (
        mongo_id TEXT PRIMARY KEY,
        poll_id TEXT NOT NULL UNIQUE,
        started_at TIMESTAMPTZ,
        finished_at TIMESTAMPTZ,
        status TEXT NOT NULL,
        vehicle_count INTEGER,
        raw_document JSONB NOT NULL,
        ingested_at TIMESTAMPTZ NOT NULL DEFAULT now()
    )
"""

INSERT_RAW_ROWS_SQL = """
    INSERT INTO bronze.raw_ridango_poll_events (
        mongo_id,
        poll_id,
        started_at,
        finished_at,
        status,
        vehicle_count,
        raw_document
    )
    VALUES %s
    ON CONFLICT DO NOTHING
"""

CREATE_GPS_ARRIVAL_ESTIMATES_SQL = """
    CREATE TABLE IF NOT EXISTS bronze.gps_stop_arrival_estimates (
        service_date DATE NOT NULL,
        trip_id TEXT NOT NULL,
        stop_id TEXT NOT NULL,
        stop_sequence INTEGER NOT NULL,
        estimated_arrival TIMESTAMPTZ NOT NULL,
        vehicle_id TEXT NOT NULL,
        distance_m DOUBLE PRECISION NOT NULL,
        recorded_at TIMESTAMPTZ NOT NULL DEFAULT now(),
        PRIMARY KEY (service_date, trip_id, stop_sequence)
    )
"""

CREATE_SCHEDULE_LOOKUP_INDEX_SQL = """
    CREATE INDEX IF NOT EXISTS idx_gold_fact_arrival_trip_schedule
        ON gold.fact_stop_arrival (service_date, trip_id, stop_sequence)
"""

INSERT_GPS_ARRIVAL_ESTIMATES_SQL = """
    WITH positions AS (
        SELECT
            e.poll_id,
            e.started_at AS observed_at,
            p.item->>'deviceId' AS vehicle_id,
            split_part(p.item->'trip'->>'tripId', '|', 2) AS trip_id,
            CASE
                WHEN p.item->'trip'->>'startDate' ~ '^[0-9]{8}$'
                THEN to_date(p.item->'trip'->>'startDate', 'YYYYMMDD')
            END AS service_date,
            (p.item->'position'->>'latitude')::double precision AS latitude,
            (p.item->'position'->>'longitude')::double precision AS longitude
        FROM bronze.raw_ridango_poll_events AS e
        CROSS JOIN LATERAL jsonb_array_elements(
            CASE
                WHEN jsonb_typeof(e.raw_document->'payload') = 'array'
                THEN e.raw_document->'payload'
                ELSE '[]'::jsonb
            END
        ) AS p(item)
        WHERE e.status = 'success'
          AND e.started_at >= now() - make_interval(mins => %(lookback_minutes)s)
          AND e.started_at IS NOT NULL
          AND p.item->>'deviceId' IS NOT NULL
          AND p.item->'trip'->>'tripId' LIKE '%%|%%'
          AND p.item->'trip'->>'startDate' ~ '^[0-9]{8}$'
          AND p.item->'position'->>'latitude' IS NOT NULL
          AND p.item->'position'->>'longitude' IS NOT NULL
    ),
    stop_distances AS (
        SELECT
            positions.*,
            schedule.stop_id,
            schedule.stop_sequence,
            2 * 6371000 * ASIN(SQRT(LEAST(
                1.0,
                POWER(SIN(RADIANS(stop.latitude - positions.latitude) / 2), 2)
                + COS(RADIANS(positions.latitude))
                * COS(RADIANS(stop.latitude))
                * POWER(SIN(RADIANS(stop.longitude - positions.longitude) / 2), 2)
            ))) AS distance_m
        FROM positions
        JOIN gold.fact_stop_arrival AS schedule
          ON schedule.trip_id = positions.trip_id
         AND schedule.service_date = positions.service_date
        JOIN gold.dim_stop AS stop
          ON stop.stop_id = schedule.stop_id
         AND stop.is_current
    ),
    neighboring_positions AS (
        SELECT
            stop_distances.*,
            LAG(distance_m) OVER position_order AS previous_distance_m,
            LAG(observed_at) OVER position_order AS previous_observed_at,
            LEAD(distance_m) OVER position_order AS next_distance_m,
            LEAD(observed_at) OVER position_order AS next_observed_at
        FROM stop_distances
        WINDOW position_order AS (
            PARTITION BY vehicle_id, trip_id, service_date, stop_id, stop_sequence
            ORDER BY observed_at, poll_id
        )
    ),
    confirmed_entries AS (
        SELECT *
        FROM neighboring_positions
        WHERE previous_distance_m > %(radius_m)s
          AND distance_m <= %(radius_m)s
          AND next_distance_m <= %(radius_m)s
          AND observed_at - previous_observed_at
              BETWEEN make_interval(secs => %(min_gap_seconds)s)
                  AND make_interval(secs => %(max_gap_seconds)s)
          AND next_observed_at - observed_at
              BETWEEN make_interval(secs => %(min_gap_seconds)s)
                  AND make_interval(secs => %(max_gap_seconds)s)
    )
    INSERT INTO bronze.gps_stop_arrival_estimates (
        service_date,
        trip_id,
        stop_id,
        stop_sequence,
        estimated_arrival,
        vehicle_id,
        distance_m
    )
    SELECT DISTINCT ON (service_date, trip_id, stop_sequence)
        service_date,
        trip_id,
        stop_id,
        stop_sequence,
        observed_at,
        vehicle_id,
        distance_m
    FROM confirmed_entries
    ORDER BY service_date, trip_id, stop_sequence, observed_at
    ON CONFLICT (service_date, trip_id, stop_sequence) DO NOTHING
"""


def mongo_documents_after(
    collection: Any, watermark: str | None
) -> Iterator[dict[str, Any]]:
    query: dict[str, Any] = {}
    if watermark:
        if not ObjectId.is_valid(watermark):
            raise ValueError(f"Invalid Mongo ObjectId watermark: {watermark}")
        query["_id"] = {"$gt": ObjectId(watermark)}
    return iter(collection.find(query).sort("_id", ASCENDING).batch_size(BATCH_SIZE))


def document_to_row(document: dict[str, Any]) -> tuple[Any, ...]:
    mongo_id = document.get("_id")
    poll_id = document.get("poll_id")
    status = document.get("status")
    if not isinstance(mongo_id, ObjectId):
        raise ValueError("Ridango poll document _id must be a Mongo ObjectId")
    if not poll_id:
        raise ValueError(f"Ridango poll document {mongo_id} has no poll_id")
    if not status:
        raise ValueError(f"Ridango poll document {mongo_id} has no status")

    return (
        str(mongo_id),
        poll_id,
        document.get("started_at"),
        document.get("finished_at"),
        status,
        document.get("vehicle_count"),
        json_util.dumps(document),
    )


def insert_batch(connection: Any, rows: list[tuple[Any, ...]]) -> int:
    with connection.cursor() as cursor:
        execute_values(
            cursor,
            INSERT_RAW_ROWS_SQL,
            rows,
            template="(%s, %s, %s, %s, %s, %s, %s::jsonb)",
            page_size=BATCH_SIZE,
        )
        inserted_count = cursor.rowcount
    connection.commit()
    return inserted_count


def record_gps_stop_arrival_estimates(connection: Any) -> int:
    with connection.cursor() as cursor:
        cursor.execute(CREATE_GPS_ARRIVAL_ESTIMATES_SQL)
        cursor.execute(CREATE_SCHEDULE_LOOKUP_INDEX_SQL)
        cursor.execute(
            INSERT_GPS_ARRIVAL_ESTIMATES_SQL,
            {
                "lookback_minutes": 5,
                "radius_m": 20,
                "min_gap_seconds": 5,
                "max_gap_seconds": 60,
            },
        )
        recorded_count = cursor.rowcount
    connection.commit()
    return recorded_count


@dag(
    dag_id="ingest_ridango_mongo",
    start_date=datetime(2026, 1, 1, tzinfo=timezone.utc),
    schedule="*/1 * * * *",
    catchup=False,
    max_active_runs=1,
    tags=["bronze", "ridango", "mongo"],
)
def ingest_ridango_mongo():
    @task
    def copy_new_poll_events() -> None:
        mongo_connection = BaseHook.get_connection("ridango_mongo")
        mongo_client = MongoClient(
            host=mongo_connection.host,
            port=mongo_connection.port or 27017,
            username=mongo_connection.login,
            password=mongo_connection.password,
            authSource=mongo_connection.extra_dejson.get("authSource", "admin"),
            tz_aware=True,
            serverSelectionTimeoutMS=10000,
        )
        postgres_connection = None
        mongo_cursor = None

        try:
            mongo_client.admin.command("ping")
            postgres_connection = PostgresHook(
                postgres_conn_id="warehouse_postgres"
            ).get_conn()
            mongo_database = os.environ.get("MONGO_DATABASE", "tartu_bus")
            mongo_collection = os.environ.get(
                "MONGO_COLLECTION", "ridango_poll_events"
            )
            collection = mongo_client[mongo_database][mongo_collection]

            with postgres_connection.cursor() as cursor:
                cursor.execute(CREATE_RAW_TABLE_SQL)
                cursor.execute(
                    """
                    SELECT mongo_id
                    FROM bronze.raw_ridango_poll_events
                    ORDER BY mongo_id DESC
                    LIMIT 1
                    """
                )
                watermark_row = cursor.fetchone()
            postgres_connection.commit()

            watermark = watermark_row[0] if watermark_row else None
            mongo_cursor = mongo_documents_after(collection, watermark)
            inserted_count = 0
            batch: list[tuple[Any, ...]] = []

            for document in mongo_cursor:
                batch.append(document_to_row(document))
                if len(batch) >= BATCH_SIZE:
                    inserted_count += insert_batch(postgres_connection, batch)
                    batch.clear()

            if batch:
                inserted_count += insert_batch(postgres_connection, batch)

            LOGGER.info(
                "Copied %s new poll events from MongoDB %s.%s",
                inserted_count,
                mongo_database,
                mongo_collection,
            )
            arrival_estimate_count = record_gps_stop_arrival_estimates(
                postgres_connection
            )
            LOGGER.info(
                "Recorded %s GPS-based stop arrival estimates",
                arrival_estimate_count,
            )
        finally:
            if mongo_cursor is not None:
                mongo_cursor.close()
            if postgres_connection is not None:
                postgres_connection.close()
            mongo_client.close()

    copy_new_poll_events()


ingest_ridango_mongo()