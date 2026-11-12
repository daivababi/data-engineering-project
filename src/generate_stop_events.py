#!/usr/bin/env python3

"""Derive confirmed stop-arrival facts from cleaned Ridango vehicle observations."""

from __future__ import annotations

import logging
import os
import time
from pathlib import Path

import psycopg2


POSTGRES_HOST = os.getenv("POSTGRES_HOST", "postgres")
POSTGRES_PORT = int(os.getenv("POSTGRES_PORT", "5432"))
POSTGRES_DB = os.environ["POSTGRES_DB"]
POSTGRES_USER = os.environ["POSTGRES_USER"]
POSTGRES_PASSWORD = os.environ["POSTGRES_PASSWORD"]
FACT_GENERATOR_INTERVAL_SECONDS = float(
    os.getenv("FACT_GENERATOR_INTERVAL_SECONDS", "10")
)
FACT_GENERATOR_BATCH_SIZE = int(os.getenv("FACT_GENERATOR_BATCH_SIZE", "500"))
STOP_RADIUS_METERS = float(os.getenv("STOP_RADIUS_METERS", "25"))
DDL_PATH = Path("/docker-entrypoint-initdb.d/05_create_observed_stop_arrivals.sql")

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)


def postgres_connection():
    return psycopg2.connect(
        host=POSTGRES_HOST,
        port=POSTGRES_PORT,
        dbname=POSTGRES_DB,
        user=POSTGRES_USER,
        password=POSTGRES_PASSWORD,
        options="-c timezone=Europe/Tallinn",
    )


def ensure_fact_tables(connection) -> None:
    with DDL_PATH.open(encoding="utf-8") as ddl_file, connection.cursor() as cursor:
        cursor.execute(ddl_file.read())
    connection.commit()


def unprocessed_observations(connection):
    with connection.cursor() as cursor:
        cursor.execute(
            """
            SELECT
                observation.source_poll_id,
                observation.source_item_index,
                observation.device_id,
                observation.gtfs_trip_id,
                observation.service_date,
                observation.observed_at,
                observation.latitude,
                observation.longitude,
                observation.is_valid
            FROM silver.vehicle_observation AS observation
            LEFT JOIN gold.stop_event_processing AS processed
                ON processed.source_poll_id = observation.source_poll_id
                AND processed.source_item_index = observation.source_item_index
            WHERE processed.source_poll_id IS NULL
            ORDER BY observation.observed_at NULLS LAST,
                observation.source_poll_id,
                observation.source_item_index
            LIMIT %s
            """,
            (FACT_GENERATOR_BATCH_SIZE,),
        )
        return cursor.fetchall()


def service_is_active(cursor, gtfs_trip_id, service_date) -> bool:
    cursor.execute(
        """
        SELECT EXISTS (
            SELECT 1
            FROM silver.silver_trips AS trip
            JOIN silver.silver_service_dates AS service
                ON service.service_id = trip.service_id
            WHERE trip.trip_id = %s
              AND service.service_date = %s
        )
        """,
        (gtfs_trip_id, service_date),
    )
    return cursor.fetchone()[0]


def closest_future_stop(cursor, observation):
    (
        _poll_id,
        _item_index,
        _device_id,
        gtfs_trip_id,
        service_date,
        observed_at,
        latitude,
        longitude,
        _is_valid,
    ) = observation

    cursor.execute(
        """
        WITH last_arrival AS (
            SELECT COALESCE(MAX(stop_sequence), -1) AS stop_sequence
            FROM gold.fact_observed_stop_arrival
            WHERE gtfs_trip_id = %s
              AND service_date = %s
        ), candidates AS (
            SELECT
                stop_time.stop_id,
                stop_time.stop_sequence,
                route.route_short_name AS line_no,
                stop.stop_name,
                (%s::date + stop_time.arrival_offset_seconds * interval '1 second')
                    AT TIME ZONE 'Europe/Tallinn' AS scheduled_arrival,
                6371000 * acos(LEAST(1.0, GREATEST(-1.0,
                    sin(radians(%s)) * sin(radians(stop.stop_lat))
                    + cos(radians(%s)) * cos(radians(stop.stop_lat))
                    * cos(radians(stop.stop_lon) - radians(%s))
                ))) AS distance_meters
            FROM silver.silver_stop_times AS stop_time
            JOIN silver.silver_stops AS stop
                ON stop.stop_id = stop_time.stop_id
            JOIN silver.silver_trips AS trip
                ON trip.trip_id = stop_time.trip_id
            LEFT JOIN silver.silver_routes AS route
                ON route.route_id = trip.route_id
            CROSS JOIN last_arrival
            WHERE stop_time.trip_id = %s
              AND stop_time.stop_sequence > last_arrival.stop_sequence
        )
        SELECT stop_id, stop_sequence, line_no, stop_name, scheduled_arrival, distance_meters
        FROM candidates
        WHERE distance_meters <= %s
        ORDER BY distance_meters, stop_sequence
        LIMIT 1
        """,
        (
            gtfs_trip_id,
            service_date,
            service_date,
            latitude,
            latitude,
            longitude,
            gtfs_trip_id,
            STOP_RADIUS_METERS,
        ),
    )
    return cursor.fetchone()


def mark_processed(cursor, poll_id, item_index, outcome, arrival_id=None) -> None:
    cursor.execute(
        """
        INSERT INTO gold.stop_event_processing (
            source_poll_id, source_item_index, outcome, arrival_id
        )
        VALUES (%s, %s, %s, %s)
        """,
        (poll_id, item_index, outcome, arrival_id),
    )


def process_observation(connection, observation) -> str:
    (
        poll_id,
        item_index,
        device_id,
        gtfs_trip_id,
        service_date,
        observed_at,
        _latitude,
        _longitude,
        is_valid,
    ) = observation

    with connection:
        with connection.cursor() as cursor:
            if not is_valid:
                mark_processed(cursor, poll_id, item_index, "invalid_observation")
                return "invalid_observation"

            if not service_is_active(cursor, gtfs_trip_id, service_date):
                mark_processed(cursor, poll_id, item_index, "inactive_service")
                return "inactive_service"

            candidate = closest_future_stop(cursor, observation)
            if candidate is None:
                mark_processed(cursor, poll_id, item_index, "no_stop_within_radius")
                return "no_stop_within_radius"

            (
                stop_id,
                stop_sequence,
                line_no,
                stop_name,
                scheduled_arrival,
                distance_meters,
            ) = candidate
            cursor.execute(
                """
                INSERT INTO gold.fact_observed_stop_arrival (
                    source_poll_id, source_item_index, device_id, gtfs_trip_id, line_no,
                    service_date, stop_id, stop_name, stop_sequence, scheduled_arrival,
                    actual_arrival, delay_minutes, distance_meters
                )
                VALUES (
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    GREATEST(
                        0,
                        FLOOR(EXTRACT(EPOCH FROM (%s - %s)) / 60)::integer
                    ),
                    %s
                )
                ON CONFLICT (gtfs_trip_id, service_date, stop_sequence) DO NOTHING
                RETURNING arrival_id
                """,
                (
                    poll_id,
                    item_index,
                    device_id,
                    gtfs_trip_id,
                    line_no,
                    service_date,
                    stop_id,
                    stop_name,
                    stop_sequence,
                    scheduled_arrival,
                    observed_at,
                    observed_at,
                    scheduled_arrival,
                    distance_meters,
                ),
            )
            row = cursor.fetchone()
            if row is None:
                mark_processed(cursor, poll_id, item_index, "arrival_already_recorded")
                return "arrival_already_recorded"

            mark_processed(cursor, poll_id, item_index, "arrival_recorded", row[0])
            return "arrival_recorded"


def run_once(connection) -> int:
    processed_count = 0
    for observation in unprocessed_observations(connection):
        outcome = process_observation(connection, observation)
        processed_count += 1
        logger.info(
            "Processed observation %s/%s (%s)",
            observation[0],
            observation[1],
            outcome,
        )
    return processed_count


def main() -> None:
    if FACT_GENERATOR_INTERVAL_SECONDS <= 0:
        raise ValueError("FACT_GENERATOR_INTERVAL_SECONDS must be greater than zero")
    if FACT_GENERATOR_BATCH_SIZE <= 0:
        raise ValueError("FACT_GENERATOR_BATCH_SIZE must be greater than zero")
    if STOP_RADIUS_METERS <= 0:
        raise ValueError("STOP_RADIUS_METERS must be greater than zero")

    connection = postgres_connection()
    try:
        ensure_fact_tables(connection)
        logger.info(
            "Starting stop-event generator; interval=%s seconds, radius=%s metres",
            FACT_GENERATOR_INTERVAL_SECONDS,
            STOP_RADIUS_METERS,
        )
        while True:
            try:
                processed_count = run_once(connection)
                if processed_count:
                    logger.info("Processed %s observations", processed_count)
            except Exception:
                logger.exception("Could not generate stop-arrival facts")
                connection.rollback()
            time.sleep(FACT_GENERATOR_INTERVAL_SECONDS)
    finally:
        connection.close()


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        logger.info("Stop-event generator stopped")
