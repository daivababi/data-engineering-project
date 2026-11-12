#!/usr/bin/env python3

"""Continuously copy validated Ridango vehicle observations from MongoDB to PostgreSQL."""

from __future__ import annotations

import logging
import math
import os
import time
from datetime import date, datetime
from pathlib import Path
from typing import Any

import psycopg2
from pymongo import MongoClient


MONGO_URI = os.environ["MONGO_URI"]
MONGO_DATABASE = os.getenv("MONGO_DATABASE", "tartu_bus")
MONGO_COLLECTION = os.getenv("MONGO_COLLECTION", "ridango_poll_events")
POSTGRES_HOST = os.getenv("POSTGRES_HOST", "postgres")
POSTGRES_PORT = int(os.getenv("POSTGRES_PORT", "5432"))
POSTGRES_DB = os.environ["POSTGRES_DB"]
POSTGRES_USER = os.environ["POSTGRES_USER"]
POSTGRES_PASSWORD = os.environ["POSTGRES_PASSWORD"]
CLEANER_INTERVAL_SECONDS = float(os.getenv("CLEANER_INTERVAL_SECONDS", "10"))
DDL_PATH = Path("/docker-entrypoint-initdb.d/04_create_vehicle_observation.sql")

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


def ensure_observation_table(connection) -> None:
    with DDL_PATH.open(encoding="utf-8") as ddl_file, connection.cursor() as cursor:
        cursor.execute(ddl_file.read())
    connection.commit()


def latest_cleaned_poll_id(connection) -> str | None:
    with connection.cursor() as cursor:
        cursor.execute(
            """
            SELECT source_poll_id::text
            FROM silver.vehicle_observation
            ORDER BY cleaned_at DESC
            LIMIT 1
            """
        )
        row = cursor.fetchone()
    return row[0] if row else None


def parse_text(value: Any, field: str, errors: list[str]) -> str | None:
    if not isinstance(value, str) or not value.strip():
        errors.append(f"{field} missing or invalid")
        return None
    return value.strip()


def parse_timestamp(value: Any, errors: list[str]) -> datetime | None:
    if not isinstance(value, str):
        errors.append("update_time missing or invalid")
        return None
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        errors.append("update_time invalid")
        return None
    if parsed.tzinfo is None:
        errors.append("update_time has no timezone")
        return None
    return parsed


def parse_service_date(value: Any, errors: list[str]) -> date | None:
    if not isinstance(value, str):
        errors.append("service_date missing or invalid")
        return None
    try:
        return datetime.strptime(value, "%Y%m%d").date()
    except ValueError:
        errors.append("service_date invalid")
        return None


def parse_coordinate(
    value: Any, field: str, lower: float, upper: float, errors: list[str]
) -> float | None:
    if isinstance(value, bool):
        errors.append(f"{field} invalid")
        return None
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        errors.append(f"{field} missing or invalid")
        return None
    if not math.isfinite(parsed) or not lower <= parsed <= upper:
        errors.append(f"{field} out of range")
        return None
    return parsed


def extract_gtfs_trip_id(ridango_trip_id: str | None, errors: list[str]) -> str | None:
    if ridango_trip_id is None:
        return None
    if "|" not in ridango_trip_id:
        errors.append("gtfs_trip_id not present in ridango_trip_id")
        return None
    gtfs_trip_id = ridango_trip_id.rsplit("|", 1)[1].strip()
    if not gtfs_trip_id:
        errors.append("gtfs_trip_id missing")
        return None
    return gtfs_trip_id


def trip_match_count(cursor, gtfs_trip_id: str | None) -> int:
    if gtfs_trip_id is None:
        return 0
    cursor.execute(
        "SELECT COUNT(*) FROM gold.dim_trip WHERE trip_id = %s", (gtfs_trip_id,)
    )
    return cursor.fetchone()[0]


def observation_values(cursor, item: Any) -> tuple[Any, ...]:
    errors: list[str] = []
    if not isinstance(item, dict):
        return (None, None, None, None, None, None, None, None, False, "payload item invalid")

    device_id = parse_text(item.get("deviceId"), "device_id", errors)
    trip = item.get("trip") if isinstance(item.get("trip"), dict) else {}
    if not trip:
        errors.append("trip missing or invalid")
    ridango_trip_id = parse_text(trip.get("tripId"), "ridango_trip_id", errors)
    gtfs_trip_id = extract_gtfs_trip_id(ridango_trip_id, errors)
    route_short_name = parse_text(trip.get("routeShortName"), "route_short_name", errors)
    observed_at = parse_timestamp(item.get("updateTime"), errors)
    service_date = parse_service_date(trip.get("startDate"), errors)

    position = item.get("position") if isinstance(item.get("position"), dict) else {}
    if not position:
        errors.append("position missing or invalid")
    latitude = parse_coordinate(position.get("latitude"), "latitude", -90, 90, errors)
    longitude = parse_coordinate(position.get("longitude"), "longitude", -180, 180, errors)

    matches = trip_match_count(cursor, gtfs_trip_id)
    if gtfs_trip_id is not None and matches != 1:
        errors.append(f"gtfs_trip_id matched {matches} trips; expected 1")

    is_valid = not errors
    return (
        device_id,
        observed_at,
        service_date,
        ridango_trip_id,
        gtfs_trip_id,
        route_short_name,
        latitude,
        longitude,
        is_valid,
        None if is_valid else "; ".join(errors),
    )


def clean_poll(connection, raw_poll: dict[str, Any]) -> int:
    poll_id = raw_poll["poll_id"]
    payload = raw_poll.get("payload")
    if not isinstance(payload, list):
        raise ValueError(f"Poll {poll_id} has a non-list payload")

    with connection:
        with connection.cursor() as cursor:
            for item_index, item in enumerate(payload):
                values = observation_values(cursor, item)
                cursor.execute(
                    """
                    INSERT INTO silver.vehicle_observation (
                        source_poll_id, source_item_index, device_id, observed_at,
                        service_date, ridango_trip_id, gtfs_trip_id, route_short_name,
                        latitude, longitude, is_valid, validation_error
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """,
                    (poll_id, item_index, *values),
                )
    return len(payload)


def new_successful_polls(collection, last_poll_id: str | None):
    query: dict[str, Any] = {"status": "success"}
    if last_poll_id is not None:
        last_poll = collection.find_one({"poll_id": last_poll_id}, {"started_at": 1})
        if last_poll is None:
            raise RuntimeError(f"Last cleaned poll {last_poll_id} is missing from MongoDB")
        query["started_at"] = {"$gt": last_poll["started_at"]}
    return collection.find(query).sort("started_at", 1)


def run_once(connection, collection) -> int:
    last_poll_id = latest_cleaned_poll_id(connection)
    processed_count = 0
    for raw_poll in new_successful_polls(collection, last_poll_id):
        item_count = clean_poll(connection, raw_poll)
        processed_count += 1
        logger.info("Cleaned poll %s (%s observations)", raw_poll["poll_id"], item_count)
    return processed_count


def main() -> None:
    if CLEANER_INTERVAL_SECONDS <= 0:
        raise ValueError("CLEANER_INTERVAL_SECONDS must be greater than zero")

    mongo_client = MongoClient(MONGO_URI)
    collection = mongo_client[MONGO_DATABASE][MONGO_COLLECTION]
    connection = postgres_connection()
    try:
        ensure_observation_table(connection)
        logger.info("Starting Ridango cleaner; interval=%s seconds", CLEANER_INTERVAL_SECONDS)
        while True:
            try:
                processed_count = run_once(connection, collection)
                if processed_count:
                    logger.info("Processed %s raw poll(s)", processed_count)
            except Exception:
                logger.exception("Could not clean new Ridango poll events")
                connection.rollback()
            time.sleep(CLEANER_INTERVAL_SECONDS)
    finally:
        connection.close()


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        logger.info("Ridango cleaner stopped")
