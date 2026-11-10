#!/usr/bin/env python3

"""Continuously archive Ridango vehicle-status polling attempts in MongoDB."""

from __future__ import annotations

import json
import logging
import os
import time
import uuid
from datetime import UTC, datetime
from typing import Any

import websocket
from pymongo import MongoClient


URL = "wss://api.ridango.com/rt-ws/vehicle-status"
SUBSCRIPTION = {
    "regionId": 32,
    "topLeftCoordinates": {"longitude": 26.4820, "latitude": 58.4751},
    "bottomRightCoordinates": {"longitude": 26.9720, "latitude": 58.2871},
}

MONGO_URI = os.environ["MONGO_URI"]
MONGO_DATABASE = os.getenv("MONGO_DATABASE", "tartu_bus")
MONGO_COLLECTION = os.getenv("MONGO_COLLECTION", "ridango_poll_events")
POLL_INTERVAL_SECONDS = float(os.getenv("POLL_INTERVAL_SECONDS", "15"))
WEBSOCKET_TIMEOUT_SECONDS = float(os.getenv("WEBSOCKET_TIMEOUT_SECONDS", "30"))

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)


class RidangoApiError(Exception):
    def __init__(self, payload: dict[str, Any]) -> None:
        self.payload = payload
        super().__init__(str(payload.get("errorMessage", "Ridango API error")))


def utc_now() -> datetime:
    return datetime.now(UTC)


def poll_id(started_at: datetime, payload: Any) -> str:
    """Return a deterministic UUID derived from the poll timestamp and payload."""
    hash_input = json.dumps(
        {"started_at": started_at.isoformat(), "payload": payload},
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
    )
    return str(uuid.uuid5(uuid.NAMESPACE_URL, hash_input))


def fetch_response() -> list[dict[str, Any]]:
    connection = websocket.create_connection(
        URL,
        origin="https://tartu.pilet.ee",
        timeout=WEBSOCKET_TIMEOUT_SECONDS,
    )
    try:
        connection.send(json.dumps(SUBSCRIPTION))
        while True:
            message = json.loads(connection.recv())
            if isinstance(message, list):
                return message
            if isinstance(message, dict) and message.get("errorMessage"):
                raise RidangoApiError(message)
    finally:
        connection.close()


def successful_event(started_at: datetime, payload: list[dict[str, Any]]) -> dict[str, Any]:
    return {
        "poll_id": poll_id(started_at, payload),
        "started_at": started_at,
        "finished_at": utc_now(),
        "status": "success" if payload else "empty",
        "vehicle_count": len(payload),
        "subscription": SUBSCRIPTION,
        "payload": payload,
    }


def failed_event(started_at: datetime, error: Exception) -> dict[str, Any]:
    payload = error.payload if isinstance(error, RidangoApiError) else None
    return {
        "poll_id": poll_id(started_at, payload),
        "started_at": started_at,
        "finished_at": utc_now(),
        "status": "api_error" if payload is not None else "error",
        "error_type": type(error).__name__,
        "error_message": str(error),
        "subscription": SUBSCRIPTION,
        "payload": payload,
    }


def main() -> None:
    if POLL_INTERVAL_SECONDS <= 0:
        raise ValueError("POLL_INTERVAL_SECONDS must be greater than zero")

    client = MongoClient(MONGO_URI)
    collection = client[MONGO_DATABASE][MONGO_COLLECTION]
    collection.create_index("poll_id", unique=True)
    collection.create_index([("status", 1), ("started_at", -1)])

    logger.info("Starting Ridango poller; interval=%s seconds", POLL_INTERVAL_SECONDS)
    while True:
        started_at = utc_now()
        try:
            event = successful_event(started_at, fetch_response())
        except Exception as error:
            event = failed_event(started_at, error)

        try:
            collection.insert_one(event)
            logger.info("Stored poll event %s (%s)", event["poll_id"], event["status"])
        except Exception:
            logger.exception("Could not store poll event in MongoDB")

        time.sleep(POLL_INTERVAL_SECONDS)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        logger.info("Ridango poller stopped")
