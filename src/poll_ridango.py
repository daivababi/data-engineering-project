#!/usr/bin/env python3

import os
import csv
import json
import time
from pathlib import Path
from datetime import datetime, timezone

import websocket


URL = "wss://api.ridango.com/rt-ws/vehicle-status"
OUTDIR = Path(os.getenv("RIDANGO_OUTPUT_DIR", "data/ridango_responses"))
POLL_INTERVAL_SECONDS = float(os.getenv("POLL_INTERVAL_SECONDS", "15"))

SUBSCRIPTION = {
    "regionId": 32,
    "topLeftCoordinates": {"longitude": 26.4820, "latitude": 58.4751},  # TODO: Codex-recommended limits; need validation
    "bottomRightCoordinates": {"longitude": 26.9720, "latitude": 58.2871},  # TODO: Codex-recommended limits; need validation
}


def flatten(data, prefix=""):
    row = {}
    for key, value in data.items():
        column = f"{prefix}_{key}" if prefix else key
        if isinstance(value, dict):
            row.update(flatten(value, column))
        else:
            row[column] = value
    return row


def timestamp():
    """Return a UTC timestamp suitable for a unique, sortable filename."""
    return f"{datetime.now(timezone.utc):%Y%m%d-%H%M%S.%f}"[:-3]


def fetch_buses():
    connection = websocket.create_connection(
        URL,
        origin="https://tartu.pilet.ee",
        timeout=30,
    )
    try:
        connection.send(json.dumps(SUBSCRIPTION))

        while True:
            message = json.loads(connection.recv())
            if isinstance(message, list):
                return message
            if isinstance(message, dict) and message.get("errorMessage"):
                raise RuntimeError(message["errorMessage"])
    finally:
        connection.close()


def save_buses(buses, collected_at):
    rows = [flatten(bus) for bus in buses]
    fieldnames = list(dict.fromkeys(key for row in rows for key in row))
    outpath = OUTDIR / f"{collected_at}.csv"

    with outpath.open("w", newline="", encoding="utf-8") as csv_file:
        writer = csv.DictWriter(csv_file, fieldnames=fieldnames)
        if fieldnames:
            writer.writeheader()
        writer.writerows(rows)

    print(f"Saved {len(rows)} buses to {outpath}", flush=True)


def save_error(error, collected_at):
    error_path = OUTDIR / f"{collected_at}.error.txt"
    error_path.write_text(
        f"{type(error).__name__}: {error}\n",
        encoding="utf-8",
    )
    print(f"Saved error to {error_path}", flush=True)


def main():
    if POLL_INTERVAL_SECONDS <= 0:
        raise ValueError("POLL_INTERVAL_SECONDS must be greater than zero")

    OUTDIR.mkdir(parents=True, exist_ok=True)
    print(
        f"Polling {URL} every {POLL_INTERVAL_SECONDS:g} seconds; "
        f"saving responses in {OUTDIR}",
        flush=True,
    )

    while True:
        cycle_started = time.monotonic()
        collected_at = timestamp()

        try:
            save_buses(fetch_buses(), collected_at)
        except Exception as error:
            save_error(error, collected_at)

        elapsed = time.monotonic() - cycle_started
        time.sleep(max(0, POLL_INTERVAL_SECONDS - elapsed))


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("Polling stopped", flush=True)
