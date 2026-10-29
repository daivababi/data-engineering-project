#!/usr/bin/env python3

import os
import csv
import json
from pathlib import Path
from datetime import datetime

import websocket


URL = "wss://api.ridango.com/rt-ws/vehicle-status"
OUTDIR = Path('data/ridango_responses')
os.makedirs(OUTDIR, exist_ok=True)

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


timestamp = f"{datetime.now():%Y%m%d-%H%M%S.%f}"[:-3]
connection = None
try:
    connection = websocket.create_connection(
        URL,
        origin="https://tartu.pilet.ee",
        timeout=30,
    )
    connection.send(json.dumps(SUBSCRIPTION))

    while True:
        message = json.loads(connection.recv())
        if isinstance(message, list):
            # Uncomment this line to check error-handling
            #raise RuntimeError(message["errorMessage"])
        
            buses = message
            break
        if isinstance(message, dict) and message.get("errorMessage"):
            raise RuntimeError(message["errorMessage"])

    rows = [flatten(bus) for bus in buses]
    fieldnames = list(dict.fromkeys(key for row in rows for key in row))
    outpath = OUTDIR / f"{timestamp}.csv"

    with outpath.open("w", newline="", encoding="utf-8") as csv_file:
        writer = csv.DictWriter(csv_file, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)

    print(f"Saved {len(rows)} buses to {outpath}")
except Exception as error:
    error_path = OUTDIR / f"{timestamp}.error.txt"
    error_path.write_text(
        f"{type(error).__name__}: {error}\n",
        encoding="utf-8",
    )
    print(f"Saved error to {error_path}")
finally:
    if connection is not None:
        connection.close()
