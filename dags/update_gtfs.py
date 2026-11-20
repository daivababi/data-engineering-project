from airflow.decorators import dag, task
from datetime import datetime

from pathlib import Path
import zipfile
import requests

GTFS_URL = "https://eu-gtfs.remix.com/tartu.zip"

ZIP_PATH = Path("/tmp/tartu_gtfs.zip")
RAW_DIR = Path("/tmp/gtfs")


@dag(
    dag_id="update_gtfs",
    start_date=datetime(2026, 1, 1),
    schedule="@daily",
    catchup=False,
    tags=["gtfs"],
)
def update_gtfs():

    @task
    def check_source():

        response = requests.head(GTFS_URL, timeout=30)

        if response.status_code != 200:
            raise Exception("GTFS source unavailable")

        print(f"Source OK: {response.status_code}")

    @task
    def download_and_extract_gtfs():

        response = requests.get(GTFS_URL, timeout=120)
        response.raise_for_status()

        ZIP_PATH.write_bytes(response.content)

        print(f"Downloaded {len(response.content)} bytes")

        RAW_DIR.mkdir(parents=True, exist_ok=True)

        with zipfile.ZipFile(ZIP_PATH, "r") as archive:
            archive.extractall(RAW_DIR)

        print(f"GTFS extracted to {RAW_DIR}")

    check_source() >> download_and_extract_gtfs()


update_gtfs()