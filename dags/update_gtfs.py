from airflow.decorators import dag, task
from airflow.operators.bash import BashOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import datetime
from pathlib import Path
import requests
import zipfile

GTFS_URL = "https://eu-gtfs.remix.com/tartu.zip"
LAST_MODIFIED_FILE = Path("/tmp/gtfs_last_modified.txt")
ZIP_PATH = Path("/tmp/tartu_gtfs.zip")
EXTRACT_DIR = Path("/tmp/gtfs")

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
    def check_if_changed():
        response = requests.head(GTFS_URL, timeout=30)
        new_last_modified = response.headers.get("Last-Modified")
        print(f"Current Last-Modified: {new_last_modified}")

        if not LAST_MODIFIED_FILE.exists():
            print("No previous value found — treating as changed")
            return True

        old_last_modified = LAST_MODIFIED_FILE.read_text().strip()
        print(f"Previous Last-Modified: {old_last_modified}")

        if old_last_modified == new_last_modified:
            print("GTFS has not changed")
            return False

        print("GTFS has changed")
        return True

    @task
    def download_and_extract_gtfs(has_changed):
        if not has_changed:
            print("Skipping download")
            return

        response = requests.get(GTFS_URL, timeout=120)
        response.raise_for_status()
        ZIP_PATH.write_bytes(response.content)
        print(f"Downloaded {len(response.content)} bytes")

        EXTRACT_DIR.mkdir(parents=True, exist_ok=True)
        with zipfile.ZipFile(ZIP_PATH, "r") as archive:
            archive.extractall(EXTRACT_DIR)
            print("ZIP contents:", archive.namelist())
        head = requests.head(GTFS_URL, timeout=30)
        LAST_MODIFIED_FILE.write_text(head.headers.get("Last-Modified"))
        print("GTFS downloaded, extracted, and Last-Modified updated")

    check = check_source()
    changed = check_if_changed()
    download = download_and_extract_gtfs(changed)

    trigger_gtfs = TriggerDagRunOperator(
        task_id="trigger_ingest_gtfs",
        trigger_dag_id="ingest_gtfs",
    )
    trigger_stops = TriggerDagRunOperator(
        task_id="trigger_ingest_stops",
        trigger_dag_id="ingest_stops",
    )
    dbt_build = BashOperator(
        task_id="dbt_build",
        bash_command="cd /dbt && dbt run --select stop_arrival_stats",
    )

    check >> changed >> download >> trigger_gtfs >> trigger_stops >> dbt_build

update_gtfs()