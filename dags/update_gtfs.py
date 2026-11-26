from airflow.decorators import dag, task
from airflow.operators.bash import BashOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import datetime
from airflow.exceptions import AirflowSkipException
from pathlib import Path
import requests
import zipfile
import json
import hashlib
import shutil

GTFS_VERSIONS_DIR = Path("/opt/airflow/data/gtfs_versions")
RAW_FALLBACK_DIR = Path("/opt/airflow/data/raw")

GTFS_URL = "https://eu-gtfs.remix.com/tartu.zip"
LAST_MODIFIED_FILE = Path("/opt/airflow/data/gtfs_versions/last_modified.txt")
CURRENT_VERSION_FILE = GTFS_VERSIONS_DIR / "current_gtfs.json"
ZIP_PATH = Path("/tmp/tartu_gtfs.zip")



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
        try:
            response = requests.head(GTFS_URL, timeout=30)
            response.raise_for_status()
            print(f"Source OK: {response.status_code}")
            return True

        except requests.RequestException:
            print("GTFS source unavailable")
            return False

    @task
    def check_if_changed(source_available):
        GTFS_VERSIONS_DIR.mkdir(parents=True, exist_ok=True)
        if not source_available:
            if CURRENT_VERSION_FILE.exists():
                print("GTFS source unavailable, using last known version") 
                return {"action": "skip"}
            print("GTFS source unavailable and no previous version found, using fallback raw data")
            return {"action": "fallback"}

        response = requests.head(GTFS_URL, timeout=30)
        response.raise_for_status()
        new_last_modified = response.headers.get("Last-Modified")
        print(f"Current Last-Modified: {new_last_modified}")

        if LAST_MODIFIED_FILE.exists():

            old_last_modified = LAST_MODIFIED_FILE.read_text().strip()
            print(f"Previous Last-Modified: {old_last_modified}")

            if old_last_modified == new_last_modified and new_last_modified is not None:
                print("GTFS has not changed")
                return {"action": "skip"}

        print("GTFS has changed or this is the first download")

        return {"action": "download", "last_modified": new_last_modified}

    @task
    def download_and_extract_gtfs(result):

        if result["action"] == "skip":

            print("Skipping download")

            raise AirflowSkipException(
                "GTFS has not changed"
            )

        version = datetime.now().strftime("%Y%m%d_%H%M")
        version_dir = GTFS_VERSIONS_DIR / version

        version_dir.mkdir(parents=True, exist_ok=True)

        if result["action"] == "fallback":
            files = list(RAW_FALLBACK_DIR.glob("*.txt"))
            if not files:
                raise Exception("No fallback GTFS files found")
            for file in files:
                shutil.copy(file, version_dir / file.name)
            source_type = "fallback"
            last_modified = None
        else:
            response = requests.get(GTFS_URL, timeout=120)
            response.raise_for_status()

            ZIP_PATH.write_bytes(response.content)

            print(f"Downloaded {len(response.content)} bytes")

            with zipfile.ZipFile(ZIP_PATH, "r") as archive:
                 archive.extractall(version_dir)
            if not list(version_dir.glob("*.txt")):
                raise Exception("No GTFS .txt files found in the downloaded zip")
            
            source_type = "download"
            last_modified = result.get("last_modified")

        metadata = {
            "version": version,
            "downloaded_at": datetime.now().isoformat(),
            "source_url": GTFS_URL,
            "last_modified": last_modified,
            "source_type": source_type
        }

        (version_dir / "metadata.json").write_text(
            json.dumps(metadata, indent=2)
        )
        if last_modified is not None:
            LAST_MODIFIED_FILE.write_text(last_modified)
        version_info = {
            "version": version,
            "path": str(version_dir)
        }
        CURRENT_VERSION_FILE.write_text(json.dumps(version_info, indent=2))
        print(f"GTFS extracted to {version_dir}")

        print("Files after extraction:")
        print(list(version_dir.glob("*")))
        return version_info

    check = check_source()
    changed = check_if_changed(check)
    download = download_and_extract_gtfs(changed)

    trigger_gtfs = TriggerDagRunOperator(
        task_id="trigger_ingest_gtfs",
        trigger_dag_id="ingest_gtfs",
        conf={
            "gtfs_version": "{{ ti.xcom_pull(task_ids='download_and_extract_gtfs')['version'] }}",
            "gtfs_path": "{{ ti.xcom_pull(task_ids='download_and_extract_gtfs')['path'] }}"
        },
        wait_for_completion=True,
    )

    trigger_stops = TriggerDagRunOperator(
        task_id="trigger_ingest_stops",
        trigger_dag_id="ingest_stops",
        conf={
            "gtfs_version": "{{ ti.xcom_pull(task_ids='download_and_extract_gtfs')['version'] }}",
            "gtfs_path": "{{ ti.xcom_pull(task_ids='download_and_extract_gtfs')['path'] }}"
        },
        wait_for_completion=True,
    )
    
    dbt_build = BashOperator(
        task_id="dbt_build",
        bash_command="cd /dbt && dbt run --select stop_arrival_stats",
    )

    check >> changed >> download >> trigger_gtfs >> trigger_stops >> dbt_build

update_gtfs()