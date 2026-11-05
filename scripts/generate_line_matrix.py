import os
from pathlib import Path

import pandas as pd
from sqlalchemy import URL, create_engine


def load_project_environment():
    env_file = Path(__file__).resolve().parents[1] / ".env"
    if not env_file.is_file():
        return

    for line in env_file.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        value = value.strip()
        if len(value) >= 2 and value[0] == value[-1] and value[0] in {"'", '"'}:
            value = value[1:-1]
        os.environ.setdefault(key.strip(), value)


load_project_environment()

DB_URL = URL.create(
    "postgresql+psycopg2",
    username=os.getenv("POSTGRES_USER", "project18"),
    password=os.getenv("POSTGRES_PASSWORD", "project18"),
    host=os.getenv("POSTGRES_HOST", "localhost"),
    port=int(os.getenv("POSTGRES_PORT", "5434")),
    database=os.getenv("POSTGRES_DB", "project18"),
)


def export_line_matrices_by_day_type():
    print("Connecting to database...")
    engine = create_engine(DB_URL)

    query = """
        SELECT
            line_no,
            direction_id,
            day_type,
            trip_id,
            stop_id,
            stop_sequence,
            arrival_time
        FROM gold.schedule_stop_arrivals
        ORDER BY line_no, direction_id, day_type, trip_id, stop_sequence;
    """

    print("Querying schedule data from PostgreSQL...")
    with engine.connect() as connection:
        df = pd.read_sql(query, connection)
    print(f"Retrieved {len(df)} total stop schedule records.")

    if df.empty:
        print("Warning: Query returned 0 rows. Verify Bronze and Gold schedule models.")
        return

    df["arrival_time"] = df["arrival_time"].astype(str).str.slice(0, 5)

    output_dir = Path("csv_data/matrices")
    output_dir.mkdir(parents=True, exist_ok=True)

    grouped = df.groupby(["line_no", "direction_id", "day_type"])

    count = 0
    for (line, direction, day_type), group in grouped:
        ordered_stops = group.sort_values("stop_sequence")["stop_id"].unique()

        matrix = group.pivot_table(
            index="trip_id",
            columns="stop_id",
            values="arrival_time",
            aggfunc="first",
        )

        matrix = matrix.reindex(columns=ordered_stops)

        filename = (
            output_dir
            / f"line{line}_dir{direction}_{day_type.lower()}_matrix.csv"
        )
        matrix.to_csv(filename, sep=";", index_label="Time")
        count += 1
        print(f"Generated matrix file: {filename}")

    print(f"\nDone! Successfully generated {count} timetable matrices in HH:MM format.")


if __name__ == "__main__":
    export_line_matrices_by_day_type()
