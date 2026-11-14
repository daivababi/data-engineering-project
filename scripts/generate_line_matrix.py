import os
from pathlib import Path

import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.engine import make_url

# Database connection URI
DB_URI = "postgresql://project18:project18@localhost:5432/project18"

def export_line_matrices_by_day_type():
    print("Connecting to database...")
    database_url = make_url(DB_URI).set(
        port=int(os.getenv("POSTGRES_PORT", "5434"))
    )
    engine = create_engine(database_url)
    
    query = """
        SELECT DISTINCT
            arrivals.line_no,
            trips.direction_id,
            COALESCE(calendar.day_type, 'Exceptions') AS day_type,
            arrivals.trip_id,
            arrivals.stop_id,
            arrivals.stop_sequence,
            split_part(arrivals.scheduled_arrival_gtfs, ':', 1)
                || ':' || split_part(arrivals.scheduled_arrival_gtfs, ':', 2)
                AS arrival_time
        FROM silver.silver_scheduled_arrivals AS arrivals
        JOIN silver.silver_trips AS trips
            ON arrivals.trip_id = trips.trip_id
        LEFT JOIN silver.silver_calendar AS calendar
            ON arrivals.service_id = calendar.service_id
        ORDER BY
            arrivals.line_no,
            trips.direction_id,
            day_type,
            arrivals.trip_id,
            arrivals.stop_sequence;
    """
    
    print("Querying schedule data from PostgreSQL...")
    df = pd.read_sql(query, engine)
    print(f"Retrieved {len(df)} total stop schedule records.")
    
    if df.empty:
        print("Warning: Query returned 0 rows. Verify bronze.raw_stop_times and silver.silver_trips.")
        return

    # Format arrival_time to show only HH:MM (strip :SS)
    df['arrival_time'] = df['arrival_time'].astype(str).str.slice(0, 5)

    output_dir = Path("csv_data") / "matrices"
    output_dir.mkdir(parents=True, exist_ok=True)

    grouped = df.groupby(['line_no', 'direction_id', 'day_type'])

    count = 0
    for (line, direction, day_type), group in grouped:
        group = group.sort_values(
            ['trip_id', 'stop_sequence', 'stop_id']
        ).copy()
        group['stop_visit'] = (
            group.groupby(['trip_id', 'stop_id']).cumcount() + 1
        )
        group['stop_column'] = group['stop_id'].astype(str)
        repeated_stops = group['stop_visit'] > 1
        group.loc[repeated_stops, 'stop_column'] += (
            '_' + group.loc[repeated_stops, 'stop_visit'].astype(str)
        )

        ordered_stops = (
            group.sort_values('stop_sequence')
            .drop_duplicates('stop_column')['stop_column']
            .tolist()
        )
        cells = (
            group.groupby(['trip_id', 'stop_column'], sort=False)['arrival_time']
            .agg(lambda values: ' / '.join(dict.fromkeys(values)))
            .reset_index()
        )
        matrix = cells.pivot(
            index='trip_id',
            columns='stop_column',
            values='arrival_time'
        ).reindex(columns=ordered_stops)

        trip_start_times = (
            group.sort_values('stop_sequence')
            .drop_duplicates('trip_id')
            .set_index('trip_id')['arrival_time']
        )
        matrix.insert(0, 'Time', trip_start_times.reindex(matrix.index))
        matrix.insert(1, 'Trip', matrix.index)
        matrix.insert(0, 'Route', str(line))
        matrix = matrix.sort_values(['Time', 'Trip'])
        
        filename = output_dir / (
            f"line{line}_dir{direction}_{day_type.lower()}.csv"
        )
        matrix.to_csv(filename, sep=";", index=False)
        count += 1
        print(f"Generated matrix file: {filename}")

    print(
        f"\nDone! Generated {count} timetable matrices. "
        "Each row is a trip ordered by its first-stop arrival time (HH:MM). "
        "Repeated stop visits have columns such as 20307_2."
    )

if __name__ == "__main__":
    export_line_matrices_by_day_type()
