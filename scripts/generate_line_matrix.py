import os
import pandas as pd
from sqlalchemy import create_engine

# Database connection URI
DB_URI = "postgresql://project18:project18@localhost:5432/project18"

def export_line_matrices_by_day_type():
    print("Connecting to database...")
    engine = create_engine(DB_URI)
    
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
    df = pd.read_sql(query, engine)
    print(f"Retrieved {len(df)} total stop schedule records.")
    
    if df.empty:
        print("Warning: Query returned 0 rows. Verify bronze.raw_stop_times and silver.silver_trips.")
        return

    # 1. Format arrival_time to show only HH:MM (strip :SS)
    df['arrival_time'] = df['arrival_time'].astype(str).str.slice(0, 5)

    output_dir = "csv_data/matrices"
    os.makedirs(output_dir, exist_ok=True)

    grouped = df.groupby(['line_no', 'direction_id', 'day_type'])

    count = 0
    for (line, direction, day_type), group in grouped:
        ordered_stops = group.sort_values('stop_sequence')['stop_id'].unique()
        
        matrix = group.pivot_table(
            index='trip_id',
            columns='stop_id',
            values='arrival_time',
            aggfunc='first'
        )
        
        matrix = matrix.reindex(columns=ordered_stops)
        
        filename = f"{output_dir}/line{line}_dir{direction}_{day_type.lower()}_matrix.csv"
        matrix.to_csv(filename, sep=';', index_label='Time')
        count += 1
        print(f"Generated matrix file: {filename}")

    print(f"\nDone! Successfully generated {count} timetable matrices in HH:MM format.")

if __name__ == "__main__":
    export_line_matrices_by_day_type()