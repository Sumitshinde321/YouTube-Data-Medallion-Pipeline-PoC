import os
import sys
# Add parent directory to sys.path to allow running from any cwd
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import pandas as pd
from io import StringIO
from datetime import datetime
from scripts.config import SOURCE_DATA_DIR, CHUNK_SIZE
from scripts.database_utils import get_connection


def get_last_loaded(cursor):
    cursor.execute("""
        SELECT last_loaded
        FROM metadata.pipeline_metadata
        WHERE pipeline_name='videos_pipeline'
    """)

    row = cursor.fetchone()

    if row:
        return row[0]

    return None


def load_csv_to_bronze(file_path, table_name, batch_id, chunk_size=CHUNK_SIZE):
    """
    Ingests a CSV file into the specified Bronze raw table in PostgreSQL.

    BATCH PROCESSING: the source CSV is streamed in chunks of `chunk_size`
    rows (via pandas' chunksize reader) rather than loaded whole into memory.
    Each chunk is independently:
      1. cleaned/tagged with metadata columns,
      2. appended to the immutable raw file on disk,
      3. COPY'd into Postgres.
    This keeps peak memory roughly constant regardless of file size.
    """
    if not os.path.exists(file_path):
        print(f"Error: File {file_path} does not exist. Skipping.")
        return 0

    print(f"Loading {file_path} into {table_name} (chunk_size={chunk_size})...")

    # Bronze disk destination (Immutable RAW partitioned storage)
    table_short_name = table_name.split('.')[-1].replace('_raw', '')
    bronze_disk_dir = os.path.join(
        os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
        "bronze", table_short_name, batch_id
    )
    os.makedirs(bronze_disk_dir, exist_ok=True)
    bronze_disk_path = os.path.join(bronze_disk_dir, f"{table_short_name}_raw.csv")

    # Remove any partial file from a previous failed run so appends start clean
    if os.path.exists(bronze_disk_path):
        os.remove(bronze_disk_path)

    total_rows = 0
    first_chunk = True

    conn = get_connection()
    cursor = conn.cursor()
    try:
        reader = pd.read_csv(file_path, dtype=str, keep_default_na=False, chunksize=chunk_size)

        for chunk_num, chunk in enumerate(reader, start=1):
            chunk.columns = chunk.columns.str.strip()
            for col in chunk.columns:
                chunk[col] = chunk[col].astype(str).str.rstrip('\r')

            # Add metadata columns
            chunk['_loaded_at'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')
            chunk['_source_file'] = os.path.basename(file_path)
            chunk['_batch_id'] = batch_id

            # Append this chunk to the on-disk raw copy (header only once)
            chunk.to_csv(bronze_disk_path, mode='a', index=False,
                         header=first_chunk, encoding='utf-8')

            # Build an in-memory buffer for just this chunk and COPY it
            buffer = StringIO()
            chunk.to_csv(buffer, index=False, header=False, sep='|', na_rep='\\N')
            buffer.seek(0)

            columns = [f'"{col}"' if not col.startswith('_') else col for col in chunk.columns]
            columns_str = ", ".join(columns)
            copy_query = f"COPY {table_name} ({columns_str}) FROM STDIN WITH CSV DELIMITER '|' NULL '\\N'"

            cursor.copy_expert(copy_query, buffer)
            conn.commit()

            total_rows += len(chunk)
            first_chunk = False
            print(f"  chunk {chunk_num}: loaded {len(chunk)} rows (running total: {total_rows})")

        print(f"Successfully loaded {total_rows} rows into {table_name}.")
        return total_rows
    except Exception as e:
        conn.rollback()
        print(f"Failed to load {table_name}: {e}")
        raise e
    finally:
        cursor.close()
        conn.close()


def run_bronze_stage(batch_type, batch_id):
    """
    Orchestrates the Bronze ingestion stage.
    """
    print("\n" + "="*40)
    print(f"RUNNING BRONZE LAYER STAGE - BATCH: {batch_type.upper()}")
    print("="*40)

    batch_dir = os.path.join(SOURCE_DATA_DIR, batch_type)

    # Ingest the 3 tables (each streamed in chunks)
    channels_loaded = load_csv_to_bronze(
        os.path.join(batch_dir, "channels.csv"),
        "bronze.channels_raw",
        batch_id
    )

    videos_loaded = load_csv_to_bronze(
        os.path.join(batch_dir, "videos.csv"),
        "bronze.videos_raw",
        batch_id
    )

    comments_loaded = load_csv_to_bronze(
        os.path.join(batch_dir, "comments.csv"),
        "bronze.comments_raw",
        batch_id
    )

    total_loaded = channels_loaded + videos_loaded + comments_loaded
    print(f"Bronze Stage Completed. Total Raw Records Loaded: {total_loaded}")
    return total_loaded


if __name__ == "__main__":
    import uuid
    batch = sys.argv[1] if len(sys.argv) > 1 else "initial"
    run_id = sys.argv[2] if len(sys.argv) > 2 else f"local_run_{uuid.uuid4().hex[:8]}"
    run_bronze_stage(batch, run_id)
