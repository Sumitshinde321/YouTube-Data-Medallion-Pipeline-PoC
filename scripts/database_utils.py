import os
import sys
# Add parent directory to sys.path to allow running from any cwd
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import psycopg2
import traceback
from sqlalchemy import create_engine
from datetime import datetime
from scripts.config import DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASSWORD, DATABASE_URL

def get_connection():
    """Establish a direct psycopg2 connection to the PostgreSQL database."""
    return psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        database=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD
    )

def get_engine():
    """Get a SQLAlchemy engine for pandas integration."""
    return create_engine(DATABASE_URL)

def run_sql_file(file_path):
    """Execute a SQL script file."""
    conn = get_connection()
    cursor = conn.cursor()
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            sql = f.read()
        cursor.execute(sql)
        conn.commit()
    except Exception as e:
        conn.rollback()
        print(f"Error executing SQL file {file_path}: {e}")
        raise e
    finally:
        cursor.close()
        conn.close()

# ==========================================
# METADATA & PIPELINE TRACKING FUNCTIONS
# ==========================================

def start_pipeline_run(run_id, pipeline_name, batch_type):
    """Log the start of a pipeline run in the database."""
    conn = get_connection()
    cursor = conn.cursor()
    try:
        cursor.execute(
            """
            INSERT INTO metadata.pipeline_runs (run_id, pipeline_name, batch_type, start_time, status)
            VALUES (%s, %s, %s, %s, 'running')
            ON CONFLICT (run_id) DO UPDATE 
            SET start_time = EXCLUDED.start_time, status = 'running', end_time = NULL;
            """,
            (run_id, pipeline_name, batch_type, datetime.now())
        )
        conn.commit()
    except Exception as e:
        print(f"Failed to start pipeline run metadata: {e}")
    finally:
        cursor.close()
        conn.close()

def end_pipeline_run(run_id, processed, inserted, updated, rejected):
    """Log the successful completion of a pipeline run."""
    conn = get_connection()
    cursor = conn.cursor()
    try:
        cursor.execute(
            """
            SELECT start_time FROM metadata.pipeline_runs WHERE run_id = %s;
            """,
            (run_id,)
        )
        row = cursor.fetchone()
        start_time = row[0] if row else datetime.now()
        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()

        cursor.execute(
            """
            UPDATE metadata.pipeline_runs
            SET end_time = %s,
                status = 'success',
                duration_seconds = %s,
                records_processed = %s,
                records_inserted = %s,
                records_updated = %s,
                records_rejected = %s
            WHERE run_id = %s;
            """,
            (end_time, duration, processed, inserted, updated, rejected, run_id)
        )
        conn.commit()
    except Exception as e:
        print(f"Failed to end pipeline run metadata: {e}")
    finally:
        cursor.close()
        conn.close()

def fail_pipeline_run(run_id, error_message=None):
    """Log a pipeline run failure, logging stack trace if available."""
    conn = get_connection()
    cursor = conn.cursor()
    try:
        cursor.execute(
            """
            SELECT start_time FROM metadata.pipeline_runs WHERE run_id = %s;
            """,
            (run_id,)
        )
        row = cursor.fetchone()
        start_time = row[0] if row else datetime.now()
        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()

        cursor.execute(
            """
            UPDATE metadata.pipeline_runs
            SET end_time = %s,
                status = 'failed',
                duration_seconds = %s
            WHERE run_id = %s;
            """,
            (end_time, duration, run_id)
        )
        conn.commit()
        
        if error_message:
            tb = traceback.format_exc()
            log_pipeline_error(run_id, error_message, tb)
            
    except Exception as e:
        print(f"Failed to fail pipeline run metadata: {e}")
    finally:
        cursor.close()
        conn.close()

def log_pipeline_error(run_id, error_message, stack_trace):
    """Log an explicit runtime error inside the metadata.error_logs table."""
    conn = get_connection()
    cursor = conn.cursor()
    try:
        cursor.execute(
            """
            INSERT INTO metadata.error_logs (run_id, error_message, stack_trace)
            VALUES (%s, %s, %s);
            """,
            (run_id, error_message, stack_trace)
        )
        conn.commit()
    except Exception as e:
        print(f"Failed to log error metadata: {e}")
    finally:
        cursor.close()
        conn.close()

def log_dq_rule(run_id, table_name, column_name, rule_type, status, failed_count):
    """Log a Data Quality rule result in metadata.dq_rules_run."""
    conn = get_connection()
    cursor = conn.cursor()
    try:
        cursor.execute(
            """
            INSERT INTO metadata.dq_rules_run (run_id, table_name, column_name, rule_type, status, failed_records_count)
            VALUES (%s, %s, %s, %s, %s, %s);
            """,
            (run_id, table_name, column_name, rule_type, status, failed_count)
        )
        conn.commit()
    except Exception as e:
        print(f"Failed to log DQ rule metadata: {e}")
    finally:
        cursor.close()
        conn.close()

def log_rejected_records_batch(run_id, table_name, df_rejected, reason_column):
    """Efficiently log a batch of rejected records into metadata.rejected_records."""
    if df_rejected.empty:
        return
    
    conn = get_connection()
    cursor = conn.cursor()
    try:
        # Convert df row-by-row into JSON/string format and execute insert
        records = []
        for idx, row in df_rejected.iterrows():
            reason = row[reason_column]
            # Convert row to dictionary excluding the error reason column
            row_dict = row.drop(labels=[reason_column]).to_dict()
            row_json = str(row_dict)
            records.append((run_id, table_name, row_json, reason))
        
        cursor.executemany(
            """
            INSERT INTO metadata.rejected_records (run_id, source_table, raw_row_data, failure_reason)
            VALUES (%s, %s, %s, %s);
            """,
            records
        )
        conn.commit()
    except Exception as e:
        conn.rollback()
        print(f"Failed to log rejected records batch: {e}")
    finally:
        cursor.close()
        conn.close()
