import os
import sys
import pandas as pd
from datetime import datetime

# Add parent directory to sys.path to allow running from any cwd
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from scripts.database_utils import get_connection

def export_gold_to_disk():
    """
    Queries all gold tables and reporting marts from PostgreSQL
    and writes them to the workspace gold/ folder as CSV files.
    """
    print("\nExporting Gold layers and reporting marts to physical disk folders...")
    
    workspace_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    gold_disk_dir = os.path.join(workspace_dir, "gold")
    os.makedirs(gold_disk_dir, exist_ok=True)
    
    gold_tables = [
        "dim_channels",
        "dim_videos",
        "fact_video_stats",
        "mart_channel_performance",
        "mart_category_insights",
        "mart_kpi_summary"
    ]
    
    conn = get_connection()
    try:
        for table in gold_tables:
            table_disk_dir = os.path.join(gold_disk_dir, table)
            os.makedirs(table_disk_dir, exist_ok=True)
            
            # Query the table
            query = f"SELECT * FROM gold.{table};"
            df = pd.read_sql(query, conn)
            
            # Write to CSV
            csv_path = os.path.join(table_disk_dir, f"{table}.csv")
            df.to_csv(csv_path, index=False, encoding='utf-8')
            print(f"Exported gold.{table} to {csv_path} ({len(df)} rows).")
    except Exception as e:
        print(f"Failed to export gold tables to disk: {e}")
    finally:
        conn.close()

def run_gold_stage(batch_id):
    """
    Reads transform_gold.sql, replaces placeholders with current parameters,
    and runs the transformation queries in PostgreSQL.
    Also exports gold schemas back to the workspace as CSVs.
    """
    print("\n" + "="*40)
    print(f"RUNNING GOLD LAYER STAGE - BATCH ID: {batch_id}")
    print("="*40)
    
    # Path to SQL file
    sql_file_path = os.path.join(
        os.path.dirname(__file__), "sql", "transform_gold.sql"
    )
    
    if not os.path.exists(sql_file_path):
        print(f"Error: SQL transformation file {sql_file_path} not found.")
        sys.exit(1)
        
    # Generate execution timestamp
    exec_ts = datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')
    
    # Read and format the SQL
    with open(sql_file_path, 'r', encoding='utf-8') as f:
        sql_content = f.read()
        
    formatted_sql = sql_content.format(
        batch_id=batch_id,
        execution_timestamp=exec_ts
    )
    
    # Execute query
    print("Executing Gold layer transformations (SCD Type 1 & 2, Facts, Marts)...")
    conn = get_connection()
    cursor = conn.cursor()
    try:
        cursor.execute(formatted_sql)
        conn.commit()
        print("Gold transformations completed successfully.")
        
        # Export tables from Postgres back to disk folders
        export_gold_to_disk()
        
        # Print summary of dimensional state
        cursor.execute("SELECT COUNT(*) FROM gold.dim_channels;")
        dim_ch_count = cursor.fetchone()[0]
        cursor.execute("SELECT COUNT(*) FROM gold.dim_videos;")
        dim_vd_count = cursor.fetchone()[0]
        cursor.execute("SELECT COUNT(*) FROM gold.fact_video_stats;")
        fact_count = cursor.fetchone()[0]
        
        print("\n" + "="*40)
        print("GOLD LAYER SUMMARY")
        print(f"Channels in Dimension: {dim_ch_count} (including dummy & historical versions)")
        print(f"Videos in Dimension:   {dim_vd_count}")
        print(f"Total Fact Rows:       {fact_count}")
        print("="*40)
        
    except Exception as e:
        conn.rollback()
        print(f"Failed to execute Gold transformations: {e}")
        raise e
    finally:
        cursor.close()
        conn.close()

if __name__ == "__main__":
    import uuid
    run_id = sys.argv[1] if len(sys.argv) > 1 else f"local_run_{uuid.uuid4().hex[:8]}"
    run_gold_stage(run_id)
