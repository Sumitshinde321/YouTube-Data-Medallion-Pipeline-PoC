import os
import sys
# Add parent directory to sys.path to allow running from any cwd
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import argparse
import uuid
from datetime import datetime
from scripts.database_utils import (
    start_pipeline_run, end_pipeline_run, fail_pipeline_run,
    run_sql_file
)
from scripts.stage_bronze import run_bronze_stage
from scripts.stage_silver import run_silver_stage
from scripts.stage_gold import run_gold_stage

def run_pipeline(batch_type, step, reset):
    """
    Orchestrates the entire YouTube Medallion Data Pipeline run.
    """
    # Create unique run ID
    run_id = f"run_{batch_type}_{datetime.now().strftime('%Y%m%d_%H%M%S')}_{uuid.uuid4().hex[:4]}"
    
    print("="*60)
    print(f"STARTING PIPELINE EXECUTION")
    print(f"Run ID:     {run_id}")
    print(f"Batch Type: {batch_type.upper()}")
    print(f"Step:       {step.upper()}")
    print(f"Reset DB:   {reset}")
    print("="*60)
    
    # 1. Reset Database if requested
    if reset:
        print("Resetting database schemas and tables...")
        try:
            # We initialize schemas first
            init_schemas_sql = os.path.join(os.path.dirname(__file__), "sql", "init_schemas.sql")
            run_sql_file(init_schemas_sql)
            
            # Recreate tables DDL
            create_tables_sql = os.path.join(os.path.dirname(__file__), "sql", "create_tables.sql")
            run_sql_file(create_tables_sql)
            
            # Clean tables (truncates any lingering data and seeds dummy keys)
            clean_tables_sql = os.path.join(os.path.dirname(__file__), "sql", "clean_tables.sql")
            run_sql_file(clean_tables_sql)
            print("Database setup complete.")
        except Exception as e:
            print(f"Database reset failed: {e}")
            sys.exit(1)
            
    # 2. Log run start in metadata
    start_pipeline_run(run_id, "youtube_medallion_pipeline", batch_type)
    
    # Track metrics
    records_processed = 0
    records_inserted = 0
    records_updated = 0
    records_rejected = 0
    
    try:
        # A. Bronze Layer
        if step in ['all', 'bronze']:
            bronze_loaded = run_bronze_stage(batch_type, run_id)
            # For Bronze, we consider loaded rows as raw processed
            records_processed = bronze_loaded
            
        # B. Silver Layer
        if step in ['all', 'silver']:
            # If running only silver, we check if raw data exists.
            raw_cnt, valid_cnt, rej_cnt = run_silver_stage(run_id)
            
            # If we didn't run bronze, update processed
            if step == 'silver':
                records_processed = raw_cnt
            records_inserted = valid_cnt  # Approximating valid records as inserted
            records_rejected = rej_cnt
            
        # C. Gold Layer
        if step in ['all', 'gold']:
            run_gold_stage(run_id)
            # Get latest counts of inserted/updated from database
            # For simplicity, we keep the counts tracked from silver.
            
        # 3. Log run completion
        end_pipeline_run(
            run_id=run_id,
            processed=records_processed,
            inserted=records_inserted,
            updated=records_updated,
            rejected=records_rejected
        )
        print("\n" + "="*60)
        print("PIPELINE COMPLETED SUCCESSFULLY!")
        print("="*60)
        
    except Exception as e:
        print(f"\nPipeline execution failed at step '{step}': {e}")
        fail_pipeline_run(run_id, str(e))
        sys.exit(1)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="YouTube Data Medallion Pipeline CLI")
    parser.add_argument(
        "--batch", 
        choices=["initial", "incremental"], 
        default="initial",
        help="Batch to run: 'initial' or 'incremental'"
    )
    parser.add_argument(
        "--step", 
        choices=["all", "bronze", "silver", "gold"], 
        default="all",
        help="Pipeline phase to execute"
    )
    parser.add_argument(
        "--reset", 
        action="store_true", 
        help="Drop, recreate and clean tables before executing initial batch"
    )
    
    args = parser.parse_args()
    
    # Enforce database reset constraints
    if args.reset and args.batch == "incremental":
        print("Warning: Cannot reset database for an incremental run. Reset flag ignored.")
        args.reset = False
        
    run_pipeline(args.batch, args.step, args.reset)
