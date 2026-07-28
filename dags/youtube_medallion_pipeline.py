from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator

default_args = {
    'owner': 'data_engineering_team',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

with DAG(
    'youtube_medallion_pipeline',
    default_args=default_args,
    description='An end-to-end Medallion pipeline for YouTube analytics in Postgres',
    schedule_interval=None,  # Manually triggered
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=['youtube', 'medallion', 'pg'],
) as dag:

    # 1. Start pipeline task
    start_pipeline = EmptyOperator(
        task_id='start_pipeline'
    )

    # 2. Check and seed files (sub-sampled to 50,000 records by default)
    # This runs the seed_sources script if files don't exist, or resets them based on conf
    seed_files = BashOperator(
        task_id='seed_source_files',
        bash_command='python /opt/airflow/scripts/seed_sources.py --size 50000',
        env={
            'DB_HOST': 'postgres',
            'RAW_DATASET_DIR': '/opt/airflow/data',  # Path inside container
            'SOURCE_DATA_DIR': '/opt/airflow/source_data'
        }
    )

    # 3. Ingest to Bronze Layer
    # Uses Jinja templating to read the "batch" parameter from the trigger configuration (defaults to 'initial')
    # If the user passes {"reset": true} during manual trigger, we reset the DB (only for initial run)
    run_bronze = BashOperator(
        task_id='stage_bronze_layer',
        bash_command='''
        batch="{{ dag_run.conf.get('batch', 'initial') }}"
        reset_flag=""
        if [ "$batch" = "initial" ] && [ "{{ dag_run.conf.get('reset', 'false') }}" = "true" ]; then
            reset_flag="--reset"
        fi
        python /opt/airflow/scripts/run_pipeline.py --batch "$batch" --step bronze $reset_flag
        ''',
        env={
            'DB_HOST': 'postgres',
            'SOURCE_DATA_DIR': '/opt/airflow/source_data'
        }
    )

    # 4. Cleansing & Validation -> Silver Layer
    run_silver = BashOperator(
        task_id='validate_and_stage_silver_layer',
        bash_command='python /opt/airflow/scripts/run_pipeline.py --batch "{{ dag_run.conf.get(\'batch\', \'initial\') }}" --step silver',
        env={
            'DB_HOST': 'postgres',
            'SOURCE_DATA_DIR': '/opt/airflow/source_data'
        }
    )

    # 5. SCD & Aggregations -> Gold Layer
    run_gold = BashOperator(
        task_id='transform_and_load_gold_layer',
        bash_command='python /opt/airflow/scripts/run_pipeline.py --batch "{{ dag_run.conf.get(\'batch\', \'initial\') }}" --step gold',
        env={
            'DB_HOST': 'postgres',
            'SOURCE_DATA_DIR': '/opt/airflow/source_data'
        }
    )

    # 6. End pipeline task
    end_pipeline = EmptyOperator(
        task_id='end_pipeline'
    )

    # Define task dependencies
    start_pipeline >> seed_files >> run_bronze >> run_silver >> run_gold >> end_pipeline
