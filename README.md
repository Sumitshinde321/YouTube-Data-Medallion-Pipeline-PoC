# YouTube Data Engineering Medallion Pipeline PoC

This project is an end-to-end open-source data engineering pipeline implementing the **Medallion Architecture (Bronze -> Silver -> Gold)**. It orchestrates the ingestion, validation, cleansing, and transformation of a YouTube dataset using **Apache Airflow**, **PostgreSQL**, and **Docker**.

The final Gold layer is designed to be easily consumed by **Power BI** for building dashboards.

---

## Architecture Overview

The pipeline implements a **hybrid Lakehouse architecture**, organizing and storing data in both a **PostgreSQL database** (for query speed and relations) and mirroring the layers as **physical directories on disk** within the workspace for direct file access:

1. **Bronze Layer (Raw Ingestion)**:
   - *Database*: Ingests raw CSV fields as `TEXT` tables under the `bronze` schema to prevent type mismatches.
   - *Filesystem (`bronze/`)*: Stores the raw CSV files partitioned by batch run ID (e.g. `bronze/videos/run_xxx/videos_raw.csv`), maintaining an immutable log.
2. **Silver Layer (Cleansed & Standardized)**:
   - *Database*: Cleanses, standardizes string columns, converts YouTube metric strings (e.g., `10K` to `10000`, `2.7M` to `2700000`), and stages rows in the `silver` schema.
   - *Filesystem (`silver/`)*: Stores valid cleansed records as clean CSV files under `silver/` partitioned by batch run ID. Rejections are isolated in `metadata/rejected/`.
3. **Gold Layer (Dimensional Star Schema)**:
   - *Database*: Implements SCD Type 2 history versioning for channels and SCD Type 1 updates for videos. Links facts using temporal joins, and generates reporting marts.
   - *Filesystem (`gold/`)*: Exports the consolidated Gold dimensions, facts, and reporting marts as CSV files (e.g. `gold/dim_channels/dim_channels.csv`) automatically at the end of each run.
4. **Metadata (Auditing & DQ)**: Tracks pipeline metrics, runs audits, and stores error logs.

---

## Directory Structure

```
├── dags/
│   └── youtube_medallion_pipeline.py  # Apache Airflow DAG
├── scripts/
│   ├── sql/
│   │   ├── init_dbs.sql               # Initializes DW and Airflow DBs in container
│   │   ├── init_schemas.sql           # Schema setup (bronze, silver, gold, metadata)
│   │   ├── create_tables.sql          # Table DDL definitions
│   │   ├── transform_gold.sql         # SQL query script for Gold ETL
│   │   └── clean_tables.sql           # SQL script to reset database state
│   ├── config.py                      # DB connection and path environment configs
│   ├── database_utils.py              # Run logging, audit, and connection utilities
│   ├── seed_sources.py                # Splits and sub-samples raw dataset for testing
│   ├── stage_bronze.py                # Raw copy loader for Bronze
│   ├── stage_silver.py                # DQ validation, cleansing, and Silver loader
│   ├── stage_gold.py                  # Executes Gold transformations
│   └── run_pipeline.py                # Central CLI pipeline wrapper
├── Dockerfile                         # Custom Airflow image with dependencies
├── docker-compose.yml                 # Orchestration setup for Postgres and Airflow
├── requirements.txt                   # Local python testing requirements
└── README.md                          # Quick start instructions
```

---

## Quick Start Setup

### Prerequisites
- Docker & Docker Compose
- Python 3.9+ (if running scripts locally)
- A folder on your Desktop named `YouTube Dataset` containing the 3 CSV files (`channels.csv`, `videos.csv`, `comments.csv`).

### Step 1: Run Seeding & Sub-sampling
To simulate initial and incremental loading cycles and verify the code on smaller datasets (to save laptop memory and time), run:
```bash
# Installs python libraries locally
pip install -r requirements.txt

# Sub-samples 50,000 rows by default and splits them into initial and incremental folders
python scripts/seed_sources.py --size 50000
```
This creates a folder named `source_data/` containing:
- `initial/`: Initial data (70% load).
- `incremental/`: New data + modified channel names (tests SCD Type 2) + modified video stats + 5 injected faulty rows (tests data quality rejection).

*Note: Pass `--full` if you want to seed the entire 600MB raw dataset.*

---

## Running with Docker and Airflow

1. **Spin up the stack**:
   ```bash
   docker compose up -d --build
   ```
2. **Access the Airflow UI**:
   Open `http://localhost:8080` in your browser.
   - **Username**: `admin`
   - **Password**: `admin`
3. **Unpause and Trigger the DAG**:
   - Locate the DAG `youtube_medallion_pipeline`.
   - Click the toggle to **Unpause** it.
   - Click the play button to **Trigger DAG**.
   - By default, it runs the `initial` batch.
   - To run the `incremental` batch, trigger the DAG with the config:
     ```json
     {"batch": "incremental"}
     ```

---

## Running Locally (Alternative)

If you have PostgreSQL installed on your machine or want to test execution without Airflow, you can run the CLI script locally (with the Postgres Docker container running):

```bash
# 1. Reset schemas and execute the initial batch loader (bronze -> silver -> gold)
python scripts/run_pipeline.py --batch initial --step all --reset

# 2. Load the incremental batch (adds new data, triggers SCD history, logs 5 rejected rows)
python scripts/run_pipeline.py --batch incremental --step all
```

---

## Connecting to Power BI

To build your Power BI dashboards:
1. Open Power BI Desktop.
2. Select **Get Data** $\rightarrow$ **PostgreSQL database**.
3. Set **Server** to `localhost` and **Database** to `youtube_dw`.
4. Enter credentials: Username = `airflow`, Password = `airflow`.
5. Select tables from the `gold` schema:
   - `dim_channels`
   - `dim_videos`
   - `fact_video_stats`
   - `mart_channel_performance`
   - `mart_category_insights`
   - `mart_kpi_summary`
6. Model your relations matching `fact_video_stats.video_sk` to `dim_videos.video_sk` and `fact_video_stats.channel_sk` to `dim_channels.channel_sk`.
