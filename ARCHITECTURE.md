# YouTube Data Engineering Medallion Pipeline - Architectural Specification

This document provides a detailed technical breakdown of the architectural layers, database schemas, and transformation logic implemented in the YouTube Data Medallion Pipeline PoC.

---

## 1. Medallion Layer Design

```
Raw CSV Dataset (Desktop)
       │
       ▼
 ┌───────────┐
 │  BRONZE   │  - raw.channels_raw, raw.videos_raw, raw.comments_raw
 │  (Raw)    │  - Immutable text storage, records loaded with zero schema casting
 └─────┬─────┘
       │  Data Quality Validation & Cleansing
       ▼
 ┌───────────┐
 │  SILVER   │  - silver.channels, silver.videos, silver.comments
 │ (Cleaned) │  - Strictly typed, cleansed strings, parsed dates, metric conversion
 └─────┬─────┘
       │  SCD Type 1 & 2 Merge, Joins, Derived Metrics, Aggregations
       ▼
 ┌───────────┐
 │   GOLD    │  - dim_channels, dim_videos, fact_video_stats (Star Schema)
 │ (Marts)   │  - mart_channel_performance, mart_category_insights, mart_kpi_summary
 └───────────┘
```

### Bronze Layer (Raw Ingest)
- Ingested files retain their exact CSV column naming.
- All column types are defined as `TEXT` to absorb structural fluctuations.
- **Audit Columns Added**:
  - `_loaded_at`: Timestamp indicating when the row was stored.
  - `_source_file`: Name of the source file (e.g. `channels.csv`).
  - `_batch_id`: Ingestion execution ID mapping directly to the metadata table.

### Silver Layer (Cleansing & Conforming)
- Applies trim logic, nullifies text string representations of nulls (e.g. `'nan'`).
- Cleans and converts YouTube metrics with shorthand qualifiers (e.g. `12.5K` $\rightarrow$ `12500`, `3M` $\rightarrow$ `3000000`) before casting.
- Normalizes timestamp formats to SQL-compatible datetime objects.

### Gold Layer (Dimensional Model)
Designed using a standard **Star Schema** dimensional modeling pattern optimized for BI dashboards (Power BI):
- **Surrogate Keys**: Integer SERIAL keys are generated for the dimensions (`channel_sk`, `video_sk`) to support indexing and temporal SCD tracking.
- **Referential Integrity**: A dummy row with a surrogate key of `-1` (`UNKNOWN`) is seeded in all dimension tables. If a fact row references a dimension key that does not exist in the dimension table (e.g. an orphaned video or comment), the join falls back to `-1` instead of dropping the row, preventing reporting gaps.

---

## 2. Slowly Changing Dimensions (SCD)

### SCD Type 2 (Channels)
Tracks the historical updates of channels (e.g. tracking channel name or keyword updates over time).
- **Columns**:
  - `channel_sk` (Primary Key, generated integer)
  - `channel_id` (Business key from YouTube)
  - `effective_start`: Timestamp when this version became active.
  - `effective_end`: Timestamp when this version was closed out (active records have `9999-12-31 23:59:59`).
  - `is_current`: Boolean flag indicating if this is the active representation.
  - `version`: Monotonically increasing version number starting at `1`.
- **SQL Mechanics**:
  1. Detect updates: Find rows in `silver.channels` where attributes differ from active records in `gold.dim_channels`.
  2. Close old version: Update the active record's `effective_end = NOW()`, `is_current = FALSE`.
  3. Open new version: Insert a new row with `version = old_version + 1`, `effective_start = NOW()`, `is_current = TRUE`.
  4. Ingest new entities: Insert brand new channels with `version = 1`, `effective_start = '1900-01-01 00:00:00'`, `is_current = TRUE`.

### SCD Type 1 (Videos)
Overwrites old records with the newest values. Used for attributes where history tracking is not required (e.g. changing title, description, or category mappings).
- **SQL Mechanics**:
  - Uses PostgreSQL `ON CONFLICT (video_id) DO UPDATE SET title = EXCLUDED.title, ...` to overwrite existing rows while keeping surrogate keys (`video_sk`) unchanged.

---

## 3. Data Quality (DQ) & Rejected Records Framework

Before staging data from Bronze to Silver, every batch is verified against a strict validation matrix:

| Table | Column | Rule Type | Description / Constraint |
|---|---|---|---|
| Channels | `Id` | Null Check | Rejects rows if ID is empty or null. |
| Channels | `Id` | Duplicate Check | Removes batch duplicates, keeping the first row. |
| Videos | `Id` | Null Check | Rejects rows if ID is empty or null. |
| Videos | `ChannelId` | Null Check | Rejects rows if Channel ID is empty or null. |
| Videos | `ViewsCount` | Positive Check | Rejects rows if Views count is negative. |
| Videos | `GrabDate` | Date Check | Rejects rows if GrabDate is not a valid timestamp. |
| Comments | `Id` | Null Check | Rejects rows if ID is empty or null. |
| Comments | `VideoId` | Null Check | Rejects rows if Video ID is empty or null. |

### Rejected Record Handler
If a row fails any validation check:
- It is excluded from the Silver staging table.
- The raw row representation is serialized as a JSON string and written to the `metadata.rejected_records` audit log.
- The failure reason (e.g. `"Invalid LikesCount integer type; ViewsCount cannot be negative;"`) is written alongside the row, allowing engineers to audit source anomalies.
- The pipeline count of rejected records is updated in the run execution log.

---

## 4. Metadata & Auditing Framework

Provides complete operational observability:

- `metadata.pipeline_runs`: Tracks every job run with status, start/end timestamps, duration, and processed metrics:
  - `records_processed`: Total raw rows ingested to Bronze.
  - `records_inserted`: Total clean rows written to Silver.
  - `records_rejected`: Total invalid rows isolated.
- `metadata.dq_rules_run`: Logs the count of passed and failed records for every validation rule executed during a run, creating an audit trail of data health over time.
- `metadata.error_logs`: Captures runtime Python exceptions and SQL constraint failures, storing the exact error message and execution stack trace.
