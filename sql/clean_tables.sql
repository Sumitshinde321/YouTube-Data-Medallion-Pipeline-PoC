-- Script to clean and reset all pipeline tables for testing

TRUNCATE TABLE bronze.channels_raw CASCADE;
TRUNCATE TABLE bronze.videos_raw CASCADE;
TRUNCATE TABLE bronze.comments_raw CASCADE;

TRUNCATE TABLE silver.channels CASCADE;
TRUNCATE TABLE silver.videos CASCADE;
TRUNCATE TABLE silver.comments CASCADE;

-- Dim and Fact tables truncate (requires recreating dummy keys)
TRUNCATE TABLE gold.dim_channels CASCADE;
TRUNCATE TABLE gold.dim_videos CASCADE;
TRUNCATE TABLE gold.fact_video_stats CASCADE;
TRUNCATE TABLE gold.mart_channel_performance CASCADE;
TRUNCATE TABLE gold.mart_category_insights CASCADE;
TRUNCATE TABLE gold.mart_kpi_summary CASCADE;

TRUNCATE TABLE metadata.pipeline_runs CASCADE;
TRUNCATE TABLE metadata.dq_rules_run CASCADE;
TRUNCATE TABLE metadata.rejected_records CASCADE;
TRUNCATE TABLE metadata.error_logs CASCADE;

-- Reinsert dummy records
INSERT INTO gold.dim_channels (channel_sk, channel_id, name, keywords, tags, effective_start, effective_end, is_current, version)
VALUES (-1, 'UNKNOWN', 'Unknown Channel', 'UNKNOWN', 'UNKNOWN', '1900-01-01 00:00:00', '9999-12-31 23:59:59', TRUE, 1)
ON CONFLICT DO NOTHING;

INSERT INTO gold.dim_videos (video_sk, video_id, title, description, category_id, is_live_content, channel_id)
VALUES (-1, 'UNKNOWN', 'Unknown Video', 'UNKNOWN', 'UNKNOWN', FALSE, 'UNKNOWN')
ON CONFLICT (video_id) DO NOTHING;
