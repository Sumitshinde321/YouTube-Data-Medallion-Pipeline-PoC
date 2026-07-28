-- =========================================================================
-- transform_gold.sql
-- Performs SCD Type 1, SCD Type 2, Fact loading, and Mart aggregations.
-- This script contains placeholders (like {batch_id}) that are replaced
-- with actual run parameters before execution in stage_gold.py.
-- =========================================================================

-- ==========================================
-- 1. SCD TYPE 2: CHANNELS DIMENSION
-- ==========================================

-- A. Identify records that are new or have changed attributes (SCD Type 2 trigger)
CREATE TEMP TABLE changed_channels ON COMMIT DROP AS
SELECT 
    s.channel_id,
    s.name,
    s.keywords,
    s.tags,
    d.version as old_version,
    d.channel_sk as old_sk
FROM silver.channels s
JOIN gold.dim_channels d 
    ON s.channel_id = d.channel_id 
    AND d.is_current = TRUE
WHERE s._batch_id = '{batch_id}'
  AND (
      s.name IS DISTINCT FROM d.name OR 
      s.keywords IS DISTINCT FROM d.keywords OR
      s.tags IS DISTINCT FROM d.tags
  );

-- B. Close out the existing active records by setting effective_end to current timestamp and is_current = FALSE
UPDATE gold.dim_channels d
SET 
    effective_end = '{execution_timestamp}',
    is_current = FALSE,
    _updated_at = CURRENT_TIMESTAMP
FROM changed_channels c
WHERE d.channel_sk = c.old_sk;

-- C. Insert new active records for the updated channels (increment version)
INSERT INTO gold.dim_channels (
    channel_id, name, keywords, tags, 
    effective_start, effective_end, is_current, version, _created_at, _updated_at
)
SELECT 
    channel_id, name, keywords, tags, 
    '{execution_timestamp}'::TIMESTAMP, '9999-12-31 23:59:59'::TIMESTAMP, TRUE, old_version + 1, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP
FROM changed_channels;

-- D. Insert completely new channels that don't exist in dim_channels yet (version = 1)
INSERT INTO gold.dim_channels (
    channel_id, name, keywords, tags, 
    effective_start, effective_end, is_current, version, _created_at, _updated_at
)
SELECT 
    s.channel_id, s.name, s.keywords, s.tags, 
    '1900-01-01 00:00:00'::TIMESTAMP, '9999-12-31 23:59:59'::TIMESTAMP, TRUE, 1, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP
FROM silver.channels s
LEFT JOIN gold.dim_channels d ON s.channel_id = d.channel_id
WHERE s._batch_id = '{batch_id}' 
  AND d.channel_id IS NULL;


-- ==========================================
-- 2. SCD TYPE 1: VIDEOS DIMENSION
-- ==========================================

-- Overwrites values on conflict (SCD Type 1)
INSERT INTO gold.dim_videos (
    video_id, title, description, category_id, is_live_content, channel_id, _created_at, _updated_at
)
SELECT 
    video_id, title, description, category_id, is_live_content, channel_id, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP
FROM silver.videos
WHERE _batch_id = '{batch_id}'
ON CONFLICT (video_id) 
DO UPDATE SET 
    title = EXCLUDED.title,
    description = EXCLUDED.description,
    category_id = EXCLUDED.category_id,
    is_live_content = EXCLUDED.is_live_content,
    channel_id = EXCLUDED.channel_id,
    _updated_at = CURRENT_TIMESTAMP;


-- ==========================================
-- 3. FACT TABLE: VIDEO STATISTICS
-- ==========================================

-- Clean up any existing fact rows for the current batch to support idempotency
DELETE FROM gold.fact_video_stats WHERE _batch_id = '{batch_id}';

-- Load new stats into Fact table, joining dim_videos and dim_channels (SCD Type 2 temporal join)
INSERT INTO gold.fact_video_stats (
    video_sk, channel_sk, views_count, likes_count, dislikes_count, comments_count, 
    engagement_rate, virtual_revenue, grab_date, _batch_id
)
SELECT 
    COALESCE(dv.video_sk, -1) as video_sk,
    COALESCE(dc.channel_sk, -1) as channel_sk,
    sv.views_count,
    sv.likes_count,
    sv.dislikes_count,
    COALESCE(comm.comm_count, 0) as comments_count,
    CAST(
        (sv.likes_count + sv.dislikes_count + COALESCE(comm.comm_count, 0))::DECIMAL / NULLIF(sv.views_count, 0) 
        AS DECIMAL(10, 4)
    ) as engagement_rate,
    CAST(sv.views_count * 0.002 AS DECIMAL(15, 2)) as virtual_revenue,
    sv.grab_date,
    sv._batch_id
FROM silver.videos sv
LEFT JOIN gold.dim_videos dv 
    ON sv.video_id = dv.video_id
LEFT JOIN gold.dim_channels dc 
    ON sv.channel_id = dc.channel_id 
    AND sv.grab_date >= dc.effective_start 
    AND sv.grab_date < dc.effective_end
LEFT JOIN (
    -- Count of comments per video
    SELECT video_id, COUNT(*) as comm_count
    FROM silver.comments
    GROUP BY video_id
) comm ON sv.video_id = comm.video_id
WHERE sv._batch_id = '{batch_id}';


-- ==========================================
-- 4. REPORTING MARTS (Aggregations)
-- ==========================================

-- Recalculate Channel Performance Mart
TRUNCATE TABLE gold.mart_channel_performance;

INSERT INTO gold.mart_channel_performance (
    channel_id, channel_name, total_videos, total_views, total_likes, 
    total_dislikes, total_comments, estimated_revenue, average_engagement_rate, updated_at
)
WITH latest_video_stats AS (
    -- Get the most recent snapshot of each video's statistics
    SELECT 
        f.video_sk,
        f.channel_sk,
        f.views_count,
        f.likes_count,
        f.dislikes_count,
        f.comments_count,
        f.virtual_revenue,
        f.engagement_rate,
        ROW_NUMBER() OVER(PARTITION BY f.video_sk ORDER BY f.grab_date DESC) as rn
    FROM gold.fact_video_stats f
),
current_video_stats AS (
    SELECT * FROM latest_video_stats WHERE rn = 1
),
channel_agg AS (
    SELECT 
        dc.channel_id,
        MAX(dc.name) as channel_name,
        COUNT(DISTINCT cvs.video_sk) as total_videos,
        SUM(cvs.views_count) as total_views,
        SUM(cvs.likes_count) as total_likes,
        SUM(cvs.dislikes_count) as total_dislikes,
        SUM(cvs.comments_count) as total_comments,
        SUM(cvs.virtual_revenue) as estimated_revenue,
        AVG(cvs.engagement_rate) as average_engagement_rate
    FROM current_video_stats cvs
    JOIN gold.dim_channels dc ON cvs.channel_sk = dc.channel_sk
    GROUP BY dc.channel_id
)
SELECT 
    channel_id, channel_name, total_videos, total_views, total_likes, 
    total_dislikes, total_comments, estimated_revenue, average_engagement_rate, CURRENT_TIMESTAMP
FROM channel_agg;


-- Recalculate Category Insights Mart
TRUNCATE TABLE gold.mart_category_insights;

INSERT INTO gold.mart_category_insights (
    category_id, total_videos, total_views, total_likes, average_length, top_video_title, updated_at
)
WITH latest_video_stats AS (
    SELECT 
        f.video_sk,
        f.views_count,
        f.likes_count,
        ROW_NUMBER() OVER(PARTITION BY f.video_sk ORDER BY f.grab_date DESC) as rn
    FROM gold.fact_video_stats f
),
current_video_stats AS (
    SELECT * FROM latest_video_stats WHERE rn = 1
),
video_ranking AS (
    -- Rank videos by views within their category to find the top video
    SELECT 
        COALESCE(dv.category_id, 'UNKNOWN') as category_id,
        dv.title as video_title,
        cvs.views_count,
        ROW_NUMBER() OVER(PARTITION BY COALESCE(dv.category_id, 'UNKNOWN') ORDER BY cvs.views_count DESC) as cat_rn
    FROM current_video_stats cvs
    JOIN gold.dim_videos dv ON cvs.video_sk = dv.video_sk
),
top_video AS (
    SELECT category_id, video_title FROM video_ranking WHERE cat_rn = 1
),
category_agg AS (
    SELECT 
        COALESCE(dv.category_id, 'UNKNOWN') as category_id,
        COUNT(DISTINCT cvs.video_sk) as total_videos,
        SUM(cvs.views_count) as total_views,
        SUM(cvs.likes_count) as total_likes,
        AVG(sv.length) as average_length
    FROM current_video_stats cvs
    JOIN gold.dim_videos dv ON cvs.video_sk = dv.video_sk
    JOIN silver.videos sv ON dv.video_id = sv.video_id
    GROUP BY COALESCE(dv.category_id, 'UNKNOWN')
)
SELECT 
    ca.category_id,
    ca.total_videos,
    ca.total_views,
    ca.total_likes,
    ca.average_length,
    COALESCE(tv.video_title, 'N/A') as top_video_title,
    CURRENT_TIMESTAMP
FROM category_agg ca
LEFT JOIN top_video tv ON ca.category_id = tv.category_id;


-- Recalculate KPI Summary Mart (single row)
TRUNCATE TABLE gold.mart_kpi_summary;

INSERT INTO gold.mart_kpi_summary (
    kpi_id, total_channels, total_videos, total_views, total_likes, total_comments, total_virtual_revenue, overall_engagement_rate, updated_at
)
WITH latest_video_stats AS (
    SELECT 
        f.video_sk,
        f.views_count,
        f.likes_count,
        f.dislikes_count,
        f.comments_count,
        f.virtual_revenue,
        ROW_NUMBER() OVER(PARTITION BY f.video_sk ORDER BY f.grab_date DESC) as rn
    FROM gold.fact_video_stats f
),
current_video_stats AS (
    SELECT * FROM latest_video_stats WHERE rn = 1
)
SELECT 
    1 as kpi_id,
    (SELECT COUNT(DISTINCT channel_id) FROM gold.dim_channels WHERE channel_id != 'UNKNOWN') as total_channels,
    (SELECT COUNT(DISTINCT video_id) FROM gold.dim_videos WHERE video_id != 'UNKNOWN') as total_videos,
    SUM(views_count) as total_views,
    SUM(likes_count) as total_likes,
    SUM(comments_count) as total_comments,
    SUM(virtual_revenue) as total_virtual_revenue,
    CAST(
        SUM(likes_count + dislikes_count + comments_count)::DECIMAL / NULLIF(SUM(views_count), 0)
        AS DECIMAL(10, 4)
    ) as overall_engagement_rate,
    CURRENT_TIMESTAMP
FROM current_video_stats;
