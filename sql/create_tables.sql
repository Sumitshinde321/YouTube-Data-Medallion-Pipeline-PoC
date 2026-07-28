-- ==========================================
-- 1. BRONZE LAYER (Immutable Raw Ingestion)
-- ==========================================

CREATE TABLE IF NOT EXISTS bronze.channels_raw (
    "Id" TEXT,
    "Name" TEXT,
    "Keywords" TEXT,
    "Description" TEXT,
    "Tags" TEXT,
    "UserId" TEXT,
    -- Metadata columns
    _loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    _source_file TEXT,
    _batch_id TEXT
);

CREATE TABLE IF NOT EXISTS bronze.videos_raw (
    "Id" TEXT,
    "Title" TEXT,
    "Description" TEXT,
    "Length" TEXT,
    "ViewsCount" TEXT,
    "Keywords" TEXT,
    "LikesCount" TEXT,
    "CategoryId" TEXT,
    "DislikesCount" TEXT,
    "UserId" TEXT,
    "IsLiveContent" TEXT,
    "ChannelId" TEXT,
    "GrabDate" TEXT,
    -- Metadata columns
    _loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    _source_file TEXT,
    _batch_id TEXT
);

CREATE TABLE IF NOT EXISTS bronze.comments_raw (
    "Id" TEXT,
    "Text" TEXT,
    "AuthorName" TEXT,
    "AuthorChannelId" TEXT,
    "PublishedTime" TEXT,
    "LikeCount" TEXT,
    "VideoId" TEXT,
    "GrabDate" TEXT,
    -- Metadata columns
    _loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    _source_file TEXT,
    _batch_id TEXT
);


-- ==========================================
-- 2. SILVER LAYER (Cleansed & Standardized)
-- ==========================================

CREATE TABLE IF NOT EXISTS silver.channels (
    channel_id VARCHAR(255) PRIMARY KEY,
    name VARCHAR(255),
    keywords TEXT,
    description TEXT,
    tags TEXT,
    user_id VARCHAR(255),
    _cleansed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    _batch_id VARCHAR(255)
);

CREATE TABLE IF NOT EXISTS silver.videos (
    video_id VARCHAR(255) PRIMARY KEY,
    title VARCHAR(255),
    description TEXT,
    length DECIMAL(12, 2),
    views_count BIGINT,
    keywords TEXT,
    likes_count BIGINT,
    category_id VARCHAR(255),
    dislikes_count BIGINT,
    user_id VARCHAR(255),
    is_live_content BOOLEAN,
    channel_id VARCHAR(255),
    grab_date TIMESTAMP,
    _cleansed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    _batch_id VARCHAR(255)
);

CREATE TABLE IF NOT EXISTS silver.comments (
    comment_id VARCHAR(255) PRIMARY KEY,
    text TEXT,
    author_name VARCHAR(255),
    author_channel_id VARCHAR(255),
    published_time VARCHAR(255),
    like_count BIGINT,
    video_id VARCHAR(255),
    grab_date TIMESTAMP,
    _cleansed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    _batch_id VARCHAR(255)
);


-- ==========================================
-- 3. METADATA & AUDIT LAYER
-- ==========================================

CREATE TABLE IF NOT EXISTS metadata.pipeline_runs (
    run_id VARCHAR(255) PRIMARY KEY,
    pipeline_name VARCHAR(255) NOT NULL,
    batch_type VARCHAR(50) NOT NULL, -- 'initial' or 'incremental'
    start_time TIMESTAMP NOT NULL,
    end_time TIMESTAMP,
    status VARCHAR(50) NOT NULL, -- 'running', 'success', 'failed'
    duration_seconds DECIMAL(10, 2),
    records_processed BIGINT DEFAULT 0,
    records_inserted BIGINT DEFAULT 0,
    records_updated BIGINT DEFAULT 0,
    records_rejected BIGINT DEFAULT 0
);

CREATE TABLE IF NOT EXISTS metadata.dq_rules_run (
    rule_run_id SERIAL PRIMARY KEY,
    run_id VARCHAR(255) REFERENCES metadata.pipeline_runs(run_id) ON DELETE CASCADE,
    table_name VARCHAR(255) NOT NULL,
    column_name VARCHAR(255),
    rule_type VARCHAR(255) NOT NULL, -- 'null_check', 'primary_key_check', 'type_check', etc.
    status VARCHAR(50) NOT NULL, -- 'passed', 'failed'
    failed_records_count BIGINT DEFAULT 0
);

CREATE TABLE IF NOT EXISTS metadata.rejected_records (
    rejected_id SERIAL PRIMARY KEY,
    run_id VARCHAR(255) REFERENCES metadata.pipeline_runs(run_id) ON DELETE CASCADE,
    source_table VARCHAR(255) NOT NULL,
    raw_row_data TEXT NOT NULL,
    failure_reason TEXT NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS metadata.error_logs (
    log_id SERIAL PRIMARY KEY,
    run_id VARCHAR(255) REFERENCES metadata.pipeline_runs(run_id) ON DELETE CASCADE,
    error_message TEXT NOT NULL,
    stack_trace TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);


-- ==========================================
-- 4. GOLD LAYER (Star Schema Dimensions & Facts)
-- ==========================================

-- SCD Type 2 Dimension Table (Channels)
CREATE TABLE IF NOT EXISTS gold.dim_channels (
    channel_sk SERIAL PRIMARY KEY,
    channel_id VARCHAR(255) NOT NULL,
    name VARCHAR(255),
    keywords TEXT,
    tags TEXT,
    effective_start TIMESTAMP NOT NULL,
    effective_end TIMESTAMP NOT NULL,
    is_current BOOLEAN NOT NULL,
    version INT NOT NULL,
    _created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    _updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Index for fast lookup on dimension queries
CREATE INDEX IF NOT EXISTS idx_dim_channels_lookup ON gold.dim_channels (channel_id, is_current);

-- SCD Type 1 Dimension Table (Videos - Overwrites history)
CREATE TABLE IF NOT EXISTS gold.dim_videos (
    video_sk SERIAL PRIMARY KEY,
    video_id VARCHAR(255) UNIQUE NOT NULL,
    title VARCHAR(255),
    description TEXT,
    category_id VARCHAR(255),
    is_live_content BOOLEAN,
    channel_id VARCHAR(255),
    _created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    _updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Fact Table: Video Statistics
CREATE TABLE IF NOT EXISTS gold.fact_video_stats (
    fact_key SERIAL PRIMARY KEY,
    video_sk INT NOT NULL REFERENCES gold.dim_videos(video_sk) ON DELETE CASCADE,
    channel_sk INT NOT NULL REFERENCES gold.dim_channels(channel_sk) ON DELETE CASCADE,
    views_count BIGINT,
    likes_count BIGINT,
    dislikes_count BIGINT,
    comments_count BIGINT,
    engagement_rate DECIMAL(10, 4), -- (likes+dislikes+comments) / views
    virtual_revenue DECIMAL(15, 2), -- views * CPM ($2.00 / 1000 views)
    grab_date TIMESTAMP,
    _batch_id VARCHAR(255)
);

-- Index on Fact keys for aggregations
CREATE INDEX IF NOT EXISTS idx_fact_video_stats_sk ON gold.fact_video_stats(video_sk, channel_sk);


-- ==========================================
-- 5. REPORTING MARTS (Aggregations)
-- ==========================================

CREATE TABLE IF NOT EXISTS gold.mart_channel_performance (
    channel_id VARCHAR(255) PRIMARY KEY,
    channel_name VARCHAR(255),
    total_videos INT,
    total_views BIGINT,
    total_likes BIGINT,
    total_dislikes BIGINT,
    total_comments BIGINT,
    estimated_revenue DECIMAL(15, 2),
    average_engagement_rate DECIMAL(10, 4),
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS gold.mart_category_insights (
    category_id VARCHAR(255) PRIMARY KEY,
    total_videos INT,
    total_views BIGINT,
    total_likes BIGINT,
    average_length DECIMAL(12, 2),
    top_video_title VARCHAR(255),
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS gold.mart_kpi_summary (
    kpi_id INT PRIMARY KEY DEFAULT 1,
    total_channels BIGINT,
    total_videos BIGINT,
    total_views BIGINT,
    total_likes BIGINT,
    total_comments BIGINT,
    total_virtual_revenue DECIMAL(15, 2),
    overall_engagement_rate DECIMAL(10, 4),
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT chk_kpi_id CHECK (kpi_id = 1) -- Ensures only one row exists
);

-- Create Unknown/Dummy records in dimensions to handle referential integrity
-- Insert dummy channel if not exists
INSERT INTO gold.dim_channels (channel_sk, channel_id, name, keywords, tags, effective_start, effective_end, is_current, version)
SELECT -1, 'UNKNOWN', 'Unknown Channel', 'UNKNOWN', 'UNKNOWN', '1900-01-01 00:00:00', '9999-12-31 23:59:59', TRUE, 1
ON CONFLICT DO NOTHING;

-- Insert dummy video if not exists
INSERT INTO gold.dim_videos (video_sk, video_id, title, description, category_id, is_live_content, channel_id)
SELECT -1, 'UNKNOWN', 'Unknown Video', 'UNKNOWN', 'UNKNOWN', FALSE, 'UNKNOWN'
ON CONFLICT (video_id) DO NOTHING;
