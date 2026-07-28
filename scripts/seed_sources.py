import os
import sys
# Add parent directory to sys.path to allow running from any cwd
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import pandas as pd
import numpy as np
import argparse
from scripts.config import RAW_DATASET_DIR, SOURCE_DATA_DIR

def seed_data(subsample_size=50000, full_run=False):
    """
    Reads the raw YouTube dataset from the Desktop and splits it into
    'initial' and 'incremental' batches inside the workspace 'source_data' folder.
    Also injects SCD updates and invalid rows to test pipeline robustness.
    """
    print("="*60)
    print("STARTING DATA SEEDING AND SPLITTING PROCESS")
    print("="*60)
    
    # Verify raw files exist
    channels_path = os.path.join(RAW_DATASET_DIR, "channels.csv")
    videos_path = os.path.join(RAW_DATASET_DIR, "videos.csv")
    comments_path = os.path.join(RAW_DATASET_DIR, "comments.csv")
    
    for path in [channels_path, videos_path, comments_path]:
        if not os.path.exists(path):
            print(f"Error: Source file {path} not found.")
            sys.exit(1)
            
    # Create target directories
    initial_dir = os.path.join(SOURCE_DATA_DIR, "initial")
    incremental_dir = os.path.join(SOURCE_DATA_DIR, "incremental")
    os.makedirs(initial_dir, exist_ok=True)
    os.makedirs(incremental_dir, exist_ok=True)
    
    print(f"Reading source files from: {RAW_DATASET_DIR}")
    print(f"Writing seeded files to: {SOURCE_DATA_DIR}")

    # ==========================================
    # 1. LOAD CHANNELS
    # ==========================================
    print("\nProcessing Channels...")
    df_channels_raw = pd.read_csv(channels_path, lineterminator='\n')
    df_channels_raw.columns = df_channels_raw.columns.str.strip()
    for col in df_channels_raw.columns:
        df_channels_raw[col] = df_channels_raw[col].astype(str).str.rstrip('\r').replace({'nan': None})
    print(f"Total raw channels in source: {len(df_channels_raw)}")
    
    if full_run:
        df_channels = df_channels_raw
    else:
        # Take a subset of channels
        df_channels = df_channels_raw.head(20000)
    
    # Split Channels: 70% Initial, 30% Incremental
    split_idx_ch = int(len(df_channels) * 0.7)
    df_ch_initial = df_channels.iloc[:split_idx_ch].copy()
    df_ch_incremental = df_channels.iloc[split_idx_ch:].copy()
    
    # Simulate SCD Type 2 Changes in Incremental Batch:
    # Take 5 channels from initial batch, modify their name or keywords, and append to incremental batch.
    print("Simulating SCD Type 2 updates for channels...")
    scd_channels = df_ch_initial.head(5).copy()
    scd_channels['Name'] = scd_channels['Name'] + " - Updated Brand Name"
    scd_channels['Keywords'] = scd_channels['Keywords'].fillna('') + ", updated_tag"
    df_ch_incremental = pd.concat([df_ch_incremental, scd_channels], ignore_index=True)
    
    # Inject 1 Faulty Row (Null ID) in Incremental channels to test DQ
    faulty_ch = pd.DataFrame([{
        'Id': None, 'Name': 'Faulty Channel (No ID)', 'Keywords': 'faulty', 
        'Description': 'This channel has no ID', 'Tags': 'faulty', 'UserId': 'user_123'
    }])
    df_ch_incremental = pd.concat([df_ch_incremental, faulty_ch], ignore_index=True)

    # Save channels
    df_ch_initial.to_csv(os.path.join(initial_dir, "channels.csv"), index=False, encoding='utf-8')
    df_ch_incremental.to_csv(os.path.join(incremental_dir, "channels.csv"), index=False, encoding='utf-8')
    print(f"Seeded Channels -> Initial: {len(df_ch_initial)} rows | Incremental: {len(df_ch_incremental)} rows")

    # Get set of channel IDs to filter videos (for referential integrity / sub-sampling)
    seeded_channel_ids = set(df_channels['Id'].dropna().unique())

    # ==========================================
    # 2. LOAD VIDEOS (with chunking due to size)
    # ==========================================
    print("\nProcessing Videos...")
    video_chunks = []
    chunksize = 100000
    row_count = 0
    max_videos_to_load = subsample_size if not full_run else float('inf')
    
    for chunk in pd.read_csv(videos_path, lineterminator='\n', chunksize=chunksize, low_memory=False):
        chunk.columns = chunk.columns.str.strip()
        for col in chunk.columns:
            chunk[col] = chunk[col].astype(str).str.rstrip('\r').replace({'nan': None})
        # Filter for videos that belong to our seeded channels
        filtered_chunk = chunk[chunk['ChannelId'].isin(seeded_channel_ids)]
        video_chunks.append(filtered_chunk)
        row_count += len(filtered_chunk)
        if row_count >= max_videos_to_load:
            break
            
    df_videos = pd.concat(video_chunks, ignore_index=True)
    if not full_run:
        df_videos = df_videos.head(subsample_size)
    print(f"Total processed videos: {len(df_videos)}")
    
    # Split Videos: 70% Initial, 30% Incremental
    split_idx_v = int(len(df_videos) * 0.7)
    df_v_initial = df_videos.iloc[:split_idx_v].copy()
    df_v_incremental = df_videos.iloc[split_idx_v:].copy()
    
    # Simulate SCD Type 1 updates in Incremental batch:
    # Take 5 videos from initial, change their Title (dim change) and ViewsCount (fact change)
    print("Simulating SCD Type 1 updates and stats changes for videos...")
    scd_videos = df_v_initial.head(5).copy()
    scd_videos['Title'] = scd_videos['Title'] + " (Remastered HD)"
    scd_videos['ViewsCount'] = scd_videos['ViewsCount'].fillna(0).astype(float) + 500000 # Stats update
    df_v_incremental = pd.concat([df_v_incremental, scd_videos], ignore_index=True)
    
    # Inject Faulty Rows in Incremental videos:
    # Row 1: Null ID
    # Row 2: Negative ViewsCount (violates business rule)
    # Row 3: Non-numeric Length (violates data type validation)
    faulty_videos = pd.DataFrame([
        {
            'Id': None, 'Title': 'Faulty Video 1', 'Description': 'Null ID', 'Length': '120.0', 
            'ViewsCount': '100', 'Keywords': 'faulty', 'LikesCount': '10', 'CategoryId': 'Music', 
            'DislikesCount': '0', 'UserId': 'user_1', 'IsLiveContent': 'False', 
            'ChannelId': 'UC__a4BYZxT-uvKGeATnSg8Q', 'GrabDate': '2019-11-19 06:00:00.000'
        },
        {
            'Id': 'faulty_vid_2', 'Title': 'Faulty Video 2', 'Description': 'Negative views', 'Length': '150.5', 
            'ViewsCount': '-1000', 'Keywords': 'faulty', 'LikesCount': '50', 'CategoryId': 'Comedy', 
            'DislikesCount': '10', 'UserId': 'user_2', 'IsLiveContent': 'False', 
            'ChannelId': 'UC__a4BYZxT-uvKGeATnSg8Q', 'GrabDate': '2019-11-19 06:00:00.000'
        },
        {
            'Id': 'faulty_vid_3', 'Title': 'Faulty Video 3', 'Description': 'Invalid length datatype', 'Length': 'not_a_number', 
            'ViewsCount': '2000', 'Keywords': 'faulty', 'LikesCount': '120', 'CategoryId': 'Education', 
            'DislikesCount': '5', 'UserId': 'user_3', 'IsLiveContent': 'False', 
            'ChannelId': 'UC__a4BYZxT-uvKGeATnSg8Q', 'GrabDate': '2019-11-19 06:00:00.000'
        }
    ])
    df_v_incremental = pd.concat([df_v_incremental, faulty_videos], ignore_index=True)

    # Save videos
    df_v_initial.to_csv(os.path.join(initial_dir, "videos.csv"), index=False, encoding='utf-8')
    df_v_incremental.to_csv(os.path.join(incremental_dir, "videos.csv"), index=False, encoding='utf-8')
    print(f"Seeded Videos -> Initial: {len(df_v_initial)} rows | Incremental: {len(df_v_incremental)} rows")

    # Get set of seeded Video IDs to filter comments
    seeded_video_ids = set(df_videos['Id'].dropna().unique())

    # ==========================================
    # 3. LOAD COMMENTS
    # ==========================================
    print("\nProcessing Comments...")
    comment_chunks = []
    row_count_c = 0
    max_comments_to_load = subsample_size if not full_run else float('inf')
    
    for chunk in pd.read_csv(comments_path, lineterminator='\n', chunksize=chunksize, low_memory=False):
        chunk.columns = chunk.columns.str.strip()
        for col in chunk.columns:
            chunk[col] = chunk[col].astype(str).str.rstrip('\r').replace({'nan': None})
        # Filter comments for videos that exist in our seeded set
        filtered_chunk = chunk[chunk['VideoId'].isin(seeded_video_ids)]
        comment_chunks.append(filtered_chunk)
        row_count_c += len(filtered_chunk)
        if row_count_c >= max_comments_to_load:
            break
            
    df_comments = pd.concat(comment_chunks, ignore_index=True)
    if not full_run:
        df_comments = df_comments.head(subsample_size)
    print(f"Total processed comments: {len(df_comments)}")
    
    # Split Comments: 70% Initial, 30% Incremental
    split_idx_c = int(len(df_comments) * 0.7)
    df_c_initial = df_comments.iloc[:split_idx_c].copy()
    df_c_incremental = df_comments.iloc[split_idx_c:].copy()
    
    # Inject 1 Faulty Row (Null ID) in comments to test DQ
    faulty_comment = pd.DataFrame([{
        'Id': None, 'Text': 'Faulty comment with no ID', 'AuthorName': 'Anonymous', 
        'AuthorChannelId': 'UCNbnKFaHnNsw_T9_BNpqgtg', 'PublishedTime': '1 day ago', 
        'LikeCount': '0', 'VideoId': '___EuLzIPr0', 'GrabDate': '2019-11-17 14:00:00.000'
    }])
    df_c_incremental = pd.concat([df_c_incremental, faulty_comment], ignore_index=True)

    # Save comments
    df_c_initial.to_csv(os.path.join(initial_dir, "comments.csv"), index=False, encoding='utf-8')
    df_c_incremental.to_csv(os.path.join(incremental_dir, "comments.csv"), index=False, encoding='utf-8')
    print(f"Seeded Comments -> Initial: {len(df_c_initial)} rows | Incremental: {len(df_c_incremental)} rows")
    
    print("\n" + "="*60)
    print("SEEDING COMPLETED SUCCESSFULLY!")
    print("="*60)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Seed source files for medallion pipeline PoC.")
    parser.add_argument("--full", action="store_true", help="Process the full raw dataset (slow).")
    parser.add_argument("--size", type=int, default=50000, help="Sub-sample size for videos/comments (default 50000).")
    args = parser.parse_args()
    
    seed_data(subsample_size=args.size, full_run=args.full)
