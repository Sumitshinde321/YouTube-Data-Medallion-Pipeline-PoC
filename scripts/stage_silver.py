import os
import sys
# Add parent directory to sys.path to allow running from any cwd
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import uuid
import pandas as pd
import numpy as np
from datetime import datetime
from io import StringIO
from scripts.config import CHUNK_SIZE
from scripts.database_utils import (
    get_connection, get_engine, log_dq_rule,
    log_rejected_records_batch
)


def load_bronze_chunks(table_name, batch_id, chunk_size=CHUNK_SIZE):
    """
    Yield rows from a Bronze table for the current batch in chunks, using a
    SERVER-SIDE (named) psycopg2 cursor.

    This is the key difference from a plain pandas.read_sql call: a named
    cursor tells Postgres to keep the result set server-side and stream it
    back `chunk_size` rows at a time, instead of pulling every matching row
    into client memory before pandas ever sees it. That's what actually
    caps memory usage on large Bronze tables.
    """
    conn = get_connection()
    cursor_name = f"silver_stream_{table_name.replace('.', '_')}_{uuid.uuid4().hex[:8]}"
    cursor = conn.cursor(name=cursor_name)
    cursor.itersize = chunk_size
    try:
        cursor.execute(f'SELECT * FROM {table_name} WHERE _batch_id = %s', (batch_id,))
        colnames = [desc[0] for desc in cursor.description]
        while True:
            rows = cursor.fetchmany(chunk_size)
            if not rows:
                break
            yield pd.DataFrame(rows, columns=colnames)
    finally:
        cursor.close()
        conn.close()


def upsert_to_silver(df, target_table, pkey_col, update_cols):
    """
    Performs a fast upsert from a Pandas DataFrame to a Silver table in PostgreSQL
    using a temporary staging table and INSERT ON CONFLICT.

    Called once per chunk, so each call only ever holds one chunk's worth
    of rows in the temp table rather than the whole batch.
    """
    if df.empty:
        return

    conn = get_connection()
    cursor = conn.cursor()

    temp_table = f"temp_{target_table.split('.')[-1]}_{uuid.uuid4().hex[:8]}"

    try:
        cursor.execute(f"DROP TABLE IF EXISTS {temp_table};")
        cursor.execute(f"CREATE TEMP TABLE {temp_table} (LIKE {target_table} INCLUDING DEFAULTS);")

        buffer = StringIO()
        df.to_csv(buffer, index=False, header=False, sep='|', na_rep='\\N')
        buffer.seek(0)

        columns = [f'"{col}"' for col in df.columns]
        columns_str = ", ".join(columns)

        copy_query = f"COPY {temp_table} ({columns_str}) FROM STDIN WITH CSV DELIMITER '|' NULL '\\N'"
        cursor.copy_expert(copy_query, buffer)

        set_clause = ", ".join([f"{col} = EXCLUDED.{col}" for col in update_cols])
        upsert_query = f"""
            INSERT INTO {target_table} ({columns_str})
            SELECT {columns_str} FROM {temp_table}
            ON CONFLICT ({pkey_col})
            DO UPDATE SET {set_clause};
        """

        cursor.execute(upsert_query)
        cursor.execute(f"DROP TABLE IF EXISTS {temp_table};")
        conn.commit()
    except Exception as e:
        conn.rollback()
        print(f"Upsert failed for {target_table}: {e}")
        raise e
    finally:
        cursor.close()
        conn.close()


def parse_youtube_metric(val):
    """
    Parses YouTube metrics containing shorthand suffixes (K for thousands, M for millions, B for billions)
    and returns a standard clean integer. Returns None if parsing fails.
    """
    if pd.isna(val) or val == '':
        return 0
    val_str = str(val).strip().upper()
    if not val_str:
        return 0
    try:
        multiplier = 1
        if val_str.endswith('K'):
            multiplier = 1000
            val_str = val_str[:-1].strip()
        elif val_str.endswith('M'):
            multiplier = 1000000
            val_str = val_str[:-1].strip()
        elif val_str.endswith('B'):
            multiplier = 1000000000
            val_str = val_str[:-1].strip()

        val_str = val_str.replace(',', '')
        return int(float(val_str) * multiplier)
    except Exception:
        return None


def _append_csv(path, df, first_write):
    """Append a chunk to a CSV file, writing the header only on the first write."""
    if df.empty:
        return
    os.makedirs(os.path.dirname(path), exist_ok=True)
    df.to_csv(path, mode='a', index=False, header=first_write, encoding='utf-8')


def validate_and_stage_channels(batch_id, chunk_size=CHUNK_SIZE):
    """Process bronze.channels_raw to silver.channels, chunk by chunk."""
    print("Validating and Staging Channels (chunked)...")

    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    silver_csv = os.path.join(base_dir, "silver", "channels", batch_id, "channels.csv")
    rejected_csv = os.path.join(base_dir, "metadata", "rejected", batch_id, "channels_rejected.csv")

    total_raw = total_valid = total_rejected = 0
    null_id_total = dup_id_total = 0
    seen_ids = set()  # tracks IDs across chunks to catch cross-chunk duplicates
    first_valid_write = True
    first_reject_write = True

    for df_raw in load_bronze_chunks("bronze.channels_raw", batch_id, chunk_size):
        total_raw += len(df_raw)
        df_raw['_reject_reason'] = ""

        # 1. Null ID Check
        null_id_mask = df_raw['Id'].isna() | (df_raw['Id'] == '')
        df_raw.loc[null_id_mask, '_reject_reason'] += "Null channel ID; "
        null_id_total += int(null_id_mask.sum())

        # 2. Duplicate ID Check -- within this chunk AND against IDs seen in prior chunks
        dup_in_chunk = df_raw.duplicated(subset=['Id'], keep='first') & ~null_id_mask
        dup_vs_prior = df_raw['Id'].isin(seen_ids) & ~null_id_mask
        dup_id_mask = dup_in_chunk | dup_vs_prior
        df_raw.loc[dup_id_mask, '_reject_reason'] += "Duplicate channel ID in batch; "
        dup_id_total += int(dup_id_mask.sum())

        # Update the running ID set with this chunk's non-null IDs
        seen_ids.update(df_raw.loc[~null_id_mask, 'Id'].tolist())

        invalid_mask = df_raw['_reject_reason'] != ""
        df_valid = df_raw[~invalid_mask].copy()
        df_invalid = df_raw[invalid_mask].copy()

        df_valid = df_valid.rename(columns={
            'Id': 'channel_id', 'Name': 'name', 'Keywords': 'keywords',
            'Description': 'description', 'Tags': 'tags', 'UserId': 'user_id'
        })

        for col in ['channel_id', 'name', 'keywords', 'tags', 'user_id']:
            df_valid[col] = df_valid[col].astype(str).str.strip()
            df_valid[col] = df_valid[col].replace({'nan': None, '': None})

        df_valid['channel_id'] = df_valid['channel_id'].str[:255]
        df_valid['name'] = df_valid['name'].str[:255]
        df_valid['user_id'] = df_valid['user_id'].str[:255]
        df_valid['description'] = df_valid['description'].astype(str).str.strip().replace({'nan': None, '': None})

        df_valid['_cleansed_at'] = datetime.now()
        df_valid['_batch_id'] = batch_id

        valid_cols = ['channel_id', 'name', 'keywords', 'description', 'tags', 'user_id', '_cleansed_at', '_batch_id']
        df_valid = df_valid[valid_cols]

        log_rejected_records_batch(batch_id, "silver.channels", df_invalid, "_reject_reason")

        _append_csv(silver_csv, df_valid, first_valid_write)
        first_valid_write = False
        if not df_invalid.empty:
            _append_csv(rejected_csv, df_invalid, first_reject_write)
            first_reject_write = False

        update_cols = ['name', 'keywords', 'description', 'tags', 'user_id', '_cleansed_at', '_batch_id']
        upsert_to_silver(df_valid, "silver.channels", "channel_id", update_cols)

        total_valid += len(df_valid)
        total_rejected += len(df_invalid)

    log_dq_rule(batch_id, "silver.channels", "channel_id", "null_check",
                "failed" if null_id_total else "passed", null_id_total)
    log_dq_rule(batch_id, "silver.channels", "channel_id", "duplicate_check",
                "failed" if dup_id_total else "passed", dup_id_total)

    print(f"Channels Staged -> Valid (inserted/updated): {total_valid} | Rejected: {total_rejected}")
    return total_raw, total_valid, total_rejected


def validate_and_stage_videos(batch_id, chunk_size=CHUNK_SIZE):
    """Process bronze.videos_raw to silver.videos, chunk by chunk."""
    print("Validating and Staging Videos (chunked)...")

    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    silver_csv = os.path.join(base_dir, "silver", "videos", batch_id, "videos.csv")
    rejected_csv = os.path.join(base_dir, "metadata", "rejected", batch_id, "videos_rejected.csv")

    total_raw = total_valid = total_rejected = 0
    counters = {
        'null_id': 0, 'null_channel': 0, 'dup_id': 0,
        'bad_length': 0, 'bad_views': 0, 'bad_date': 0
    }
    seen_ids = set()
    first_valid_write = True
    first_reject_write = True

    for df_raw in load_bronze_chunks("bronze.videos_raw", batch_id, chunk_size):
        total_raw += len(df_raw)
        df_raw['_reject_reason'] = ""

        null_id_mask = df_raw['Id'].isna() | (df_raw['Id'] == '')
        df_raw.loc[null_id_mask, '_reject_reason'] += "Null video ID; "
        counters['null_id'] += int(null_id_mask.sum())

        null_ch_mask = df_raw['ChannelId'].isna() | (df_raw['ChannelId'] == '')
        df_raw.loc[null_ch_mask, '_reject_reason'] += "Null Channel ID; "
        counters['null_channel'] += int(null_ch_mask.sum())

        dup_in_chunk = df_raw.duplicated(subset=['Id'], keep='first') & ~null_id_mask
        dup_vs_prior = df_raw['Id'].isin(seen_ids) & ~null_id_mask
        dup_id_mask = dup_in_chunk | dup_vs_prior
        df_raw.loc[dup_id_mask, '_reject_reason'] += "Duplicate video ID in batch; "
        counters['dup_id'] += int(dup_id_mask.sum())
        seen_ids.update(df_raw.loc[~null_id_mask, 'Id'].tolist())

        lengths_numeric = pd.to_numeric(df_raw['Length'], errors='coerce')
        bad_length_mask = lengths_numeric.isna() & ~df_raw['Length'].isna() & (df_raw['Length'] != '')
        counters['bad_length'] += int(bad_length_mask.sum())

        views_parsed = df_raw['ViewsCount'].apply(parse_youtube_metric)
        bad_views_mask = views_parsed.isna() & ~df_raw['ViewsCount'].isna() & (df_raw['ViewsCount'] != '')
        counters['bad_views'] += int(bad_views_mask.sum())

        likes_parsed = df_raw['LikesCount'].apply(parse_youtube_metric)
        bad_likes_mask = likes_parsed.isna() & ~df_raw['LikesCount'].isna() & (df_raw['LikesCount'] != '')

        dislikes_parsed = df_raw['DislikesCount'].apply(parse_youtube_metric)
        bad_dislikes_mask = dislikes_parsed.isna() & ~df_raw['DislikesCount'].isna() & (df_raw['DislikesCount'] != '')

        dates_parsed = pd.to_datetime(df_raw['GrabDate'], errors='coerce')
        bad_date_mask = dates_parsed.isna() & ~df_raw['GrabDate'].isna() & (df_raw['GrabDate'] != '')
        counters['bad_date'] += int(bad_date_mask.sum())

        neg_views_mask = (views_parsed < 0) & ~views_parsed.isna()
        df_raw.loc[neg_views_mask, '_reject_reason'] += "ViewsCount cannot be negative; "

        neg_likes_mask = (likes_parsed < 0) & ~likes_parsed.isna()
        df_raw.loc[neg_likes_mask, '_reject_reason'] += "LikesCount cannot be negative; "

        neg_dislikes_mask = (dislikes_parsed < 0) & ~dislikes_parsed.isna()
        df_raw.loc[neg_dislikes_mask, '_reject_reason'] += "DislikesCount cannot be negative; "

        neg_length_mask = (lengths_numeric < 0) & ~lengths_numeric.isna()
        df_raw.loc[neg_length_mask, '_reject_reason'] += "Length cannot be negative; "

        type_fail_mask = bad_length_mask | bad_views_mask | bad_likes_mask | bad_dislikes_mask | bad_date_mask
        df_raw.loc[type_fail_mask, '_reject_reason'] += "Data type casting failure; "

        invalid_mask = df_raw['_reject_reason'] != ""
        df_valid = df_raw[~invalid_mask].copy()
        df_invalid = df_raw[invalid_mask].copy()

        df_valid = df_valid.rename(columns={
            'Id': 'video_id', 'Title': 'title', 'Description': 'description',
            'Keywords': 'keywords', 'CategoryId': 'category_id',
            'UserId': 'user_id', 'ChannelId': 'channel_id'
        })

        df_valid['length'] = pd.to_numeric(df_valid['Length'], errors='coerce').fillna(0.0)
        df_valid['views_count'] = df_valid['ViewsCount'].apply(parse_youtube_metric).fillna(0).astype(int)
        df_valid['likes_count'] = df_valid['LikesCount'].apply(parse_youtube_metric).fillna(0).astype(int)
        df_valid['dislikes_count'] = df_valid['DislikesCount'].apply(parse_youtube_metric).fillna(0).astype(int)

        df_valid['is_live_content'] = df_valid['IsLiveContent'].astype(str).str.lower().str.strip() == 'true'
        df_valid['grab_date'] = pd.to_datetime(df_valid['GrabDate'], errors='coerce')

        for col in ['video_id', 'title', 'keywords', 'category_id', 'user_id', 'channel_id']:
            df_valid[col] = df_valid[col].astype(str).str.strip()
            df_valid[col] = df_valid[col].replace({'nan': None, '': None})

        df_valid['video_id'] = df_valid['video_id'].str[:255]
        df_valid['title'] = df_valid['title'].str[:255]
        df_valid['category_id'] = df_valid['category_id'].str[:255]
        df_valid['user_id'] = df_valid['user_id'].str[:255]
        df_valid['channel_id'] = df_valid['channel_id'].str[:255]

        df_valid['description'] = df_valid['description'].astype(str).str.strip().replace({'nan': None, '': None})

        df_valid['_cleansed_at'] = datetime.now()
        df_valid['_batch_id'] = batch_id

        valid_cols = [
            'video_id', 'title', 'description', 'length', 'views_count', 'keywords',
            'likes_count', 'category_id', 'dislikes_count', 'user_id', 'is_live_content',
            'channel_id', 'grab_date', '_cleansed_at', '_batch_id'
        ]
        df_valid = df_valid[valid_cols]

        log_rejected_records_batch(batch_id, "silver.videos", df_invalid, "_reject_reason")

        _append_csv(silver_csv, df_valid, first_valid_write)
        first_valid_write = False
        if not df_invalid.empty:
            _append_csv(rejected_csv, df_invalid, first_reject_write)
            first_reject_write = False

        update_cols = [
            'title', 'description', 'length', 'views_count', 'keywords', 'likes_count',
            'category_id', 'dislikes_count', 'user_id', 'is_live_content', 'channel_id',
            'grab_date', '_cleansed_at', '_batch_id'
        ]
        upsert_to_silver(df_valid, "silver.videos", "video_id", update_cols)

        total_valid += len(df_valid)
        total_rejected += len(df_invalid)

    log_dq_rule(batch_id, "silver.videos", "video_id", "null_check_id",
                "failed" if counters['null_id'] else "passed", counters['null_id'])
    log_dq_rule(batch_id, "silver.videos", "channel_id", "null_check_channel",
                "failed" if counters['null_channel'] else "passed", counters['null_channel'])
    log_dq_rule(batch_id, "silver.videos", "video_id", "duplicate_check",
                "failed" if counters['dup_id'] else "passed", counters['dup_id'])
    log_dq_rule(batch_id, "silver.videos", "length", "type_check",
                "failed" if counters['bad_length'] else "passed", counters['bad_length'])
    log_dq_rule(batch_id, "silver.videos", "views_count", "type_check",
                "failed" if counters['bad_views'] else "passed", counters['bad_views'])
    log_dq_rule(batch_id, "silver.videos", "grab_date", "type_check",
                "failed" if counters['bad_date'] else "passed", counters['bad_date'])

    print(f"Videos Staged -> Valid (inserted/updated): {total_valid} | Rejected: {total_rejected}")
    return total_raw, total_valid, total_rejected


def validate_and_stage_comments(batch_id, chunk_size=CHUNK_SIZE):
    """Process bronze.comments_raw to silver.comments, chunk by chunk."""
    print("Validating and Staging Comments (chunked)...")

    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    silver_csv = os.path.join(base_dir, "silver", "comments", batch_id, "comments.csv")
    rejected_csv = os.path.join(base_dir, "metadata", "rejected", batch_id, "comments_rejected.csv")

    total_raw = total_valid = total_rejected = 0
    counters = {'null_id': 0, 'null_video': 0, 'dup_id': 0, 'bad_likes': 0, 'bad_date': 0}
    seen_ids = set()
    first_valid_write = True
    first_reject_write = True

    for df_raw in load_bronze_chunks("bronze.comments_raw", batch_id, chunk_size):
        total_raw += len(df_raw)
        df_raw['_reject_reason'] = ""

        null_id_mask = df_raw['Id'].isna() | (df_raw['Id'] == '')
        df_raw.loc[null_id_mask, '_reject_reason'] += "Null comment ID; "
        counters['null_id'] += int(null_id_mask.sum())

        null_vid_mask = df_raw['VideoId'].isna() | (df_raw['VideoId'] == '')
        df_raw.loc[null_vid_mask, '_reject_reason'] += "Null Video ID; "
        counters['null_video'] += int(null_vid_mask.sum())

        dup_in_chunk = df_raw.duplicated(subset=['Id'], keep='first') & ~null_id_mask
        dup_vs_prior = df_raw['Id'].isin(seen_ids) & ~null_id_mask
        dup_id_mask = dup_in_chunk | dup_vs_prior
        df_raw.loc[dup_id_mask, '_reject_reason'] += "Duplicate comment ID in batch; "
        counters['dup_id'] += int(dup_id_mask.sum())
        seen_ids.update(df_raw.loc[~null_id_mask, 'Id'].tolist())

        likes_parsed = df_raw['LikeCount'].apply(parse_youtube_metric)
        bad_likes_mask = likes_parsed.isna() & ~df_raw['LikeCount'].isna() & (df_raw['LikeCount'] != '')
        counters['bad_likes'] += int(bad_likes_mask.sum())

        dates_parsed = pd.to_datetime(df_raw['GrabDate'], errors='coerce')
        bad_date_mask = dates_parsed.isna() & ~df_raw['GrabDate'].isna() & (df_raw['GrabDate'] != '')
        counters['bad_date'] += int(bad_date_mask.sum())

        neg_likes_mask = (likes_parsed < 0) & ~likes_parsed.isna()
        df_raw.loc[neg_likes_mask, '_reject_reason'] += "LikeCount cannot be negative; "

        type_fail_mask = bad_likes_mask | bad_date_mask
        df_raw.loc[type_fail_mask, '_reject_reason'] += "Data type casting failure; "

        invalid_mask = df_raw['_reject_reason'] != ""
        df_valid = df_raw[~invalid_mask].copy()
        df_invalid = df_raw[invalid_mask].copy()

        df_valid = df_valid.rename(columns={
            'Id': 'comment_id', 'Text': 'text', 'AuthorName': 'author_name',
            'AuthorChannelId': 'author_channel_id', 'PublishedTime': 'published_time',
            'VideoId': 'video_id'
        })

        df_valid['like_count'] = df_valid['LikeCount'].apply(parse_youtube_metric).fillna(0).astype(int)
        df_valid['grab_date'] = pd.to_datetime(df_valid['GrabDate'], errors='coerce')

        for col in ['comment_id', 'author_name', 'author_channel_id', 'published_time', 'video_id']:
            df_valid[col] = df_valid[col].astype(str).str.strip()
            df_valid[col] = df_valid[col].replace({'nan': None, '': None})

        df_valid['comment_id'] = df_valid['comment_id'].str[:255]
        df_valid['author_name'] = df_valid['author_name'].str[:255]
        df_valid['author_channel_id'] = df_valid['author_channel_id'].str[:255]
        df_valid['video_id'] = df_valid['video_id'].str[:255]

        df_valid['text'] = df_valid['text'].astype(str).str.strip().replace({'nan': None, '': None})

        df_valid['_cleansed_at'] = datetime.now()
        df_valid['_batch_id'] = batch_id

        valid_cols = [
            'comment_id', 'text', 'author_name', 'author_channel_id', 'published_time',
            'like_count', 'video_id', 'grab_date', '_cleansed_at', '_batch_id'
        ]
        df_valid = df_valid[valid_cols]

        log_rejected_records_batch(batch_id, "silver.comments", df_invalid, "_reject_reason")

        _append_csv(silver_csv, df_valid, first_valid_write)
        first_valid_write = False
        if not df_invalid.empty:
            _append_csv(rejected_csv, df_invalid, first_reject_write)
            first_reject_write = False

        update_cols = [
            'text', 'author_name', 'author_channel_id', 'published_time',
            'like_count', 'video_id', 'grab_date', '_cleansed_at', '_batch_id'
        ]
        upsert_to_silver(df_valid, "silver.comments", "comment_id", update_cols)

        total_valid += len(df_valid)
        total_rejected += len(df_invalid)

    log_dq_rule(batch_id, "silver.comments", "comment_id", "null_check_id",
                "failed" if counters['null_id'] else "passed", counters['null_id'])
    log_dq_rule(batch_id, "silver.comments", "video_id", "null_check_video",
                "failed" if counters['null_video'] else "passed", counters['null_video'])
    log_dq_rule(batch_id, "silver.comments", "comment_id", "duplicate_check",
                "failed" if counters['dup_id'] else "passed", counters['dup_id'])
    log_dq_rule(batch_id, "silver.comments", "like_count", "type_check",
                "failed" if counters['bad_likes'] else "passed", counters['bad_likes'])
    log_dq_rule(batch_id, "silver.comments", "grab_date", "type_check",
                "failed" if counters['bad_date'] else "passed", counters['bad_date'])

    print(f"Comments Staged -> Valid (inserted/updated): {total_valid} | Rejected: {total_rejected}")
    return total_raw, total_valid, total_rejected


def run_silver_stage(batch_id):
    """
    Orchestrates the Silver staging stage.
    """
    print("\n" + "="*40)
    print(f"RUNNING SILVER LAYER STAGE - BATCH ID: {batch_id}")
    print("="*40)

    ch_raw, ch_valid, ch_rej = validate_and_stage_channels(batch_id)
    v_raw, v_valid, v_rej = validate_and_stage_videos(batch_id)
    c_raw, c_valid, c_rej = validate_and_stage_comments(batch_id)

    total_raw = ch_raw + v_raw + c_raw
    total_valid = ch_valid + v_valid + c_valid
    total_rej = ch_rej + v_rej + c_rej

    print("\n" + "="*40)
    print("SILVER STAGE SUMMARY")
    print(f"Total Raw Records Read: {total_raw}")
    print(f"Total Valid (Staged):    {total_valid}")
    print(f"Total Rejected:         {total_rej}")
    print("="*40)

    return total_raw, total_valid, total_rej


if __name__ == "__main__":
    import uuid
    run_id = sys.argv[1] if len(sys.argv) > 1 else f"local_run_{uuid.uuid4().hex[:8]}"
    run_silver_stage(run_id)
