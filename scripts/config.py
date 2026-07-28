import os
from dotenv import load_dotenv

# Load environment variables from .env file if available
load_dotenv()

DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_NAME = os.getenv("DB_NAME", "youtube_dw")
DB_USER = os.getenv("DB_USER", "airflow")
DB_PASSWORD = os.getenv("DB_PASSWORD", "airflow")

# Database URL for SQLAlchemy
DATABASE_URL = f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

# Source data folders
SOURCE_DATA_DIR = os.getenv("SOURCE_DATA_DIR", os.path.join(os.path.dirname(os.path.dirname(__file__)), "source_data"))

# Raw dataset folder on desktop (used by seed_sources.py)
RAW_DATASET_DIR = os.getenv("RAW_DATASET_DIR", "d:/UserFiles/Desktop/YouTube Dataset")

# ==========================================
# BATCH PROCESSING CONFIG
# ==========================================
# Number of rows read/written/copied per chunk in Bronze and Silver stages.
# Lower this if you're still hitting memory limits; raise it for fewer,
# larger round-trips to Postgres on a machine with more RAM.
CHUNK_SIZE = int(os.getenv("CHUNK_SIZE", "10000"))
