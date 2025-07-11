"""
This file contains configuration variables for the Reddit comment ingestion pipeline.
"""
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent

DATA_PATH = PROJECT_ROOT / "datasets" / "RC_2019-04.zst"

MIN_TIMESTAMP = 1554076800
MAX_TIMESTAMP = 1555472130
MIN_BODY_LENGTH = 10
FIELDS_TO_KEEP = [
    "id",
    "author",
    "created_utc",
    "body",
    "score",
    "subreddit",
    "controversiality"
]

# Kafka configuration
DEBUG = True
TOPIC_1 = "raw-reddit-comments"

