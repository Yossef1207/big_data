DEFAULT_KAFKA_CONFIG = {
    'compression.type': 'gzip',
    'message.timeout.ms': 5000,
    'queue.buffering.max.messages': 100000,
    'queue.buffering.max.ms': 1000,
    'batch.num.messages': 1000,
    'default.topic.config': {
        'acks': 'all'
    }
}

KAFKA_BOOTSTRAP_SERVER = "kafka:9092"  # Docker service name
# for local testing, you can change it to "localhost:29092"

RAW_COM_TOPIC = "raw-reddit-comments"
LIMIT_MESSAGES = 0

DATA_PATH = "/data/RC_2019-04.zst"  # Will be mounted from host

FIELDS_TO_KEEP = [
    "id",
    "author",
    "created_utc",
    "body",
    "score",
    "subreddit",
    "controversiality"
]

MIN_TIMESTAMP = -1 #1554076800
MAX_TIMESTAMP = -1 #1555472130
MIN_BODY_LENGTH = 10
