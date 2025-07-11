# Default Kafka producer configuration
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

KAFKA_BOOTSTRAP_SERVER = "localhost:29092"
LIMIT_MESSAGES = 20
