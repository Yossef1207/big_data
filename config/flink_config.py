

KAFKA_JAR_PATH = "file:///opt/flink/usrlib/flink-sql-connector-kafka-3.3.0-1.20.jar"
# Environment-specific Kafka endpoints
KAFKA_ENDPOINTS = {
    "docker": "kafka:9092",  # Internal Docker network endpoint
    "localhost": "localhost:29092"  # External localhost endpoint for testing
}
