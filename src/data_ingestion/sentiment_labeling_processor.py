"""
Sentiment Labeling Processor - Second Stage
Handles vectorization and sentiment prediction only
"""

import os
import logging
from typing import Optional
import gc
import time

from pyflink.common import SimpleStringSchema, WatermarkStrategy
from pyflink.datastream import StreamExecutionEnvironment, RuntimeExecutionMode, MapFunction
from pyflink.datastream.connectors.kafka import KafkaSource, KafkaOffsetsInitializer
from pyflink.datastream.connectors.kafka import KafkaSink, DeliveryGuarantee
from pyflink.datastream.connectors.kafka import KafkaRecordSerializationSchema
from pyflink.common.typeinfo import Types

from joblib import load
import json

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Configuration constants
SOURCE_TOPIC = "cleaned-reddit-comments"  # Input from cleaning stage
SINK_TOPIC = "labeled-reddit-comments"    # Final output (same as original)
KAFKA_ENDPOINT = "kafka:9092"
RETRAIN_PATH = "/shared/sentiment_model.pkl"

# Global model cache to avoid loading multiple times
MODEL_CACHE = {}


def get_model(model_type: str):
    """Lazy loading of models with caching to reduce memory usage."""
    if model_type not in MODEL_CACHE:
        base_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "/data")

        if model_type == "vectorizer":
            MODEL_CACHE[model_type] = load(os.path.join(base_path, "tfidf_vectorizer.pkl"))
        elif model_type == "sentiment":
            MODEL_CACHE[model_type] = load(os.path.join(base_path, "sentiment_model.pkl"))

        logger.info(f"Loaded {model_type} model into cache")

    return MODEL_CACHE[model_type]


class SentimentLabelingMap(MapFunction):
    """Sentiment prediction operator"""

    def __init__(self):
        self.vectorizer = None
        self.model = None
        self.label_map = {0: "negative", 1: "neutral", 2: "positive"}
        self.processed_count = 0

        # for hot-reload every ~1 hours
        self.reload_interval = 3000
        self.last_reload_check = 0
        self.shared_model_path = "/shared/sentiment_model.pkl"
        self.last_modified = None

    def open(self, runtime_context):
        """Initialize models when the operator starts"""
        try:
            logger.info("Loading models in sentiment labeling operator...")
            self.vectorizer = get_model("vectorizer")
            self.model = get_model("sentiment")
            logger.info("Models loaded successfully in sentiment labeling operator")
        except Exception as e:
            logger.error(f"Failed to load models: {e}")
            raise

    def maybe_reload_model(self):
        """If a new model is available in /shared/, load it (every ~3 hours)."""
        current_time = time.time()
        if current_time - self.last_reload_check >= self.reload_interval:
            self.last_reload_check = current_time
            logger.info("Checking for updated sentiment model in /shared/")

            if os.path.exists(self.shared_model_path):
                try:
                    current_modified = os.path.getmtime(self.shared_model_path)
                    if self.last_modified is None or current_modified != self.last_modified:
                        logger.info("Detected updated sentiment model in /shared/, reloading...")
                        self.model = load(self.shared_model_path)
                        self.last_modified = current_modified
                        logger.info("Model hot-reloaded from /shared/")
                except Exception as e:
                    logger.warning(f"Failed to reload model from /shared/: {e}")
            else:
                logger.info("No updated sentiment model found in /shared/")
    def map(self, value: str) -> str:
        """Perform sentiment prediction on cleaned comments"""
        self.maybe_reload_model()
        try:
            # Parse input JSON (from cleaning stage)
            obj = json.loads(value)
            cleaned_body = obj.get("cleaned_body", "")

            # Vectorize and predict sentiment
            if cleaned_body:
                vector = self.vectorizer.transform([cleaned_body])
                prediction = self.model.predict(vector)[0]
                label = self.label_map.get(prediction, "unknown")
            else:
                label = "unknown"

            # Create final output (same format as original labeled-reddit-comments)
            result = {
                "id": obj.get("id", ""),
                "created_utc": obj.get("created_utc", 0),
                "subreddit": obj.get("subreddit", ""),
                "score": obj.get("score", 0),
                "cleaned_body": cleaned_body,
                "label": label,
                "original_length": obj.get("original_length", 0)
            }

            self.processed_count += 1
            if self.processed_count % 1000 == 0:
                logger.info(f"Labeled {self.processed_count} comments")
                gc.collect()

            return json.dumps(result)

        except Exception as e:
            logger.warning(f"Sentiment labeling failed: {e}")
            return json.dumps({
                "error": str(e),
                "label": "error",
                "cleaned_body": "",
                "processed_count": self.processed_count
            })


class SentimentLabelingProcessor:
    """Kafka processor for sentiment labeling"""

    def __init__(self,
                 bootstrap_server: str = KAFKA_ENDPOINT,
                 source_topic: str = SOURCE_TOPIC,
                 sink_topic: str = SINK_TOPIC,
                 parallelism: int = 1):
        self.bootstrap_server = bootstrap_server
        self.source_topic = source_topic
        self.sink_topic = sink_topic

        # Initialize Flink environment
        self.env = StreamExecutionEnvironment.get_execution_environment()
        self.env.set_parallelism(parallelism)
        self.env.enable_checkpointing(30000)

        # Configure Kafka source and sink
        self.source = self._create_kafka_source()
        self.sink = self._create_kafka_sink()

        logger.info(f"Initialized sentiment labeling processor")

    def _create_kafka_source(self) -> KafkaSource:
        """Create Kafka source"""
        return KafkaSource.builder() \
            .set_bootstrap_servers(self.bootstrap_server) \
            .set_topics(self.source_topic) \
            .set_value_only_deserializer(SimpleStringSchema()) \
            .set_starting_offsets(KafkaOffsetsInitializer.earliest()) \
            .build()

    def _create_kafka_sink(self) -> KafkaSink:
        """Create Kafka sink"""
        record_serializer = KafkaRecordSerializationSchema.builder() \
            .set_topic(self.sink_topic) \
            .set_value_serialization_schema(SimpleStringSchema()) \
            .build()

        return KafkaSink.builder() \
            .set_bootstrap_servers(self.bootstrap_server) \
            .set_record_serializer(record_serializer) \
            .set_delivery_guarantee(DeliveryGuarantee.AT_LEAST_ONCE) \
            .build()

    def run(self, job_name: Optional[str] = None):
        """Execute the sentiment labeling job"""
        try:
            self.env.set_runtime_mode(RuntimeExecutionMode.STREAMING)

            # Create data stream from Kafka source
            data_stream = self.env.from_source(
                self.source,
                WatermarkStrategy.no_watermarks(),
                "Cleaned_Comments_Source"
            )

            logger.info("Starting sentiment labeling pipeline...")

            # Apply sentiment labeling transformation
            labeled_stream = data_stream.map(
                SentimentLabelingMap(),
                output_type=Types.STRING()
            )

            # Sink to labeled comments topic
            logger.info("Setting up labeled comments sink...")
            labeled_stream.sink_to(self.sink)

            # Execute the job
            job_name = job_name or "SentimentLabelingProcessor"
            logger.info(f"Starting sentiment labeling job: {job_name}")
            self.env.execute(job_name)

        except Exception as e:
            logger.error(f"Failed to execute labeling job: {e}")
            raise


def main():
    """Main entry point"""
    try:
        logger.info("Starting sentiment labeling processor")
        logger.info(f"Kafka endpoint: {KAFKA_ENDPOINT}")

        processor = SentimentLabelingProcessor(parallelism=3)
        processor.run()

    except KeyboardInterrupt:
        logger.info("Processor stopped by user")
    except Exception as e:
        logger.error(f"Application failed: {e}")
        raise


if __name__ == '__main__':
    main()