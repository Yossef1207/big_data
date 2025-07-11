"""
Memory-Optimized Flink Kafka Consumer for Raw Reddit Comments Processing

This optimized version addresses memory issues by:
1. Using a single combined operator instead of separate map operations
2. Implementing lazy model loading
3. Reducing data serialization overhead
4. Optimizing TF-IDF vector handling

Author: Optimized for low-memory environments
Version: 2.0 - Memory Optimized
"""

import os
import logging
from typing import Optional, Dict, Any
import gc

from pyflink.common import SimpleStringSchema, WatermarkStrategy
from pyflink.datastream import StreamExecutionEnvironment, RuntimeExecutionMode, MapFunction
from pyflink.datastream.connectors.kafka import KafkaSource, KafkaOffsetsInitializer
from pyflink.datastream.connectors.kafka import KafkaSink, DeliveryGuarantee
from pyflink.datastream.connectors.kafka import KafkaRecordSerializationSchema
from pyflink.common.typeinfo import Types

import emoji
import re
import nltk
from nltk.tokenize import word_tokenize
from joblib import load
import json
import numpy as np

from config.slang import Slang

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Configuration constants
TOPIC_1 = "raw-reddit-comments"
TOPIC_2 = "labeled-reddit-comments"
KAFKA_ENDPOINT = "kafka:9092"
DOCKER_RUN = True

# Global model cache to avoid loading multiple times
MODEL_CACHE = {}


def get_model(model_type: str):
    """Lazy loading of models with caching to reduce memory usage."""
    if model_type not in MODEL_CACHE:
        if DOCKER_RUN:
            base_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "/data")
        else:
            base_path = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "data_processing"))

        if model_type == "vectorizer":
            MODEL_CACHE[model_type] = load(os.path.join(base_path, "tfidf_vectorizer.pkl"))
        elif model_type == "sentiment":
            MODEL_CACHE[model_type] = load(os.path.join(base_path, "sentiment_model.pkl"))

        logger.info(f"Loaded {model_type} model into cache")

    return MODEL_CACHE[model_type]


def expand_slang(text: str) -> str:
    """Expand slang abbreviations."""
    words = text.split()
    expanded = [Slang.abbreviations.get(word.lower(), word) for word in words]
    return " ".join(expanded)


def clean_text(text: str) -> str:
    """Clean and preprocess text efficiently."""
    try:
        text = text.lower()
        text = emoji.demojize(text, delimiters=(" ", " "))
        text = expand_slang(text)
        text = re.sub(r"/?u/\w+", "", text)
        text = re.sub(r"/?r/\w+", "", text)
        text = re.sub(r"http\S+|www.\S+", "", text)
        text = re.sub(r"[^a-zA-Z0-9\s]", "", text)
        text = re.sub(r"\s+", " ", text).strip()
        tokens = word_tokenize(text)
        return " ".join(tokens)
    except Exception as e:
        logger.warning(f"Error cleaning text: {e}")
        return ""


class CombinedProcessingMap(MapFunction):
    """
    Combined processing operator that does cleaning, vectorization, and sentiment prediction
    in a single step to minimize data serialization overhead.
    """

    def __init__(self):
        self.vectorizer = None
        self.model = None
        self.label_map = {0: "negative", 1: "neutral", 2: "positive"}
        self.processed_count = 0

    def open(self, runtime_context):
        """Initialize models when the operator starts."""
        try:
            logger.info("Loading models in combined operator...")
            self.vectorizer = get_model("vectorizer")
            self.model = get_model("sentiment")
            logger.info("Models loaded successfully in combined operator")
        except Exception as e:
            logger.error(f"Failed to load models: {e}")
            raise

    def map(self, value: str) -> str:
        """Process the entire pipeline in one step."""
        try:
            # Parse input JSON
            obj = json.loads(value)
            original_body = obj.get("body", "")

            # Step 1: Clean text
            cleaned_body = clean_text(original_body)

            # Step 2: Vectorize (but don't store the full vector)
            if cleaned_body:
                vector = self.vectorizer.transform([cleaned_body])

                # Step 3: Predict sentiment immediately
                X = vector  # Use sparse matrix directly
                prediction = self.model.predict(X)[0]
                label = self.label_map.get(prediction, "unknown")
            else:
                label = "unknown"

            # Step 4: Create minimal output (no large vectors)
            result = {
                "id": obj.get("id", ""),
                "created_utc": obj.get("created_utc", 0),
                "subreddit": obj.get("subreddit", ""),
                "score": obj.get("score", 0),
                "cleaned_body": cleaned_body,
                "label": label,
                "original_length": len(original_body)
            }

            self.processed_count += 1
            if self.processed_count % 100 == 0:
                logger.info(f"Processed {self.processed_count} messages")
                # Force garbage collection periodically
                gc.collect()

            return json.dumps(result)

        except Exception as e:
            logger.warning(f"Processing failed: {e}")
            return json.dumps({
                "error": str(e),
                "label": "error",
                "cleaned_body": "",
                "processed_count": self.processed_count
            })


class FlinkKafkaConsumer:
    """Memory-optimized Flink Kafka consumer."""

    def __init__(self,
                 bootstrap_server: str = KAFKA_ENDPOINT,
                 source_topic: str = TOPIC_1,
                 sink_topic: str = TOPIC_2):
        self.bootstrap_server = bootstrap_server
        self.source_topic = source_topic
        self.sink_topic = sink_topic

        # Initialize Flink environment with memory optimizations
        self.env = StreamExecutionEnvironment.get_execution_environment()

        # Memory optimizations
        self.env.set_parallelism(1)  # Reduce parallelism for low memory

        # Configure checkpointing for memory efficiency
        self.env.enable_checkpointing(30000)  # 30 seconds

        # Configure Kafka source and sink
        self.source = self._create_kafka_source()
        self.sink = self._create_kafka_sink()

        logger.info(f"Initialized memory-optimized Kafka processor")

    def _create_kafka_source(self) -> KafkaSource:
        """Create Kafka source with memory-efficient settings."""
        return KafkaSource.builder() \
            .set_bootstrap_servers(self.bootstrap_server) \
            .set_topics(self.source_topic) \
            .set_value_only_deserializer(SimpleStringSchema()) \
            .set_starting_offsets(KafkaOffsetsInitializer.earliest()) \
            .build()

    def _create_kafka_sink(self) -> KafkaSink:
        """Create Kafka sink with memory-efficient settings."""
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
        """Execute the optimized streaming job."""
        try:
            # Configure runtime mode
            self.env.set_runtime_mode(RuntimeExecutionMode.STREAMING)

            # Create data stream from Kafka source
            data_stream = self.env.from_source(
                self.source,
                WatermarkStrategy.no_watermarks(),
                "Kafka_Source"
            )

            logger.info("Starting combined processing pipeline...")

            # Single combined processing step (replaces separate map operations)
            processed_stream = data_stream.map(
                CombinedProcessingMap(),
                output_type=Types.STRING()
            )

            # Optional: Add logging for monitoring
            processed_stream.print("Processed")

            # Sink to output topic
            logger.info("Setting up Kafka sink...")
            processed_stream.sink_to(self.sink)

            # Execute the job
            job_name = job_name or "OptimizedRedditSentimentAnalysis"
            logger.info(f"Starting optimized Flink job: {job_name}")
            self.env.execute(job_name)

        except Exception as e:
            logger.error(f"Failed to execute Flink job: {e}")
            raise

def main():
    """Main entry point for the optimized processor."""
    try:
        logger.info("Starting optimized Flink processor")
        logger.info(f"Kafka endpoint: {KAFKA_ENDPOINT}")

        # Create and run the optimized consumer
        processor = FlinkKafkaConsumer()
        processor.run()

    except KeyboardInterrupt:
        logger.info("Consumer stopped by user")
    except Exception as e:
        logger.error(f"Application failed: {e}")
        raise

if __name__ == '__main__':
    main()