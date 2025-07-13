"""
Comment Cleaning Processor - First Stage
Handles text cleaning and preprocessing only
"""

import os
import logging
from typing import Optional
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
import json

from config.slang import Slang

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Configuration constants
SOURCE_TOPIC = "raw-reddit-comments"
SINK_TOPIC = "cleaned-reddit-comments"  # New intermediate topic
KAFKA_ENDPOINT = "kafka:9092"


def expand_slang(text: str) -> str:
    """Expand slang abbreviations"""
    words = text.split()
    expanded = [Slang.abbreviations.get(word.lower(), word) for word in words]
    return " ".join(expanded)


def clean_text(text: str) -> str:
    """Clean and preprocess text"""
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


class CommentCleaningMap(MapFunction):
    """Text cleaning and preprocessing operator"""

    def __init__(self):
        self.processed_count = 0

    def open(self, runtime_context):
        """Initialize operator"""
        logger.info("Comment cleaning operator initialized")

    def map(self, value: str) -> str:
        """Clean and preprocess comments"""
        try:
            # Parse input JSON
            obj = json.loads(value)
            original_body = obj.get("body", "")

            # Clean text
            cleaned_body = clean_text(original_body)

            # Create output with cleaned text
            result = {
                "id": obj.get("id", ""),
                "created_utc": obj.get("created_utc", 0),
                "subreddit": obj.get("subreddit", ""),
                "score": obj.get("score", 0),
                "body": original_body,  # Keep original
                "cleaned_body": cleaned_body,  # Add cleaned version
                "original_length": len(original_body)
            }

            self.processed_count += 1
            if self.processed_count % 10000 == 0:
                logger.info(f"Cleaned {self.processed_count} comments")
                gc.collect()

            return json.dumps(result)

        except Exception as e:
            logger.warning(f"Cleaning failed: {e}")
            return json.dumps({
                "error": str(e),
                "cleaned_body": "",
                "processed_count": self.processed_count
            })


class CommentCleaningProcessor:
    """Kafka processor for comment cleaning"""

    def __init__(self,
                 bootstrap_server: str = KAFKA_ENDPOINT,
                 source_topic: str = SOURCE_TOPIC,
                 sink_topic: str = SINK_TOPIC):
        self.bootstrap_server = bootstrap_server
        self.source_topic = source_topic
        self.sink_topic = sink_topic

        # Initialize Flink environment
        self.env = StreamExecutionEnvironment.get_execution_environment()
        self.env.set_parallelism(1)
        self.env.enable_checkpointing(30000)

        # Configure Kafka source and sink
        self.source = self._create_kafka_source()
        self.sink = self._create_kafka_sink()

        logger.info(f"Initialized comment cleaning processor")

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
        """Execute the cleaning job"""
        try:
            self.env.set_runtime_mode(RuntimeExecutionMode.STREAMING)

            # Create data stream from Kafka source
            data_stream = self.env.from_source(
                self.source,
                WatermarkStrategy.no_watermarks(),
                "Raw_Comments_Source"
            )

            logger.info("Starting comment cleaning pipeline...")

            # Apply cleaning transformation
            cleaned_stream = data_stream.map(
                CommentCleaningMap(),
                output_type=Types.STRING()
            )

            # Sink to cleaned comments topic
            logger.info("Setting up cleaned comments sink...")
            cleaned_stream.sink_to(self.sink)

            # Execute the job
            job_name = job_name or "CommentCleaningProcessor"
            logger.info(f"Starting comment cleaning job: {job_name}")
            self.env.execute(job_name)

        except Exception as e:
            logger.error(f"Failed to execute cleaning job: {e}")
            raise


def main():
    """Main entry point"""
    try:
        logger.info("Starting comment cleaning processor")
        logger.info(f"Kafka endpoint: {KAFKA_ENDPOINT}")

        processor = CommentCleaningProcessor()
        processor.run()

    except KeyboardInterrupt:
        logger.info("Processor stopped by user")
    except Exception as e:
        logger.error(f"Application failed: {e}")
        raise


if __name__ == '__main__':
    main()