"""
Reddit Comment Kafka Producer

This module provides a Kafka producer for streaming Reddit comments
from compressed archive files. It reads Zstandard-compressed JSON files containing
Reddit comments and publishes them to a Kafka topic while maintaining temporal
ordering and original timing between messages.

Features:
    - Zstandard decompression for efficient data reading
    - Temporal batching to maintain comment timing relationships
    - Validation and filtering of comments
    - Error handling and metrics
    - Configurable speed factor for replay acceleration/deceleration

Dependencies:
    - confluent-kafka: Kafka client library
    - zstandard: Compression library for Reddit data files
    - Custom configuration modules for data and Kafka settings

Example:
    Basic usage:
        python raw_comm_producer.py --message_limit 1000 --speed_factor 2.0

    Programmatic usage:
        producer = RedditCommentProducer("localhost:9092", "comments", "data.zst")
        producer.stream_raw_data(speed_factor=1.5, limit_messages=5000)

Author: Veronika Anokhina
Date: 01.06.2025
Version: 1.0
"""

import io
import json
import time
from datetime import datetime
import argparse
import logging
from pathlib import Path
from typing import Dict, List, Optional, Any

import zstandard as zstd
from confluent_kafka import Producer, KafkaException, Message

#My own configurations
from config.kafka_producer_config import *

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class RedditCommentProducer:
    """
    Kafka producer for streaming Reddit comments.

    This class handles reading compressed Reddit comment data, validates and filters
    comments based on configurable criteria, and streams them to Kafka while
    maintaining temporal relationships between messages.

    The producer implements batching based on comment timestamps to preserve
    the original timing patterns in the data, which is crucial for realistic
    streaming analytics scenarios.

    Attributes:
        producer (Producer):    Confluent Kafka producer instance
        topic (str):            Target Kafka topic name
        raw_data_path (str):    Path to the compressed Reddit data file
        _metrics (dict):        Internal metrics tracking message processing

    """

    def __init__(self,
                 bootstrap_server:str=KAFKA_BOOTSTRAP_SERVER,
                 topic: str=RAW_COM_TOPIC,
                 data_path: str = DATA_PATH,
                 kafka_config=DEFAULT_KAFKA_CONFIG):
        """
        Initialize the Reddit Comment Producer.

        Args:
            bootstrap_servers (str): Kafka broker address
            topic (str): Name of the Kafka topic to publish messages to
            data_path (str): Path to the compressed Reddit comments file (.zst format)

        Raises:
            FileNotFoundError: If the data file doesn't exist
            KafkaException: If Kafka producer initialization fails
        """
        # Validate data file exists
        if not Path(data_path).exists():
            raise FileNotFoundError(f"Reddit data file not found: {data_path}")

        # Load default Kafka configuration and override bootstrap servers
        conf = kafka_config.copy()
        conf['bootstrap.servers'] = bootstrap_server

        try:
            self.producer = Producer(conf)
            logger.info(f"Kafka producer initialized with servers: {bootstrap_server}")
        except Exception as e:
            logger.error(f"Failed to initialize Kafka producer: {e}")
            raise

        self.topic = topic
        self.raw_data_path = data_path

        # Initialize metrics tracking
        self._metrics = {
            'messages_sent': 0,  # Successfully delivered messages
            'invalid_json': 0,  # JSON parsing failures
            'kafka_errors': 0,  # Kafka-specific errors
            'delivery_errors': 0,  # Message delivery failures
            'filtered_out': 0,  # Comments filtered by validation
            'total_processed': 0  # Total comments processed
        }

        logger.info(f"Producer initialized for topic '{topic}' with data: {data_path}")

    @staticmethod
    def validate_comment(comment: Dict[str, Any]) -> bool:
        """
        Validate if a Reddit comment meets the filtering criteria.

        This method performs comprehensive validation including:
        - Required field presence check
        - Timestamp range validation
        - Content quality filtering (deleted/removed comments)
        - Minimum body length requirement

        Args:
            comment (dict): Reddit comment dictionary with fields like 'body', 'created_utc', etc.

        Returns:
            bool: True if comment passes all validation criteria, False otherwise

        Note:
            Validation criteria are defined in config/data_config.py:
            - FIELDS_TO_KEEP: Required fields that must be present
            - MIN_TIMESTAMP, MAX_TIMESTAMP: Acceptable timestamp range
            - MIN_BODY_LENGTH: Minimum comment body length
        """
        try:
            # Check for required fields - drop fields that are not in fields to keep
            if not all(field in comment for field in FIELDS_TO_KEEP):
                return False

            # Drop fields that are not in FIELDS_TO_KEEP (keep only specified fields)
            keys_to_remove = [key for key in comment.keys() if key not in FIELDS_TO_KEEP]
            for key in keys_to_remove:
                del comment[key]

            # Validate timestamp range (Unix timestamp)
            created_utc = int(comment.get("created_utc", 0))
            if not (MIN_TIMESTAMP <= created_utc <= MAX_TIMESTAMP):
                return False

            # Filter out deleted/removed comments
            body = comment.get("body", "").strip().lower()
            if body in ["[deleted]", "[removed]", ""]:
                return False

            # Enforce minimum body length to filter out low-quality comments
            if len(comment.get("body", "").strip()) < MIN_BODY_LENGTH:
                return False

            return True

        except (ValueError, TypeError, KeyError) as e:
            # Log validation errors in debug mode
            logger.error(f"Comment validation error: {e}")
            return False

    def _delivery_report(self, err: Optional[Exception], msg: Message) -> None:
        """
        Kafka delivery callback function.

        This callback is invoked for each message produced to indicate the delivery
        result. It updates internal metrics and provides debug information about
        message delivery status.

        Args:
            err (Exception, optional): Delivery error if any, None on success
            msg (Message): Kafka message object containing delivery metadata

        Note:
            This method is called asynchronously by the Kafka producer and should
            be lightweight to avoid blocking the producer thread.
        """
        if err is not None:
            self._metrics['delivery_errors'] += 1
            logger.error(f'Message delivery failed: {err}')

        else:
            self._metrics['messages_sent'] += 1

    def _log_stats(self, total_count: int, start_time: float, final: bool = False) -> None:
        """
        Log comprehensive processing statistics.

        Provides both intermediate and final statistics about the streaming process,
        including throughput metrics, error counts, and processing rates.

        Args:
            total_count (int): Total number of comments processed so far
            start_time (float): Unix timestamp when processing started
            final (bool): Whether this is the final statistics report

        Note:
            Statistics are logged in both structured format (for parsing) and
            human-readable format (for monitoring).
        """
        elapsed = time.time() - start_time
        rate = total_count / elapsed if elapsed > 0 else 0

        # Calculate success rate
        success_rate = (self._metrics['messages_sent'] / total_count * 100) if total_count > 0 else 0

        stats = {
            'timestamp': datetime.utcnow().isoformat(),
            'total_processed': total_count,
            'processing_rate': f"{rate:.2f} msg/sec",
            'messages_sent': self._metrics['messages_sent'],
            'success_rate': f"{success_rate:.1f}%",
            'invalid_json': self._metrics['invalid_json'],
            'kafka_errors': self._metrics['kafka_errors'],
            'delivery_errors': self._metrics['delivery_errors'],
            'filtered_out': self._metrics['filtered_out'],
            'elapsed_time': f"{elapsed:.2f}s"
        }

        # Log structured stats for automated monitoring
        logger.info(f"Processing stats: {json.dumps(stats)}")

        # Print human-readable stats
        if final:
            print("\n" + "=" * 50)
            print("         FINAL PROCESSING STATISTICS")
            print("=" * 50)
        else:
            print("\n" + "=" * 50)
            print("      INTERMEDIATE PROCESSING STATISTICS")
            print("=" * 50)

        for key, value in stats.items():
            print(f"{key.replace('_', ' ').title():>20}: {value}")

        print("=" * 50)

    def submit_batch(self, batch: List[Dict[str, Any]]) -> None:
        """
        Submit a batch of comments to Kafka.

        This method handles the actual publishing of comment batches to Kafka,
        including error handling for buffer overflow and network issues.

        Args:
            batch (list): List of validated comment dictionaries to publish

        Note:
            The method uses the original comment timestamp for message timestamping,
            which enables event-time processing in downstream consumers.

            Buffer overflow is handled by polling and retrying, ensuring reliable
            delivery even under high throughput scenarios.
        """
        for batch_comment in batch:
            try:
                # Calculate message timestamp (Kafka expects milliseconds)
                message_timestamp = int(batch_comment['created_utc'] * 1000)

                # Serialize comment to JSON
                message_value = json.dumps(batch_comment).encode('utf-8')

                # Produce message with timestamp preservation
                self.producer.produce(
                    topic=self.topic,
                    value=message_value,
                    timestamp=message_timestamp,
                    callback=self._delivery_report
                )

                # Non-blocking poll to trigger delivery callbacks
                self.producer.poll(0)

            except KafkaException as e:
                self._metrics['kafka_errors'] += 1
                logger.error(f"Kafka error while producing message: {e}")


            except BufferError:
                # Local producer queue is full - wait and retry
                logger.warning("Producer buffer full, waiting for space...")
                self.producer.poll(10)  # Wait up to 10 seconds

                try:
                    # Retry the message production
                    self.producer.produce(
                        topic=self.topic,
                        value=json.dumps(batch_comment).encode('utf-8'),
                        timestamp=int(batch_comment['created_utc'] * 1000),
                        callback=self._delivery_report
                    )
                except Exception as retry_error:
                    self._metrics['kafka_errors'] += 1
                    logger.error(f"Failed to produce message after retry: {retry_error}")

            except Exception as e:
                self._metrics['kafka_errors'] += 1
                logger.error(f"Unexpected error while producing message: {e}")

    def stream_raw_data(self, speed_factor: float = 1.0, limit_messages: int = LIMIT_MESSAGES) -> None:
        """
        Stream Reddit comments from compressed file to Kafka.

        This is the main processing method that:
        1. Opens and decompresses the Reddit data file
        2. Parses JSON comments line by line
        3. Validates and filters comments
        4. Groups comments by timestamp into batches
        5. Maintains original timing between batches
        6. Publishes batches to Kafka

        Args:
            speed_factor (float): Multiplier for replay speed.
                                1.0 = original speed, 2.0 = 2x faster, 0.5 = 2x slower
            limit_messages (int): Maximum number of messages to process (for testing)

        Raises:
            FileNotFoundError: If the data file cannot be found
            zstd.ZstdError: If decompression fails
            KafkaException: If Kafka operations fail

        Note:
            The method preserves temporal relationships between comments by grouping
            them by timestamp and introducing appropriate delays between batches.
            This is crucial for realistic streaming analytics scenarios.
        """
        start_time = time.time()
        message_count = 0
        input_path = Path(self.raw_data_path)

        logger.info(f"Starting to stream data from: {input_path}")
        logger.info(f"Speed factor: {speed_factor}x, Message limit: {limit_messages}")

        try:
            with open(input_path, "rb") as compressed_file:
                logger.debug(f"Opening compressed file: {input_path}")

                # Initialize Zstandard decompressor
                #TODO BLOB
                dctx = zstd.ZstdDecompressor()
                stream_reader = dctx.stream_reader(compressed_file)
                text_stream = io.TextIOWrapper(stream_reader, encoding='utf-8')

                # Temporal batching variables
                prev_timestamp = None
                current_batch = []

                logger.info("Starting line-by-line processing...")

                for line_number, line in enumerate(text_stream, 1):
                    # Check message limit for testing scenarios
                    if message_count >= limit_messages:
                        logger.info(f"Reached message limit of {limit_messages}")
                        break

                    # Parse JSON comment
                    try:
                        comment = json.loads(line.strip())
                        self._metrics['total_processed'] += 1
                    except json.JSONDecodeError as e:
                        self._metrics['invalid_json'] += 1
                        logger.debug(f"JSON decode error on line {line_number}: {e}")
                        continue

                    # Validate comment against filtering criteria
                    if not self.validate_comment(comment):
                        self._metrics['filtered_out'] += 1
                        continue

                    current_timestamp = comment.get('created_utc')

                    # Process temporal batching
                    if prev_timestamp is not None and current_timestamp != prev_timestamp:
                        # Submit the completed batch
                        if current_batch:
                            self.submit_batch(current_batch)

                        # Calculate and apply inter-batch delay to maintain timing
                        time_diff = current_timestamp - prev_timestamp
                        adjusted_delay = time_diff / speed_factor

                        if adjusted_delay > 0:
                            time.sleep(adjusted_delay)

                        # Start new batch
                        current_batch = []

                    # Add comment to current batch
                    current_batch.append(comment)
                    prev_timestamp = current_timestamp
                    message_count += 1

                    # Periodic progress reporting
                    if message_count % 1000 == 0:
                        logger.info(f"Processed {message_count} messages...")

                # Submit the final batch
                if current_batch:
                    logger.info(f"Submitting final batch of {len(current_batch)} messages")
                    self.submit_batch(current_batch)

                # Ensure all messages are delivered before finishing
                logger.info("Flushing remaining messages...")
                self.producer.flush(timeout=30)  # 30-second timeout

                # Final statistics report
                self._log_stats(message_count, start_time, final=True)
                logger.info("Streaming completed successfully")

        except FileNotFoundError:
            logger.error(f"Data file not found: {input_path}")
            raise
        except zstd.ZstdError as e:
            logger.error(f"Decompression error: {e}")
            raise
        except KeyboardInterrupt:
            logger.info("Streaming interrupted by user")
            self.producer.flush(timeout=10)
            raise
        except Exception as e:
            logger.error(f"Unexpected error during streaming: {e}")
            raise

    def get_metrics(self) -> Dict[str, int]:
        """
        Get current processing metrics.

        Returns:
            dict: Dictionary containing current metrics including message counts,
                 error counts, and processing statistics
        """
        return self._metrics.copy()

    def close(self) -> None:
        """
        Gracefully close the producer and flush any remaining messages.

        This method should be called when shutting down to ensure all messages
        are delivered before the application exits.
        """
        logger.info("Shutting down producer...")
        self.producer.flush(timeout=30)
        logger.info("Producer shutdown complete")


def parse_arguments() -> argparse.Namespace:
    """
    Parse command-line arguments for the producer.

    Returns:
        argparse.Namespace: Parsed command-line arguments
    """
    parser = argparse.ArgumentParser(
        description="Reddit Comment Kafka Producer - Stream Reddit comments to Kafka",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )

    parser.add_argument(
        "--message_limit",
        type=int,
        default=None,
        help="Maximum number of messages to process (useful for testing)"
    )

    parser.add_argument(
        "--speed_factor",
        type=float,
        default=None,
        help="Speed multiplier for streaming (1.0=original speed, 2.0=2x faster, 0.5=2x slower)"
    )

    parser.add_argument(
        "--data_path",
        type=str,
        default=None,
        help="Path to the compressed Reddit comments file"
    )

    parser.add_argument(
        "--topic",
        type=str,
        default=None,
        help="Kafka topic to publish messages to"
    )

    parser.add_argument(
        "--bootstrap_servers",
        type=str,
        default=None,
        help="Kafka bootstrap server"
    )

    parser.add_argument(
        "--debug",
        default=False,
        help="Enable debug logging"
    )

    return parser.parse_args()


def main():
    """
    Main entry point for the Reddit Comment Producer application.

    This function:
    1. Parses command-line arguments
    2. Configures logging based on debug flag
    3. Initializes the producer with specified configuration
    4. Starts the streaming process
    5. Handles graceful shutdown
    """
    args = parse_arguments()

    # Configure debug logging if requested
    if args.debug:
        logging.getLogger().setLevel(logging.DEBUG)

    logger.info("Starting Reddit Comment Producer")
    logger.info(f"Configuration: {vars(args)}")

    constructor_args = {}
    if args.bootstrap_servers is not None:
        constructor_args['bootstrap_server'] = args.bootstrap_servers
    if args.topic is not None:
        constructor_args['topic'] = args.topic
    if args.data_path is not None:
        constructor_args['data_path'] = args.data_path

    producer = None
    try:
        # Initialize producer only with provided arguments
        producer = RedditCommentProducer(**constructor_args)

        # Prepare streaming arguments
        stream_args = {}
        if args.speed_factor is not None:
            stream_args['speed_factor'] = args.speed_factor
        if args.message_limit is not None:
            stream_args['limit_messages'] = args.message_limit

        # Start streaming data with provided arguments
        producer.stream_raw_data(**stream_args)

    except KeyboardInterrupt:
        logger.info("Producer stopped by user")
    except Exception as e:
        logger.error(f"Producer failed: {e}")
        raise
    finally:
        # Ensure graceful shutdown
        if producer:
            producer.close()


# Usage Examples and Documentation
"""
USAGE EXAMPLES:
   1. python raw_comm_producer.py --speed_factor 1.0 --message_limit 5000
      python raw_comm_producer.py

   2. Custom configuration:
   python raw_comm_producer.py \
     --bootstrap_server "localhost:29092" \
     --topic "reddit-comments-test" \
     --data_path "/path/to/RC_2023-01.zst" \
     --message_limit 10000 \
     --debug

CONFIGURATION FILES:
The producer depends on two configuration files:
You can pass them eather as command line arguments or modify the default values in the code.

1. config/kafka_producer_config.py:
   - DATA_PATH: Path to compressed Reddit data file
   - FIELDS_TO_KEEP: Required comment fields
   - MIN_TIMESTAMP, MAX_TIMESTAMP: Timestamp filtering range
   - MIN_BODY_LENGTH: Minimum comment body length
   - ==============================================
   - DEFAULT_KAFKA_CONFIG: Kafka producer configuration
   - KAFKA_BOOTSTRAP_SERVER: Default Kafka servers
   - RAW_COM_TOPIC: Default topic name
   - LIMIT_MESSAGES: Default message limit
"""

if __name__ == "__main__":
    main()
