"""
Reddit Comment Kafka Producer

Streams Reddit comments from compressed archive files to Kafka.
Maintains temporal ordering and original timing between messages.
Modified to stop processing when MAX_TIMESTAMP is exceeded (if not set to -1).

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

from config.kafka_producer_config import *

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)  # Kafka expects milliseconds for timestamps
logger = logging.getLogger(__name__)


class RedditCommentProducer:
    """Kafka producer for streaming Reddit comments with temporal batching"""

    def __init__(self,
                 bootstrap_server: str = KAFKA_BOOTSTRAP_SERVER,
                 topic: str = RAW_COM_TOPIC,
                 data_path: str = DATA_PATH,
                 kafka_config=DEFAULT_KAFKA_CONFIG):

        if not Path(data_path).exists():
            raise FileNotFoundError(f"Reddit data file not found: {data_path}")

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

        self._metrics = {
            'messages_sent': 0,
            'invalid_json': 0,
            'kafka_errors': 0,
            'delivery_errors': 0,
            'filtered_out': 0,
            'total_processed': 0,
            'timestamp_stopped': 0
        }

        logger.info(f"Producer initialized for topic '{topic}' with data: {data_path}")

    @staticmethod
    def validate_comment(comment: Dict[str, Any]) -> tuple[bool, bool]:
        """
        Validate if a Reddit comment meets filtering criteria
        Returns (is_valid, should_stop)
        """
        try:
            # Check for required fields
            if not all(field in comment for field in FIELDS_TO_KEEP):
                return False, False

            # Drop fields that are not in FIELDS_TO_KEEP
            keys_to_remove = [key for key in comment.keys() if key not in FIELDS_TO_KEEP]
            for key in keys_to_remove:
                del comment[key]

            # Check timestamp range
            created_utc = int(comment.get("created_utc", 0))

            # Check if we should stop due to timestamp exceeding maximum
            # If MAX_TIMESTAMP is -1, there's no upper limit
            if MAX_TIMESTAMP != -1 and created_utc > MAX_TIMESTAMP:
                logger.info(
                    f"Comment timestamp {created_utc} exceeds MAX_TIMESTAMP {MAX_TIMESTAMP}. Stopping processing.")
                return False, True  # Invalid for processing, should stop

            # Check minimum timestamp
            if created_utc < MIN_TIMESTAMP:
                return False, False  # Invalid but continue processing

            # Filter out deleted/removed comments
            body = comment.get("body", "").strip().lower()
            if body in ["[deleted]", "[removed]", ""]:
                return False, False

            # Enforce minimum body length
            if len(comment.get("body", "").strip()) < MIN_BODY_LENGTH:
                return False, False

            return True, False

        except (ValueError, TypeError, KeyError) as e:
            logger.error(f"Comment validation error: {e}")
            return False, False

    def _delivery_report(self, err: Optional[Exception], msg: Message) -> None:
        """Kafka delivery callback function"""
        if err is not None:
            self._metrics['delivery_errors'] += 1
            logger.error(f'Message delivery failed: {err}')
        else:
            self._metrics['messages_sent'] += 1

    def _log_stats(self, total_count: int, start_time: float, final: bool = False,
                   stopped_by_timestamp: bool = False) -> None:
        """Log processing statistics"""
        elapsed = time.time() - start_time
        rate = total_count / elapsed if elapsed > 0 else 0

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

        if stopped_by_timestamp:
            stats['stopped_reason'] = 'MAX_TIMESTAMP exceeded'
            stats['timestamp_stopped'] = self._metrics['timestamp_stopped']

        logger.info(f"Processing stats: {json.dumps(stats)}")

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
        """Submit a batch of comments to Kafka"""
        for batch_comment in batch:
            try:
                # Kafka expects milliseconds for timestamps
                message_timestamp = int(batch_comment['created_utc'] * 1000)
                message_value = json.dumps(batch_comment).encode('utf-8')

                self.producer.produce(
                    topic=self.topic,
                    value=message_value,
                    timestamp=message_timestamp,
                    callback=self._delivery_report
                )

                self.producer.poll(0)

            except KafkaException as e:
                self._metrics['kafka_errors'] += 1
                logger.error(f"Kafka error while producing message: {e}")

            except BufferError:
                logger.warning("Producer buffer full, waiting for space...")
                self.producer.poll(10)

                try:
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

    def stream_raw_data(self, speed_factor: float = 1.0, limit_messages: Optional[int] = None) -> None:
        """Stream Reddit comments from compressed file to Kafka"""
        start_time = time.time()
        message_count = 0
        input_path = Path(self.raw_data_path)
        stop_reason = None  # Track why processing stopped: 'timestamp', 'limit', or None

        # Use config default if no limit specified
        if limit_messages is None:
            limit_messages = LIMIT_MESSAGES

        logger.info(f"Starting to stream data from: {input_path}")
        logger.info(f"Speed factor: {speed_factor}x, Message limit: {limit_messages}")
        logger.info(f"Timestamp filtering: MIN={MIN_TIMESTAMP}, MAX={MAX_TIMESTAMP} (-1 means no upper limit)")

        try:
            with open(input_path, "rb") as compressed_file:
                logger.debug(f"Opening compressed file: {input_path}")

                dctx = zstd.ZstdDecompressor()
                stream_reader = dctx.stream_reader(compressed_file)
                text_stream = io.TextIOWrapper(stream_reader, encoding='utf-8')

                prev_timestamp = None
                current_batch = []

                logger.info("Starting line-by-line processing...")

                for line_number, line in enumerate(text_stream, 1):
                    # Check the message limit (if set to 0 or negative, run infinitely)
                    if 0 < limit_messages <= message_count:
                        logger.info(f"Reached message limit of {limit_messages}")
                        stop_reason = 'limit'
                        break

                    # Parse JSON comment
                    try:
                        comment = json.loads(line.strip())
                        self._metrics['total_processed'] += 1
                    except json.JSONDecodeError as e:
                        self._metrics['invalid_json'] += 1
                        logger.debug(f"JSON decode error on line {line_number}: {e}")
                        continue

                    # Validate comment and check if we should stop
                    is_valid, should_stop = self.validate_comment(comment)

                    if should_stop:
                        # Stop processing due to timestamp exceeding maximum
                        self._metrics['timestamp_stopped'] = comment.get('created_utc', 0)
                        stop_reason = 'timestamp'
                        logger.info(f"Stopping processing at line {line_number} due to timestamp limit")
                        break

                    if not is_valid:
                        self._metrics['filtered_out'] += 1
                        continue

                    current_timestamp = comment.get('created_utc')

                    # Process temporal batching
                    if prev_timestamp is not None and current_timestamp != prev_timestamp:
                        if current_batch:
                            self.submit_batch(current_batch)

                        # Calculate and apply inter-batch delay
                        time_diff = current_timestamp - prev_timestamp
                        adjusted_delay = time_diff / speed_factor

                        if adjusted_delay > 0:
                            time.sleep(adjusted_delay)

                        current_batch = []

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

                logger.info("Flushing remaining messages...")
                self.producer.flush(timeout=30)

                # Determine if stopped by timestamp
                stopped_by_timestamp = (stop_reason == 'timestamp')
                self._log_stats(message_count, start_time, final=True, stopped_by_timestamp=stopped_by_timestamp)

                # Log completion reason
                if stop_reason == 'timestamp':
                    logger.info(
                        f"Streaming completed - stopped due to timestamp limit at {self._metrics['timestamp_stopped']}")
                elif stop_reason == 'limit':
                    logger.info(f"Streaming completed - stopped due to message limit of {limit_messages}")
                else:
                    logger.info("Streaming completed successfully - reached end of file")

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

    def close(self) -> None:
        """Gracefully close the producer"""
        logger.info("Shutting down producer...")
        self.producer.flush(timeout=30)
        logger.info("Producer shutdown complete")


def parse_arguments() -> argparse.Namespace:
    """Parse command-line arguments"""
    parser = argparse.ArgumentParser(
        description="Reddit Comment Kafka Producer - Stream Reddit comments to Kafka",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )

    parser.add_argument(
        "--message_limit",
        type=int,
        default=None,
        help="Maximum number of messages to process (0 or negative for infinite)"
    )

    parser.add_argument(
        "--speed_factor",
        type=float,
        default=None,
        help="Speed multiplier for streaming (1.0=original speed, 2.0=2x faster)"
    )

    parser.add_argument(
        "--data_path",
        type=str,
        default=None,
        help="Path to the compressed Reddit comments file"
    )

    parser.add_argument(
        "--bootstrap_servers",
        type=str,
        default=None,
        help="Kafka bootstrap server"
    )

    return parser.parse_args()


def main():
    """Main entry point"""
    args = parse_arguments()

    logger.info("Starting Reddit Comment Producer")
    logger.info(f"Configuration: {vars(args)}")

    constructor_args = {}
    if args.bootstrap_servers is not None:
        constructor_args['bootstrap_server'] = args.bootstrap_servers
    if args.data_path is not None:
        constructor_args['data_path'] = args.data_path

    producer = None
    try:
        producer = RedditCommentProducer(**constructor_args)

        stream_args = {}
        if args.speed_factor is not None:
            stream_args['speed_factor'] = args.speed_factor
        if args.message_limit is not None:
            stream_args['limit_messages'] = args.message_limit

        producer.stream_raw_data(**stream_args)

    except KeyboardInterrupt:
        logger.info("Producer stopped by user")
    except Exception as e:
        logger.error(f"Producer failed: {e}")
        raise
    finally:
        if producer:
            producer.close()


"""
USAGE EXAMPLES:
   1. python raw_comm_producer.py --speed_factor 1.0 --message_limit 5000
      python raw_comm_producer.py

   2. Custom configuration:
   python raw_comm_producer.py \
     --bootstrap_server "localhost:29092" \
     --data_path "/path/to/RC_2023-01.zst" \
     --message_limit 10000 \

CONFIGURATION FILES:
Look more config/kafka_producer_config.py
You can pass them either as command line arguments or modify the default values in the code.
"""

if __name__ == "__main__":
    main()
