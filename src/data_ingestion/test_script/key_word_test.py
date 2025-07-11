"""
Test Script for Keyword Aggregation System

This script helps test the keyword aggregation functionality by:
1. Sending test keyword requests to the keyword_requests topic
2. Monitoring responses from keyword_responses topic
3. Verifying the complete pipeline works end-to-end

Usage:
    python test_keyword_system.py --test-request
    python test_keyword_system.py --monitor-responses
    python test_keyword_system.py --full-test
"""

import json
import time
import uuid
from datetime import datetime, timezone
from confluent_kafka import Producer, Consumer, KafkaException
import argparse
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Kafka configuration
KAFKA_BOOTSTRAP_SERVERS = "localhost:29092"  # Use external port when running from host
KEYWORD_REQUESTS_TOPIC = "keyword_requests"
KEYWORD_RESPONSES_TOPIC = "keyword_responses"


def create_producer():
    """Create Kafka producer"""
    config = {
        'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
        'client.id': 'keyword_test_producer'
    }
    return Producer(config)


def create_consumer(topic):
    """Create Kafka consumer"""
    config = {
        'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
        'group.id': f'keyword_test_consumer_{int(time.time())}',
        'auto.offset.reset': 'latest',
        'enable.auto.commit': True
    }
    consumer = Consumer(config)
    consumer.subscribe([topic])
    return consumer


def send_test_request(producer, user_id=None, keyword1="trump", keyword2="biden"):
    """Send a test keyword request"""
    if not user_id:
        user_id = str(uuid.uuid4())

    request = {
        'user_id': user_id,
        'keyword1': keyword1,
        'keyword2': keyword2,
        'request_time': datetime.now(timezone.utc).isoformat().replace('+00:00', 'Z')
    }

    try:
        producer.produce(
            topic=KEYWORD_REQUESTS_TOPIC,
            value=json.dumps(request).encode('utf-8'),
            callback=lambda err, msg: logger.info(f"Request sent: {msg.value()}" if not err else f"Error: {err}")
        )
        producer.flush()
        logger.info(f"Sent keyword request: {request}")
        return user_id
    except Exception as e:
        logger.error(f"Failed to send request: {e}")
        return None


def monitor_responses(duration_seconds=60):
    """Monitor keyword responses"""
    consumer = create_consumer(KEYWORD_RESPONSES_TOPIC)
    logger.info(f"Monitoring responses for {duration_seconds} seconds...")

    start_time = time.time()
    try:
        while time.time() - start_time < duration_seconds:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue
            if msg.error():
                logger.error(f"Consumer error: {msg.error()}")
                continue

            try:
                response = json.loads(msg.value().decode('utf-8'))
                logger.info(f"Received response: {json.dumps(response, indent=2)}")
                print(f"\n=== KEYWORD RESPONSE ===")
                print(f"User ID: {response.get('user_id')}")
                print(f"Keyword1 '{response.get('keyword1')}': {response.get('value1', 0):.2%} positive")
                print(f"Keyword2 '{response.get('keyword2')}': {response.get('value2', 0):.2%} positive")
                print(f"Timestamp: {response.get('timestamp')}")
                print("========================\n")
            except Exception as e:
                logger.error(f"Failed to parse response: {e}")

    except KeyboardInterrupt:
        logger.info("Monitoring stopped by user")
    finally:
        consumer.close()


def run_full_test():
    """Run complete end-to-end test"""
    logger.info("Starting full keyword aggregation test...")

    # Step 1: Send test request
    producer = create_producer()
    user_id = send_test_request(producer, keyword1="trump", keyword2="election")

    if not user_id:
        logger.error("Failed to send test request")
        return

    logger.info(f"Test request sent for user {user_id}")
    logger.info("Now monitoring for responses...")

    # Step 2: Monitor for responses
    monitor_responses(120)  # Monitor for 2 minutes


def test_multiple_requests():
    """Send multiple test requests with different keywords"""
    producer = create_producer()

    test_cases = [
        ("trump", "biden"),
        ("covid", "vaccine"),
        ("crypto", "bitcoin"),
        ("climate", "environment"),
        ("sports", "football")
    ]

    user_ids = []
    for keyword1, keyword2 in test_cases:
        user_id = send_test_request(producer, keyword1=keyword1, keyword2=keyword2)
        if user_id:
            user_ids.append(user_id)
            time.sleep(2)  # Space out requests

    logger.info(f"Sent {len(user_ids)} test requests")
    return user_ids


def check_kafka_topics():
    """Check if required Kafka topics exist"""
    from confluent_kafka.admin import AdminClient

    admin_client = AdminClient({'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS})

    try:
        metadata = admin_client.list_topics(timeout=10)
        topics = set(metadata.topics.keys())

        required_topics = {KEYWORD_REQUESTS_TOPIC, KEYWORD_RESPONSES_TOPIC, "labeled-reddit-comments"}
        missing_topics = required_topics - topics

        if missing_topics:
            logger.warning(f"Missing topics: {missing_topics}")
            return False
        else:
            logger.info("All required topics exist")
            return True

    except Exception as e:
        logger.error(f"Failed to check topics: {e}")
        return False


def main():
    parser = argparse.ArgumentParser(description="Test Keyword Aggregation System")
    parser.add_argument("--test-request", action="store_true", help="Send a single test request")
    parser.add_argument("--monitor-responses", action="store_true", help="Monitor keyword responses")
    parser.add_argument("--full-test", action="store_true", help="Run complete end-to-end test")
    parser.add_argument("--multiple-requests", action="store_true", help="Send multiple test requests")
    parser.add_argument("--check-topics", action="store_true", help="Check if Kafka topics exist")
    parser.add_argument("--keyword1", default="trump", help="First keyword for test")
    parser.add_argument("--keyword2", default="biden", help="Second keyword for test")
    parser.add_argument("--duration", type=int, default=60, help="Monitoring duration in seconds")

    #{"id": "ejualnj",
    # "created_utc": 1554076800,
    # "subreddit": "IndoorGarden",
    # "score": 3,
    # "cleaned_body": "how do you avoid getting fungus gnats in her soil my maidenhair fern seems to be prone to them because she likes water facewithrollingeyes",
    # "label": "negative",
    # "original_length": 121}

    #{"id": "ejualnk",
    # "created_utc": 1554076800,
    # "subreddit": "doordash",
    # "score": 7, "cleaned_body":
    # "we still get paid in full dont worry",
    # "label": "positive",
    # "original_length": 37}

    args = parser.parse_args()

    if args.check_topics:
        check_kafka_topics()
    elif args.test_request:
        producer = create_producer()
        user_id = send_test_request(producer, keyword1=args.keyword1, keyword2=args.keyword2)
        if user_id:
            print(f"Test request sent for user: {user_id}")
    elif args.monitor_responses:
        monitor_responses(args.duration)
    elif args.multiple_requests:
        test_multiple_requests()
    elif args.full_test:
        run_full_test()
    else:
        parser.print_help()


if __name__ == "__main__":
    main()