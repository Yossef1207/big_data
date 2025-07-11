# test_producer.py
import time
from config.data_config import *
from data_ingestion.raw_comm_producer import RedditCommentProducer

def test_connection(self):
    """Test if producer can connect to Kafka"""
    try:
        self.producer.list_topics(timeout=10)
        return True
    except Exception as e:
        print(f"Connection failed: {e}")
        return False

def run_producer_test():
    """Test the Confluent Kafka producer with various scenarios"""
    print("=== Starting Confluent Kafka Producer Test ===")

    # Test configuration
    test_config = {
        'bootstrap_servers': "localhost:29092",
        'topic': TOPIC_1,
        'test_data_path': DATA_PATH
    }

    # Scenario 1: Normal operation
    print("\n--- Testing normal message flow ---")
    normal_producer = RedditCommentProducer(
        bootstrap_servers=test_config['bootstrap_servers'],
        topic=test_config['topic'],
        data_path=test_config['test_data_path']
    )

    start_time = time.time()
    test_connection(normal_producer)

    normal_producer.stream_raw_data(speed_factor=1.0)


if __name__ == "__main__":
    run_producer_test()