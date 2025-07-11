from confluent_kafka import Producer
import json
import threading

_producer: Producer | None = None
_lock = threading.Lock()


def _get_producer() -> Producer:
    global _producer
    with _lock:
        if _producer is None:
            # Config for Kafka producer
            conf = {"bootstrap.servers": "localhost:9092"}
            _producer = Producer(conf)
        return _producer


def send_to_kafka(topic: str, payload: dict):
    """
    Sends payload (dictionary) to the specified topic as JSON.
    """
    p = _get_producer()
    data = json.dumps(payload).encode("utf-8")
    p.produce(topic, data)
    # Poll for async callbacks
    p.poll(0)