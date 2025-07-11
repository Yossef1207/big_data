from confluent_kafka import Producer
import json, os
import threading

_producer: Producer | None = None
_lock = threading.Lock()

BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")


def _get_producer() -> Producer:
    global _producer
    with _lock:
        if _producer is None:
            _producer = Producer({"bootstrap.servers": BOOTSTRAP})
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