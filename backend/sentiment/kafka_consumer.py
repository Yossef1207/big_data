from confluent_kafka import Consumer, KafkaException
import json, os
from channels.layers import get_channel_layer
from asgiref.sync import async_to_sync
from .sessions import sessions
import logging
logger = logging.getLogger("sentiment.kafka_consumer")


def start_kafka_consumer():
    """
    Listener for Kafka topic "keyword_responses".
    It sends messages to the appropriate WebSocket group based on user_id.
    It expects messages in the format:
    {
        "user_id": "some_user_id",
        "keyword1": "keyword1_value",
        "value1": 0.75,
        "keyword2": "keyword2_value",
        "value2": 0.25,
        "timestamp": "2023-10-01T12:00:00Z"
    }
    """
    BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
    conf = {
        "bootstrap.servers": BOOTSTRAP,
        "group.id": "django-sentiment-consumer",
        "auto.offset.reset": "earliest",
    }
    c = Consumer(conf)
    c.subscribe(["keyword_responses"])

    channel_layer = get_channel_layer()

    try:
        while True:
            msg = c.poll(0.1)  # wait for a maximum of 1 second
            if msg is None:
                continue
            if msg.error():
                # You can log: msg.error().str()
                continue

            data = json.loads(msg.value().decode("utf-8"))
            # We expect the format: { user_id, keyword1, value1, keyword2, value2, timestamp }
            user_id = data.get("user_id")
            if user_id not in sessions:
                continue

            logger.info("[KAFKA] push → %s: %s", user_id, data)

            async_to_sync(channel_layer.group_send)(
            f"sentiment_{user_id}",
            {
                "type": "send_sentiment",
                "message": {
                    "keyword1": data.get("keyword1"),
                    "value1": data.get("value1"),
                    "total1":   data.get("total1"),
                    "keyword2": data.get("keyword2"),
                    "value2": data.get("value2"),
                    "total2":   data.get("total2"),
                    "timestamp": data.get("timestamp"),
                },
            },
        )
    except KeyboardInterrupt:
        pass
    finally:
        c.close()