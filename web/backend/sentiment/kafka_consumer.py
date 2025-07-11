from confluent_kafka import Consumer, KafkaException
import json
from channels.layers import get_channel_layer
from asgiref.sync import async_to_sync
from .sessions import sessions


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
    conf = {
        "bootstrap.servers": "localhost:9092",
        "group.id": "django-sentiment-consumer",
        "auto.offset.reset": "earliest",
    }
    c = Consumer(conf)
    c.subscribe(["keyword_responses"])

    channel_layer = get_channel_layer()

    try:
        while True:
            msg = c.poll(1.0)  # wait for a maximum of 1 second
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

            async_to_sync(channel_layer.group_send)(
                f"sentiment_{user_id}",
                {
                    "type": "send_sentiment",
                    "message": {
                        "keyword1": data.get("keyword1"),
                        "value1": data.get("value1"),
                        "keyword2": data.get("keyword2"),
                        "value2": data.get("value2"),
                        "timestamp": data.get("timestamp"),
                    },
                },
            )
    except KeyboardInterrupt:
        pass
    finally:
        c.close()