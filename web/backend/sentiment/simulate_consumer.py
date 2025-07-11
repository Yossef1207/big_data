# sentiment/simulate_consumer.py
import time, random
from channels.layers import get_channel_layer
from asgiref.sync import async_to_sync
from .sessions import sessions

def start_simulator():
    channel_layer = get_channel_layer()
    while True:
        now = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
        if sessions:
            print(f"[SIM] mass sending for {len(sessions)} sessions @ {now}")
        for user_id, info in list(sessions.items()):
            value1 = random.random()
            value2 = 1 - value1
            print(f"[SIM] -> {user_id}: {value1:.2f}, {value2:.2f}, info: {info}")
            async_to_sync(channel_layer.group_send)(
                f"sentiment_{user_id}",
                {
                    "type": "send_sentiment",
                    "message": {
                        "keyword1": info["keyword1"],
                        "value1": value1,
                        "keyword2": info["keyword2"],
                        "value2": value2,
                        "timestamp": now,
                    },
                },
            )
        time.sleep(1)
