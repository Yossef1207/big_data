# sentiment/apps.py

import threading
from django.apps import AppConfig
from django.conf import settings

class SentimentConfig(AppConfig):
    default_auto_field = "django.db.models.BigAutoField"
    name = "sentiment"

    def ready(self):
        # Jeżeli tryb symulacji, odpalamy zawsze symulator
        if getattr(settings, "SIMULATE_DATA", False):
            print("[APPS] SIMULATE_DATA=True, uruchamiam symulator")
            from .simulate_consumer import start_simulator
            t = threading.Thread(target=start_simulator, daemon=True)
            t.start()
        else:
            print("[APPS] SIMULATE_DATA=False, uruchamiam Kafka consumer")
            from .kafka_consumer import start_kafka_consumer
            t = threading.Thread(target=start_kafka_consumer, daemon=True)
            t.start()
