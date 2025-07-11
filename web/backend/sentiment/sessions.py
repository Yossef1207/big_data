# sentiment/sessions.py

"""Registry of active sessions; we store NOTHING in the database.
Each user_id corresponds to a separate thread sending a request to Kafka every 60 seconds."""

from threading import Thread, Event
import time
from django.conf import settings
from .kafka_producer import send_to_kafka

# We store data: { user_id: { keyword1, keyword2, stop_event, thread } }
sessions: dict[str, dict] = {}


def start_session(user_id: str, keyword1: str, keyword2: str):
    # If there was an old session, stop it:
    if user_id in sessions:
        stop_existing_session(user_id)

    stop_event = Event()

    def _poll():
        while not stop_event.is_set():
            if not getattr(settings, "SIMULATE_DATA", False):
                payload = {
                    "user_id": user_id,
                    "keyword1": keyword1,
                    "keyword2": keyword2,
                    "request_time": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
                }
                # We send a request to the "keyword_requests" topic
                send_to_kafka("keyword_requests", payload)

            # Wait 60s or exit earlier if stop_event is set
            stop_event.wait(60.0)

    t = Thread(target=_poll, daemon=True)
    sessions[user_id] = {
        "keyword1": keyword1,
        "keyword2": keyword2,
        "stop_event": stop_event,
        "thread": t,
    }
    t.start()


def stop_existing_session(user_id: str):
    info = sessions.get(user_id)
    if not info:
        return
    info["stop_event"].set()
    sessions.pop(user_id, None)
