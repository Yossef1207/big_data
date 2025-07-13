import json
import os
import joblib
import numpy as np
import logging
import gc
from kafka import KafkaConsumer
from sklearn.linear_model import LogisticRegression
from joblib import load
import time
import tempfile

from pyflink.common import SimpleStringSchema, WatermarkStrategy
from pyflink.datastream import StreamExecutionEnvironment, RuntimeExecutionMode, MapFunction
from pyflink.datastream.connectors.kafka import KafkaSource, KafkaOffsetsInitializer
from pyflink.datastream.connectors.kafka import KafkaSink, DeliveryGuarantee
from pyflink.datastream.connectors.kafka import KafkaRecordSerializationSchema
from pyflink.common.typeinfo import Types

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

TOPIC = 'labeled-reddit-comments'
BOOTSTRAP_SERVERS = "kafka:9092"
BATCH_SIZE = 50_000
MODEL_PATH = '/data/sentiment_model.pkl'
LABEL_MAP = {'negative': 0, 'neutral': 1, 'positive': 2}
SAVING_PATH = "/shared/sentiment_model.pkl"

# Global model cache to avoid loading multiple times
MODEL_CACHE = {}

def get_model(model_type: str):
    """Lazy loading of models with caching to reduce memory usage."""
    if model_type not in MODEL_CACHE:

        base_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data")

        if model_type == "vectorizer":

            MODEL_CACHE[model_type] = load(os.path.join(base_path, "tfidf_vectorizer.pkl"))
            print(base_path)
        elif model_type == "sentiment":
            MODEL_CACHE[model_type] = load(os.path.join(base_path, "sentiment_model.pkl"))

        print("Loaded in cache")
        logger.info(f"Loaded {model_type} model into cache")

    return MODEL_CACHE[model_type]

def main():
    print("Starting retraining script...")
    time.sleep(40)

    print("Loading vectorizer...")
    try:
        vectorizer = get_model("vectorizer")
        #vectorizer = load(os.path.join("/app/data/", "tfidf_vectorizer.pkl"))
        print("vectorizer loaded")
    except Exception as e:
        print(e, flush=True)
    print(f"Starting Kafka consumer on topic '{TOPIC}'...")
    consumer = KafkaConsumer(
        TOPIC,
        bootstrap_servers=BOOTSTRAP_SERVERS,
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        group_id='retrainer-group',
        value_deserializer=lambda m: json.loads(m.decode('utf-8')),
    )
    texts = []
    labels = []
    while True:
        try:
            print("Polling Kafka topic for new messages...")
            raw_messages = consumer.poll(timeout_ms=5000)
            msg_count = 0

            for tp, messages in raw_messages.items():
                for message in messages:
                    obj = message.value
                    text = obj.get("cleaned_body", "")
                    label_str = obj.get("label", "")

                    if label_str not in LABEL_MAP:
                        continue

                    texts.append(text)
                    labels.append(LABEL_MAP[label_str])
                    msg_count += 1

            print(f"Received {msg_count} new messages. Current total: {len(texts)}")

            if len(texts) >= BATCH_SIZE:
                print(f"Collected {len(texts)} samples. Vectorizing and training...")
                X = vectorizer.transform(texts)
                y = np.array(labels)

                model = LogisticRegression(max_iter=300, multi_class='multinomial', solver='lbfgs')
                model.fit(X, y)

                print(f"Safe saving model to {SAVING_PATH}")
                #joblib.dump(model, SAVING_PATH)
                # Define temp file in same directory
                model_dir = os.path.dirname(SAVING_PATH)
                with tempfile.NamedTemporaryFile(dir=model_dir, delete=False) as tmp:
                    temp_path = tmp.name
                # Save the model to the temp path
                joblib.dump(model, temp_path)
                # Set permissions before the atomic move
                os.chmod(temp_path, 0o644)  # Make it readable by all
                # Atomically replace the old model
                os.replace(temp_path, "/shared/sentiment_model.pkl")
                # Ensure final file has correct permissions
                os.chmod("/shared/sentiment_model.pkl", 0o644)

                print("Model retraining complete.")
                texts.clear()
                labels.clear()
                gc.collect()

            time.sleep(10)

        except Exception as e:
            print("ERROR during Kafka loop:")
            print(traceback.format_exc())
            time.sleep(10)  # Retry later


if __name__ == '__main__':
    try:
        main()
    except Exception as e:
        print(f"ERROR in main(): {e}", flush=True)
        import traceback

        traceback.print_exc()
