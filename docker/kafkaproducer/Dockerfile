FROM python:3.9-slim

WORKDIR /app

# 1) System deps
RUN apt-get update \
    && apt-get install -y librdkafka1 libzstd1 \
    && rm -rf /var/lib/apt/lists/*

# 2) Python deps
COPY docker/kafkaproducer/requirements.txt .
RUN pip install --upgrade pip \
    && pip install --no-cache-dir -r requirements.txt

# 3) App
COPY src/data_ingestion/raw_comm_producer.py .
COPY src/data_ingestion/config/kafka_producer_config.py ./config/

# 4) Verify
RUN python -c "import confluent_kafka, zstandard"

CMD ["python", "-u", "raw_comm_producer.py"]
