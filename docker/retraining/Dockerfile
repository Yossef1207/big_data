# docker/retrainer/Dockerfile
FROM python:3.9-slim

WORKDIR /app

RUN apt-get update && apt-get install -y \
    python3 \
    python3-pip \
    python3-dev \
    --no-install-recommends && \
    rm -rf /var/lib/apt/lists/*

COPY docker/retraining/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt && \
    pip cache purge

RUN mkdir -p /app/data
COPY src/data_ingestion/retraining.py .
COPY data/sentiment_model.pkl /app/data/
COPY data/tfidf_vectorizer.pkl /app/data/

CMD ["python", "retraining.py"]