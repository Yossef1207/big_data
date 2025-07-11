FROM python:3.12-slim

RUN apt-get update && \
    apt-get install -y \
        curl gnupg git build-essential \
        libblas-dev liblapack-dev libatlas-base-dev \
        ninja-build && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /workspace
COPY requirements.txt ./
RUN pip install --no-cache-dir --upgrade pip \
    && pip install --no-cache-dir -r requirements.txt

WORKDIR /workspace/backend
COPY backend/ ./

EXPOSE 8000
CMD ["daphne", "-b", "0.0.0.0", "-p", "8000", "--access-log", "-", "sentiment_backend.asgi:application"]
