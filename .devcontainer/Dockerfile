FROM python:3.12-slim

RUN apt-get update && \
    apt-get install -y \
      curl \
      gnupg \
      git \
      build-essential \
      gfortran \
      libblas-dev \
      liblapack-dev \
      libatlas-base-dev \
      ninja-build \
      nodejs \
      npm \
    && apt-get clean && rm -rf /var/lib/apt/lists/*

WORKDIR /workspace
COPY requirements.txt .
RUN pip install --upgrade pip && pip install -r requirements.txt

COPY frontend/package*.json frontend/
RUN cd frontend && npm install

EXPOSE 8000 5173
