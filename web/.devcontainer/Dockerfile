FROM python:3.13-slim

# Installations
RUN apt-get update && \
    apt-get install -y \
        curl \
        gnupg \
        git \
        gcc \
        tree \
    && curl -fsSL https://deb.nodesource.com/setup_20.x | bash - \
    && apt-get install -y nodejs \
    && apt-get clean && rm -rf /var/lib/apt/lists/*

# Set workspace directory
WORKDIR /workspace

# Python requirements
COPY requirements.txt .
RUN pip install --upgrade pip && pip install -r requirements.txt

# Frontend setup
COPY frontend/package*.json frontend/
RUN cd frontend && npm install

# Expose ports
EXPOSE 8000 5173

# Run servers