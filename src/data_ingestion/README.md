# Reddit Data Ingestion with Kafka and Flink

This project provides a complete streaming analytics pipeline for processing Reddit comments using Kafka, Flink, and Confluent Control Center.

## Architecture Overview

```
Reddit Data (.zst) → Producer → Kafka → Flink → Processed Stream
                                  ↓
                            Control Center
```

- **Producer**: Reads compressed Reddit comment files and streams to Kafka
- **Kafka**: Message broker for reliable data streaming
- **Flink**: Stream processing engine for real-time analytics
- **Zookeeper**: Kafka cluster coordination
- **Control Center**: Web interface for Kafka monitoring and management

### Component Details

```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   Zookeeper     │    │     Kafka       │    │ Control Center  │
│   Port: 2181    │←───│   Port: 9092    │←───│   Port: 9021    │
└─────────────────┘    │   Port: 29092   │    └─────────────────┘
                       └─────────────────┘
                              │
                              ▼
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│Reddit Producer  │───▶│  Kafka Topics   │───▶│ Flink Streaming │
│                 │    │                 │    │ JobManager:8081 │
└─────────────────┘    └─────────────────┘    └─────────────────┘
```

## Prerequisites

- Docker & Docker Compose
- Python 3.8+ (for local development)
- Reddit comment data file in Zstandard format (.zst) - `data/RC_2019-04.zst`
- `sentiment_model.pkl` file in `data/` directory
- `tfidf_vectorizer.pkl` file in `data/` directory

## Configuration

Configuration files are located in the `config/` directory:
- `data_config.py`: Data processing parameters and field filtering
- `kafka_producer_config.py`: Kafka producer settings

**Important**: When running Python scripts from outside Docker containers, use host ports (e.g., `localhost:29092` instead of `kafka:9092`).

## Quick Start

### 1. Start Docker Desktop

```bash
# Step 1: Start Docker Desktop
systemctl --user start docker-desktop
# Alternatively, launch Docker Desktop via your application menu

# Step 2: Switch to Docker Desktop CLI Context
docker context use desktop-linux

# Step 3: Check Docker access
docker ps
# If you get permissions error, prefix with sudo: sudo docker ps
```

### 2. Start the Infrastructure

```bash
# Start all services
docker compose up -d

# Check service status
docker compose ps

# View logs in real time
docker compose logs -f
```

### 3. Access Web Interfaces

- **Confluent Control Center**: http://localhost:9021
- **Flink Dashboard**: http://localhost:8081

## Services

### Core Infrastructure
- **Zookeeper** (`confluentinc/cp-zookeeper:7.9.1`)
  - Port: 2181
  - Kafka cluster coordination

- **Kafka Broker** (`confluentinc/cp-kafka:7.9.1`)
  - Internal Port: 9092 (inter-service communication)
  - External Port: 29092 (host access)
  - Message streaming platform

### Management & Monitoring
- **Confluent Control Center** (`confluentinc/cp-enterprise-control-center:latest`)
  - Port: 9021
  - Web UI for Kafka management and monitoring

### Data Processing
- **Reddit Producer** (Custom built)
  - Streams Reddit comments from compressed files
  - Runs automatically for 40 comments on startup
  - Memory limit: 512MB, CPU limit: 1 core

- **Flink JobManager** (Custom built)
  - Port: 8081 (Web UI)
  - Port: 6123 (RPC)
  - Automatically submits Flink jobs on startup

- **Flink TaskManager** (Custom built)
  - Stream processing workers
  - 1 task slot per worker

### Topic Management
- **Kafka Topic Initializer**
  - Automatically creates required topics on startup:
    - `raw-reddit-comments`
    - `labeled-reddit-comments`
    - `keyword_requests`
    - `keyword_responses`

## Available Topics

| Topic Name                | Partitions | Purpose                    |
|---------------------------|------------|----------------------------|
| `raw-reddit-comments`     | 1          | Raw Reddit comment data    |
| `labeled-reddit-comments` | 1          | Processed/labeled comments |
| `keyword_requests`        | 1          | Keyword analysis requests  |
| `keyword_responses`       | 1          | Keyword analysis results   |

## Producer Usage

### Automatic Execution
The Reddit producer runs automatically when Docker starts, processing 40 comments by default.

### Manual Execution
```bash
# Enter the producer container
docker compose exec reddit-producer bash

# Run with default settings
python raw_comm_producer.py

# Run with custom parameters
python raw_comm_producer.py \
  --data_path "/data/RC_2019-04.zst" \
  --message_limit 1000 \
  --speed_factor 2.0 \
  --debug
```

### Producer Parameters

| Parameter             | Default             | Description                                                   |
|-----------------------|---------------------|---------------------------------------------------------------|
| `--message_limit`     | 40                  | Maximum messages to process                                   |
| `--speed_factor`      | 1.0                 | Streaming speed multiplier (2.0 = 2x faster, 0.5 = 2x slower) |
| `--data_path`         | From config         | Path to .zst data file                                        |
| `--topic`             | raw-reddit-comments | Kafka topic name                                              |
| `--bootstrap_servers` | kafka:9092          | Kafka servers (use localhost:29092 from host)                 |
| `--debug`             | False               | Enable debug logging                                          |

## Monitoring & Management

### Control Center Features
- Topic overview and message browsing
- Consumer group monitoring
- Broker and partition metrics
- Real-time streaming analytics

### Flink Web UI Features
- Job status and execution metrics
- Task manager overview
- Job execution plans
- Checkpoint and savepoint management

### CLI Monitoring
```bash
# List topics
docker compose exec kafka kafka-topics --list --bootstrap-server localhost:9092

# Monitor topic messages
docker compose exec kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic raw-reddit-comments \
  --from-beginning

# Check consumer groups
docker compose exec kafka kafka-consumer-groups \
  --bootstrap-server localhost:9092 \
  --list
```

## Troubleshooting

### Common Issues

Rebuild just one container:
```docker compose up --build jobmanager```

**1. Flink Job Submission Failures**

**Outside the docker container run**
```bash
# Manually submit Flink job
docker exec jobmanager /opt/flink/bin/flink run \
  -py /opt/flink/usrlib/raw_comm_processor.py \
  --jarfile /opt/flink/usrlib/flink-sql-connector-kafka-3.3.0-1.20.jar
```

```bash
docker exec jobmanager
/opt/flink/bin/flink run \
  -py /raw_comm_processor.py \
  --jarfile /opt/flink/usrlib/flink-sql-connector-kafka-3.3.0-1.20.jar
```
**From docker container run**

The raw_comm_processor copied to the container after rebuild \
```bash
/opt/flink/bin/flink run \
  -py /opt/flink/usrlib/raw_comm_processor.py \
  --jarfile /opt/flink/usrlib/flink-sql-connector-kafka-3.3.0-1.20.jar
```

**The local file run \**
```bash
/opt/flink/bin/flink run \
  -py /raw_comm_processor.py \
  --jarfile /opt/flink/usrlib/flink-sql-connector-kafka-3.3.0-1.20.jar
```



#the file from pc not from docker

**2. Kafka Connection Issues**
```bash
# Check Kafka health
docker compose exec kafka kafka-broker-api-versions --bootstrap-server localhost:9092

# Verify topics exist
docker compose exec kafka kafka-topics --list --bootstrap-server localhost:9092
```

**3. Producer Connection Problems**
- From host: Use `localhost:29092`
- From containers: Use `kafka:9092`
- Ensure Kafka healthcheck passes before running producer

**4. Control Center Not Loading**
- Wait 2-3 minutes for initialization
- Check if Kafka is healthy first
- Verify port 9021 is accessible

### Service Health Checks
```bash
# Check all services
docker compose ps

# View specific service logs
docker compose logs kafka
docker compose logs control-center
docker compose logs jobmanager
docker compose logs reddit-producer

# Restart services
docker compose restart kafka
docker compose restart reddit-producer
```

## Data Flow

1. **Data Ingestion**: Reddit Producer reads compressed files and streams to `raw-reddit-comments`
2. **Stream Processing**: Flink jobs process raw data and output to `labeled-reddit-comments`
3. **Interactive Queries**: Keyword requests/responses enable real-time analysis
4. **Monitoring**: Control Center provides visibility into the entire pipeline

## Development

### Building Custom Images
```bash
# Build all custom images
docker compose build

# Build specific service
docker compose build reddit-producer
docker compose build jobmanager
```

### Adding New Topics
Edit the `kafka-init-topics` service in docker-compose.yml:
```bash
kafka-topics --create --topic new-topic-name \
  --partitions 1 --replication-factor 1 \
  --if-not-exists --bootstrap-server kafka:9092
```

### Resource Configuration
Current limits:
- **Reddit Producer**: 512MB RAM, 1 CPU core
- **Other services**: Docker defaults

## Cleanup

```bash
# Graceful shutdown
docker compose down

# Remove volumes (WARNING: destroys data)
docker compose down -v

# Complete cleanup
docker compose down -v --remove-orphans
docker system prune
```

## Scaling

### Horizontal Scaling
```bash
# Scale task managers
docker compose up -d --scale taskmanager=3

# Scale producers (requires topic partitioning)
docker compose up -d --scale reddit-producer=2
```

Note: Topics are created automatically upon startup. Flink jobs are automatically submitted when the infrastructure starts. The comment producer runs for 40 comments by default when Docker containers start.


**Author**: Veronika Anokhina
**Date**: 08.06.2025
**Version**: 1.0