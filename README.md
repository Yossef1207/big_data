# BD25_Project_M8_A

## Name
Sentiment Analysis of Reddit comments

## Code structure

```angular2html
project-root/
├── src/                        # Core application logic
│   ├── data_ingestion/         # Kafka producer and data ingestion logic
│   ├── stream_processing/      # Flink stream processing and preprocessing
│   ├── model/                  # Model training and inference
│   └── web_interface/          # Web interface for displaying results
│
├── datasets/                   #Reddit comment data
│
├── docker/         #Dockerfiles
│
└── README.md           # Project documentation

```

## Kafka and Flink Local Setup
This Docker Compose configuration creates a local streaming pipeline with Kafka and Apache Flink.
### Core Components

1. **Kafka Cluster**
   - Single broker accessible at `kafka:9092` (container) and `localhost:29092` (host)
   - Managed by Zookeeper
   - Includes pre-created topic: `raw-reddit-comments`
   - Kafka-UI available at http://localhost:8080

2. **Flink Processing**
   - JobManager with web UI at http://localhost:8081
   - TaskManager for execution
   - Automatically deploys Python streaming job (`raw_comm_processor.py`)
   - Includes necessary Kafka connectors

## Getting Started
   
## Description

## Installation

## Usage

## Authors and acknowledgment

## License
For open source projects, say how it is licensed.
