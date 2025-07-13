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
│── terraform/
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

## Cloud Infrastructure Access & Deployment

The cloud infrastructure is fully provisioned and ready for use. To access the deployed environment, please follow these steps:

1. **Connect to the Azure Virtual Machine**

   You will need the private SSH key and the public IP address, **which can be obtained from the development team via Email**. Use the following command to establish an SSH connection:

   ```sh
   ssh -i ./.ssh/id_rsa azureuser@<ip_address> 
   ssh -i ./.ssh/id_rsa azureuser@52.224.0.84 # current command with the correct IP address
   ```

   > **Note:** Replace `<ip_address>` with the actual public IP of the VM. The IP-Address might change, but right now it is ``52.224.0.84``.

2. **Start the Docker Services**

   Once connected to the VM, initialize all required containers by running:

   ```sh
   docker compose up -d --build
   ```

This will launch all necessary services, including Kafka, Flink, and supporting components, as defined in the Docker Compose configuration.

3. **Call the service**:
   to call the service call the following link ``http://52.224.0.84:5173`` in the browser and type 2 keywords as described in [here](./frontend/README.md). The sentiment analysis results will be displayed as a graph, with timestamps on the x-axis and sentiment levels on the y.

## Description

## Installation

## Usage

## Authors and acknowledgment

## License
For open source projects, say how it is licensed.
