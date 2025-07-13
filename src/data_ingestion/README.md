# Reddit Sentiment Analysis Pipeline

A sentiment analysis system for Reddit comments using Kafka and Flink.

## Architecture

```
Reddit Data (.zst) → Producer → Kafka → Comment Cleaning → Sentiment Labeling → Keyword Aggregation → Results
                                    ↓
                            Model Retraining (Continuous Learning)
```

### Processing Pipeline

1. **Raw Data Ingestion** - `raw_comm_producer.py` streams Reddit comments to Kafka
2. **Comment Cleaning** - `comment_cleaner_processor.py` preprocesses and cleans text
3. **Sentiment Labeling** - `sentiment_labeling_processor.py` applies ML models for sentiment classification
4. **Keyword Aggregation** - `keyword_aggregation_processor.py` aggregates sentiment by keywords
5. **Model Retraining** - `retraining.py` continuously updates models with new data
6. 
### Infrastructure (Docker)

- **Kafka** (port 9092) - Message streaming
- **Zookeeper** (port 2181) - Kafka coordination  
- **Flink JobManager** (port 8081) - Stream processing management
- **Flink TaskManager** - Stream processing workers
- **Kafdrop** (port 9021) - Kafka UI
- **Model Retrainer** - Continuous model retraining service consuming labeled comments
 
### Access Points

- **Flink Dashboard**: http://localhost:8081
- **Kafka UI (Kafdrop)**: http://localhost:9021

## Make Ensure

- Reddit data file: Ensure `data/RC_2019-04.zst` exist
- ML models: Ensure `data/sentiment_model.pkl`, `data/tfidf_vectorizer.pkl` exist 
- Model retraining: Ensure `/shared` volume permissions
- Hot-reload not working: Ensure `sentiment_labeling_processor.py` can access `/shared/sentiment_model.pkl`

## Kafka Topics

| Topic                     | Purpose                                |
|---------------------------|----------------------------------------|
| `raw-reddit-comments`     | Raw Reddit comment data from producer  |
| `cleaned-reddit-comments` | Preprocessed comments (cleaning stage) |
| `labeled-reddit-comments` | Comments with sentiment labels         |
| `keyword_requests`        | User keyword analysis requests         |
| `keyword_responses`       | Sentiment analysis results             |

## Quick Start

```bash
# Start all services in the main application directory
docker compose up -d
```

## Processing Stages

### Stage 1: Comment Cleaning (`comment_cleaner_processor.py`)
- **Input**: `raw-reddit-comments`
- **Output**: `cleaned-reddit-comments`
- **Functions**:
  - Text normalization and lowercasing
  - Slang expansion
  - Tokenization

### Stage 2: Sentiment Labeling (`sentiment_labeling_processor.py`)
- **Input**: `cleaned-reddit-comments`
- **Output**: `labeled-reddit-comments`
- **Functions**:
  - TF-IDF vectorization
  - ML model prediction (positive/negative/neutral)
  - Reloading of updated models on timer

### Stage 3: Keyword Aggregation (`keyword_aggregation_processor.py`)
- **Inputs**: 
  - `keyword_requests` (user queries)
  - `labeled-reddit-comments` (sentiment data)
- **Output**: `keyword_responses`
- **Functions**:
  - Real-time keyword matching
  - Sentiment score calculation
  - Windowed processing (1000 comments processed interval)

### Model Retraining Service (`retraining.py`)
- **Input**: `labeled-reddit-comments`
- **Output**: Updated models in `/shared/sentiment_model.pkl`
- **Functions**:
  - Continuous consumption of labeled sentiment data
  - Batch collection (400,000 samples per training cycle)
  - Logistic regression model retraining
  - Atomic model replacement for hot-reloading

## Configuration

### Producer Settings (`config/kafka_producer_config.py`)
- `DATA_PATH` - Path to Reddit data file
- `LIMIT_MESSAGES` - Max messages to process
- `MIN_BODY_LENGTH` - Minimum comment length
- `FIELDS_TO_KEEP` - Required comment fields

### Processing Settings
- **Window Size**: 1000 comments (keyword aggregation)
- **Model Reload**: Every 3000 seconds
- **Checkpoint Interval**: 30 seconds
- **Retraining Batch Size**: 400,000 labeled comments
- **Model Hot-Reload**: Automatic detection of updated models

## Cleanup

```bash
# Stop services
docker compose down

# Remove data volumes
docker compose down -v

# Clean up Docker images
docker system prune -a
```

# Contributors:

- **Veronika Anokhina:** Raw Reddit comment producer implementation, Kafka and Flink infrastructure setup, keyword aggregation logic, Docker Compose setup and Docker containerization for Kafka/Flink services
- **Sára Veselá** Integration of data cleaning and sentiment labeling processes into Flink streaming, a machine learning model retraining pipeline