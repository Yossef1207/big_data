FROM flink:1.20.0

RUN echo "Install python and PyFlink" \
    && apt-get update \
    && apt-get install -y python3 python3-pip python3-dev --no-install-recommends \
    && rm -rf /var/lib/apt/lists/* \
    && ln -s /usr/bin/python3 /usr/bin/python

# Python deps
COPY docker/requirements.txt /tmp/
RUN pip install --no-cache-dir -r /tmp/requirements.txt && pip cache purge

# Kafka connector
RUN mkdir -p /opt/flink/usrlib \
    && wget -q -P /opt/flink/usrlib \
       https://repo.maven.apache.org/maven2/org/apache/flink/flink-sql-connector-kafka/3.3.0-1.20/flink-sql-connector-kafka-3.3.0-1.20.jar

# Env vars
ENV PYTHONPATH=/opt/flink/opt/python:/opt/flink/usrlib \
    PYFLINK_CLIENT_EXECUTABLE=python3 \
    FLINK_HOME=/opt/flink \
    PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1

# Config + modele
RUN mkdir -p /data
COPY src/data_ingestion/config/ /opt/flink/usrlib/config/
COPY data/tfidf_vectorizer.pkl data/sentiment_model.pkl /data/

# NLTK
RUN mkdir -p /opt/flink/usrlib/nltk_data \
    && python -c "import nltk; nltk.download('punkt', download_dir='/opt/flink/usrlib/nltk_data'); nltk.download('stopwords', download_dir='/opt/flink/usrlib/nltk_data')" \
    && python - <<EOF
import nltk
nltk.data.path.append('/opt/flink/usrlib/nltk_data')
from nltk.tokenize import word_tokenize
print('NLTK verification successful')
EOF
ENV NLTK_DATA=/opt/flink/usrlib/nltk_data

# Flink jobs
COPY src/data_ingestion/raw_comm_processor.py /opt/flink/usrlib/
COPY src/data_ingestion/keyword_aggregation_processor.py /opt/flink/usrlib/

# Cleanup
RUN apt-get clean \
    && rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/* \
    && find /opt/flink/usrlib -name "*.pyc" -delete \
    && find /opt/flink/usrlib -name "__pycache__" -type d -exec rm -rf {} + || true

CMD ["jobmanager"]
