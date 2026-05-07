# Processing Event Streams

Real-time Wikipedia page-creation event pipeline using Kafka, Spark Structured Streaming, and Cassandra

## Architecture

```
Wikimedia SSE  →  [Generator]  →  Kafka (input)
                                      ↓
                               Spark Job 1 (transform)
                                      ↓
                               Kafka (processed)
                                      ↓
                               Spark Job 2 (persist)
                                      ↓
                               Cassandra (wiki_stream.page_creates)
```

## Project Structure

```
├── cassandra/
│   └── init.cql                         # Keyspace & table schema
├── generator/
│   ├── wiki_to_kafka.py                 # Reads Wikimedia SSE stream, publishes to Kafka input topic
│   └── Dockerfile                       # Builds the generator image
├── spark-apps/
│   ├── stream_input_to_processed.py     # Spark Job 1: parse raw events, write to processed topic
│   └── stream_processed_to_cassandra.py # Spark Job 2: read processed topic, write to Cassandra
├── docker-compose.kafka.yml             # Kafka (KRaft mode) + topic init + Kafka UI
├── docker-compose.cassandra.yml         # Cassandra + keyspace/table init
└── docker-compose.spark.yml             # Spark master + worker
```

## How to Run

### 1. Create shared Docker network

```bash
docker network create hw10-stream-net
```

### 2. Start infrastructure

```bash
docker compose -f docker-compose.kafka.yml up -d
docker compose -f docker-compose.cassandra.yml up -d
docker compose -f docker-compose.spark.yml up -d
```

### 3. Build and start the generator

```bash
docker build -t hw10-wiki-generator ./generator

docker run -d \
  --name hw10-wiki-generator \
  --network hw10-stream-net \
  -e KAFKA_BOOTSTRAP_SERVERS=kafka:9092 \
  -e KAFKA_TOPIC=input \
  hw10-wiki-generator
```

### 4. Run Spark Job 1 — input → processed

```bash
docker exec -it hw10-spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --conf spark.jars.ivy=/tmp/.ivy2 \
  --conf spark.executor.memory=1g \
  --conf spark.driver.memory=1g \
  --conf spark.sql.shuffle.partitions=1 \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.5 \
  /opt/spark-apps/stream_input_to_processed.py
```

### 5. Run Spark Job 2 — processed → Cassandra

```bash
docker exec -it hw10-spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --conf spark.jars.ivy=/tmp/.ivy2 \
  --conf spark.executor.memory=1g \
  --conf spark.driver.memory=1g \
  --conf spark.sql.shuffle.partitions=1 \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.5,com.datastax.spark:spark-cassandra-connector_2.12:3.5.0 \
  /opt/spark-apps/stream_processed_to_cassandra.py
```

## Verify

**Kafka topics:**

```bash
# Raw input events
docker compose -f docker-compose.kafka.yml exec kafka \
  /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server kafka:9092 --topic input --from-beginning

# Processed events
docker compose -f docker-compose.kafka.yml exec kafka \
  /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server kafka:9092 --topic processed --from-beginning
```

**Cassandra:**

```bash
docker compose -f docker-compose.cassandra.yml exec cassandra cqlsh
```

```sql
USE wiki_stream;
SELECT * FROM page_creates LIMIT 20;
```

## Screenshots

`kafka_topics_output.png` - Kafka topic content(left side is unproccesed, right side is processed)
`cassandra_data_hw10.png` Cassandra rows written by the pipeline
```
