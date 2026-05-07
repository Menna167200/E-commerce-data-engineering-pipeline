E-Commerce Data Engineering Pipeline

An end-to-end real-time batch/stream hybrid Data Engineering project built using:

Apache Airflow
Apache Spark
Apache Kafka
MinIO (S3-compatible object storage)
Delta Lake
Docker Compose

The project ingests large-scale e-commerce behavioral data, processes it through Bronze/Silver/Gold layers, and orchestrates the entire workflow using Airflow.

Dataset used:
E-Commerce Behavior Data from Multi Category Store (Kaggle)

Tested on:

October dataset only
First 5 million rows
Architecture
Pipeline Flow
CSV Dataset
    ↓
Airflow Ingestion DAG
    ↓
Chunking Large CSV Files
    ↓
Upload Chunks to MinIO (Bronze)
    ↓
Kafka Producer sends ONLY references
    ↓
Kafka Consumer receives references
    ↓
Spark Cleaning & Transformation
    ↓
Delta Lake Silver Layer
    ↓
Gold Transformations
    ↓
Analytics-ready Delta Tables
Tech Stack
Tool	Purpose
Airflow	Workflow orchestration
Kafka	Event-driven triggering
Spark	Distributed processing
MinIO	S3-compatible object storage
Delta Lake	ACID tables + incremental processing
Docker Compose	Local infrastructure
PostgreSQL	Airflow metadata DB
Redis	Celery broker
Data Layers
Bronze Layer
Raw parquet chunks stored in MinIO
Silver Layer

Cleaned and standardized Delta tables:

deduplication
schema casting
missing value handling
median imputation
invalid value cleanup
Gold Layer
fact_purchases

Purchase-level fact table

dim_users

User KPIs:

total spent
average order value
purchase history
dim_products

Product metrics:

average price
total purchases
agg_daily_metrics

Daily business metrics:

revenue
buyers
orders
items sold
Key Engineering Challenges & Lessons Learned
1. Kafka Message Size Problem → Claim Check Pattern
Problem

Initially, I attempted to push large datasets directly through Kafka.

This quickly exposed an important distributed systems limitation:

Kafka is designed for small event messages, not large-scale data transfer.

Large payloads can:

exceed broker limits
reduce throughput
increase memory pressure
create network bottlenecks
Solution

I implemented the Claim Check Pattern (Reference-Based Messaging).

Instead of sending the data itself:

Data chunks are uploaded to MinIO
Kafka only sends a lightweight reference/event message

Example:

{
  "file_path": "s3a://ecommerce-lake/raw/chunk_0001.parquet",
  "rows": 500000
}

The consumer:

receives the reference
fetches the actual data from MinIO
triggers Spark processing

This design is similar to patterns used in large-scale streaming systems like Netflix-style architectures.

References
Claim Check Pattern (Dev.to)
Reference-Based Messaging
Claim Check Design Pattern
Kafka Broker Tuning
2. Spark + MinIO Integration

One of the trickiest infrastructure challenges was configuring:

Spark
S3A
MinIO
Delta Lake
Docker networking

Key learnings:

correct Hadoop AWS jars matter
S3 endpoint configuration matters
path-style access is required
executor/driver environments must match

Excellent resource:

Setting up AWS S3 (MinIO) and Spark using Docker Compose
3. Airflow Is an Orchestrator — Not a Streaming Consumer

A major lesson learned:

Do NOT use Airflow for infinite-running loops like Kafka consumers.

Airflow should orchestrate workflows, not host long-lived streaming services.

Correct separation:

Kafka consumer runs as an independent service
Airflow triggers batch stages and dependencies
4. Docker Rebuilds Matter

Another practical lesson:

Always rebuild Docker images after dependency or environment changes.

Especially when changing:

Java versions
Spark dependencies
Python packages
Hadoop jars

Use:

docker compose build --no-cache

when debugging infrastructure problems.

Workflow Orchestration
DAGs
ingest_dag
Reads CSV in chunks
Uploads chunks to MinIO
Publishes Kafka reference events
orchestration_dag

Coordinates:

ingestion
waiting phase
gold transformations
gold_dag

Runs Spark transformations for Gold tables.

Current Portfolio Simplification

The current orchestration uses a simple waiting task:

time.sleep(1200)

For portfolio/demo purposes this is acceptable.

In production, this would be replaced with:

sensors
completion markers
row-count validation
event-based orchestration
Running the Project
Start Infrastructure
docker compose up --build
Airflow UI
http://localhost:8080
MinIO Console
http://localhost:9001
Spark Master UI
http://localhost:9090
Project Highlights
Real-time event-driven architecture
Delta Lake medallion architecture
Kafka + Spark integration
Reference-based messaging pattern
Distributed processing
Airflow orchestration
Dockerized infrastructure
Resources & Learning References
Airflow
Airflow Tutorial by Ansh
Airflow DAG Best Practices
Kafka
Kafka Tutorial 1 - TechWorld with Nana
Kafka Tutorial 2 - TechWorld with Nana
Spark + MinIO
Spark + MinIO Setup Guide