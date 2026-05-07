# 🚀 End-to-End E-Commerce Data Engineering Pipeline

An **end-to-end, production-style Data Engineering pipeline** built using modern distributed systems tools:

- ⚡ Apache Airflow (Orchestration)
- ⚡ Apache Spark (Distributed Processing)
- ⚡ Apache Kafka (Event Streaming)
- ⚡ MinIO (S3-Compatible Storage)
- ⚡ Delta Lake (Lakehouse Architecture)
- ⚡ Docker Compose (Infrastructure)

---

# 📊 Project Overview

This project processes **millions of e-commerce behavior records** and transforms them into analytics-ready datasets using a **Medallion Architecture (Bronze → Silver → Gold)**.

### 📦 Dataset
Used dataset:
https://www.kaggle.com/datasets/mkechinov/ecommerce-behavior-data-from-multi-category-store

✔ Used only **October data**  
✔ Tested on **~5 million rows**

---

# 🧠 Architecture Overview

```
CSV Dataset
   ↓
Airflow Ingestion DAG
   ↓
Chunking Large Data
   ↓
MinIO (Bronze Layer Storage)
   ↓
Kafka (Reference Messages Only)
   ↓
Kafka Consumer
   ↓
Spark Cleaning & Transformation
   ↓
Delta Lake (Silver Layer)
   ↓
Gold Layer Aggregations
```

---

# 🏗️ Data Architecture (Medallion Model)

## 🟤 Bronze Layer
- Raw chunked parquet files
- Stored in **MinIO (S3-compatible storage)**

## ⚪ Silver Layer
Cleaned & standardized dataset:
- Deduplication
- Schema casting
- Missing value handling
- Median imputation
- Event filtering

## 🟡 Gold Layer
Business-ready analytics tables:

- `fact_purchases`
- `dim_users`
- `dim_products`
- `agg_daily_metrics`

---

# ⚠️ Key Engineering Challenges (REAL WORLD PROBLEMS)

---

## 🚨 1. Kafka Message Size Limitation → SOLVED using Claim Check Pattern

### ❌ Problem:
Kafka **cannot handle large data payloads efficiently**

- Message size limits exceeded
- Broker instability
- Memory pressure issues

---

### ✅ Solution: Claim Check Pattern (Netflix-style architecture)

Instead of sending data:

❌ WRONG:
```
Kafka → large dataset
```

✔ CORRECT:
```
Kafka → sends reference only
MinIO → stores actual data
```

Example message:
```json
{
  "file_path": "s3a://ecommerce-lake/raw/chunk_0001.parquet",
  "rows": 500000
}
```

👉 Consumer fetches data from MinIO using reference.

📌 Key Insight:
> **Kafka is a trigger system, not a data warehouse.**

---

## 🚨 2. Spark + MinIO Integration Complexity

### Challenges:
- S3A configuration issues
- Hadoop AWS dependency conflicts
- Docker networking issues
- Java + Spark environment mismatches

### Solution:
- Proper S3 endpoint configuration
- Path-style access enabled
- Matching executor/driver environments

---

## 🚨 3. Airflow Misuse (Important Architecture Lesson)

### ❌ Mistake:
Using Airflow for:
- infinite Kafka consumer loops

### ✅ Correct Design:
- Airflow = orchestration only
- Kafka consumer = independent service

📌 Key Rule:
> **Airflow is NOT a streaming engine**

---

## 🚨 4. Docker & Dependency Issues

### Key lesson:
Always rebuild images after changes:

```bash
docker compose build --no-cache
```

---

# 🌐 Services

| Service | URL |
|--------|-----|
| Airflow UI | http://localhost:8080 |
| MinIO Console | http://localhost:9001 |
| Spark Master | http://localhost:9090 |
| Kafka Broker | http://localhost:9092 |

---

# 🧪 DAGs Overview

## 📥 ingest_dag
- Reads CSV in chunks
- Uploads to MinIO
- Sends Kafka reference events

---

## 🏆 gold_dag
- Runs Spark transformations
- Creates analytics tables

---

## 🔁 orchestration_dag
- Triggers ingestion
- Waits for processing
- Triggers Gold layer

---

# 🧱 Tech Stack

- **Airflow** → Workflow orchestration  
- **Spark** → Distributed processing  
- **Kafka** → Event streaming  
- **MinIO** → Object storage (S3-compatible)  
- **Delta Lake** → ACID tables  
- **Docker** → Full infrastructure setup  

---

# 📌 Key Learnings

✔ Distributed systems design  
✔ Event-driven architecture  
✔ Medallion data architecture  
✔ Claim Check pattern  
✔ Spark + S3 integration  
✔ Real-world Airflow orchestration  

---

# 📚 References & Learning Resources

- Airflow Best Practices  
https://airflow.apache.org/docs/apache-airflow/3.1.8/best-practices.html#top-level-python-code  

- Kafka Tutorials (TechWorld with Nana)  
https://youtu.be/QkdkLdMBuL0  
https://youtu.be/B7CwU_tNYIE  

- Airflow Tutorial (Ansh Lamba)  
https://youtu.be/IiczxlbQb8s

- Docker Tutorial (Ansh Lamba)
https://youtu.be/nAHx_uSBfTg

- MinIO + Spark Setup  
https://medium.com/@dkalouris/setting-up-aws-s3-minio-and-spark-using-docker-compose-6a22ef26c6b0  

- Claim Check Pattern  
https://dev.to/willvelida/the-claim-check-pattern-reference-based-messaging-5gn7  
https://medium.com/@dmosyan/claim-check-design-pattern-603dc1f3796d  

---

# ⭐ Final Note

This project demonstrates a **real-world distributed data engineering pipeline** with:

- scalable ingestion
- event-driven architecture
- lakehouse design
- production-style orchestration

---
