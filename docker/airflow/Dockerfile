FROM apache/airflow:3.1.8

USER root

RUN apt-get update && apt-get install -y \
    gcc \
    librdkafka-dev \
    curl \
    openjdk-17-jdk \
    && apt-get clean

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH="$JAVA_HOME/bin:$PATH"

USER airflow


RUN pip install confluent-kafka boto3 pandas pyarrow requests minio pyspark 