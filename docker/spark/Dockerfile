FROM apache/spark:3.5.8-java17-python3

USER root


ENV HOME=/root
ENV PIP_CACHE_DIR=/tmp/pip-cache

RUN apt-get update && apt-get install -y curl && apt-get clean 

RUN pip install confluent-kafka boto3 pandas pyarrow requests minio pyspark delta-spark

# Hadoop AWS support for S3/MinIO
RUN curl -L -o /opt/spark/jars/hadoop-aws.jar \
    https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar

# AWS SDK for S3 
RUN curl -L -o /opt/spark/jars/aws-java-sdk-bundle.jar \
    https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar


# Delta Lake support for Spark
RUN curl -L -o /opt/spark/jars/delta-spark_2.12-3.2.0.jar \
    https://repo1.maven.org/maven2/io/delta/delta-spark_2.12/3.2.0/delta-spark_2.12-3.2.0.jar

RUN curl -L -o /opt/spark/jars/delta-storage-3.2.0.jar \
    https://repo1.maven.org/maven2/io/delta/delta-storage/3.2.0/delta-storage-3.2.0.jar

USER spark