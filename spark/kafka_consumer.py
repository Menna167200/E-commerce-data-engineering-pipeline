import os
import json
import subprocess
import tempfile
from confluent_kafka import Consumer, KafkaException

SPARK_HOME = os.getenv('SPARK_HOME')
JAVA_HOME_SPARK = os.getenv('JAVA_HOME_SPARK')

env = os.environ.copy()
env['PATH'] = f"{JAVA_HOME_SPARK}/bin:{SPARK_HOME}/bin:" + env.get('PATH', '')

consumer = Consumer({
    'bootstrap.servers': 'kafka:29092',
    'group.id': 'consumer-group',
    'auto.offset.reset': 'earliest',
    'enable.auto.commit': False
})

consumer.subscribe(['ecomm-chunks'])

try:
    while True:
        msg = consumer.poll(1.0)

        if msg is None:
            continue

        if msg.error():
            raise KafkaException(msg.error())

        ref = json.loads(msg.value().decode())
        print(f'Received file at: {ref["file_path"]} with {ref["rows"]} rows')

        # temp file
        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as ref_file:
            json.dump(ref, ref_file)
            ref_path = ref_file.name

        cmd = [
            f'{SPARK_HOME}/bin/spark-submit',
            '--master', 'spark://spark-master:7077',
            '--deploy-mode', 'client',

            '--conf', 'spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension',
            '--conf', 'spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog',

            '--conf', 'spark.hadoop.fs.s3a.endpoint=http://minio:9000',
            '--conf', 'spark.hadoop.fs.s3a.access.key=minioadmin',
            '--conf', 'spark.hadoop.fs.s3a.secret.key=minioadmin',
            '--conf', 'spark.hadoop.fs.s3a.path.style.access=true',
            '--conf', 'spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem',
            '--conf', 'spark.hadoop.fs.s3a.connection.ssl.enabled=false',

            '--conf', f'spark.executorEnv.JAVA_HOME_SPARK={JAVA_HOME_SPARK}',
            '--conf', f'spark.driverEnv.JAVA_HOME_SPARK={JAVA_HOME_SPARK}',

            '/opt/spark-apps/clean_transform.py',
            ref_path
        ]

        result = subprocess.run(cmd, env=env, capture_output=True, text=True)

        os.unlink(ref_path)

        if result.returncode == 0:
            print(f'Spark job succeeded pulling the chunk at {ref["file_path"]}')
            consumer.commit(msg)

        else:
            print(f'Spark job failed pulling the chunk at {ref["file_path"]}')
            print(f'Error: {result.stderr}')


except KeyboardInterrupt:
    print("Stopped")

finally:
    consumer.close()