import json
import io
import os
from datetime import datetime
from airflow.sdk import dag, task

CHUNK_SIZE = 500000

@dag
def ingest_dag():
    @task
    def RBM(bucket='ecommerce-lake'):
        import boto3
        import pandas as pd
        from confluent_kafka import Producer

        file_path = os.getenv('OCT_DATA_PATH')

        s3 = boto3.client(
            's3',
            endpoint_url='http://minio:9000',
            aws_access_key_id='minioadmin',
            aws_secret_access_key='minioadmin'
        )
        
        try:
            s3.head_bucket(Bucket=bucket)
            print(f'Bucket {bucket} already exists on MinIO')

        except s3.exceptions.ClientError:
            s3.create_bucket(Bucket=bucket)
            print(f'Created bucket: {bucket} on MinIO')

        producer = Producer({
            'bootstrap.servers': 'kafka:29092',
        })
  
    
        for i, chunk in enumerate(pd.read_csv(file_path, chunksize=CHUNK_SIZE)):
            if i >= 10: # Process only the first 10 chunks for testing = 5M rows
                print('Processed 10 chunks, stopping ingestion for testing purposes.')
                break
            new_path = f'raw/chunk_{i+1:04d}.parquet'

            buffer = io.BytesIO()
            chunk.to_parquet(buffer, index=False)
            buffer.seek(0)

            s3.upload_fileobj(buffer, bucket, new_path)

            print(f'Uploaded chunk {i+1} at {new_path}')


            event = {
                'event_type': 'chunk_uploaded',
                'file_path': f's3a://{bucket}/{new_path}',
                'rows': len(chunk),
                'timestamp': datetime.now().isoformat()
            }

            producer.produce(
                'ecomm-chunks',
                key=f'chunk_{i+1}',
                value=json.dumps(event).encode('utf-8'),
                callback=lambda err,msg: print(f'Triggering message sent successfully to {msg.topic()}') if err is None else print(f'Error sending triggering message: {err}')
            )
            producer.poll(0)
            
                
        producer.flush()
        return 'Ingestion is completed'

    RBM()

ingest_dag()