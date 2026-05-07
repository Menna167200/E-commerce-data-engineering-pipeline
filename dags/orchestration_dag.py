from airflow.sdk import dag, task
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator

@dag
def orchestration_dag():

    trigger_ingest = TriggerDagRunOperator(
        task_id='trigger_ingest_dag',
        trigger_dag_id='ingest_dag',
        wait_for_completion=True
    )

    # In production this would be replaced with a sensor polling
    # MinIO for a _SUCCESS marker written by the consumer
    @task
    def wait_for_consumer():
        import time
        print('Waiting for Kafka consumer to finish processing chunks')
        
        time.sleep(900) # Wait for 15 minutes (adjust as needed)

    trigger_gold = TriggerDagRunOperator(
        task_id='trigger_gold_dag',
        trigger_dag_id='gold_dag',
        wait_for_completion=True
    )

    trigger_ingest >> wait_for_consumer() >> trigger_gold

orchestration_dag()