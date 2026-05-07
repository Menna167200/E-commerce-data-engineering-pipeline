from airflow.sdk import dag, task
import subprocess
import os

@dag
def gold_dag():

    @task
    def golden_transformation():
        SPARK_HOME = os.getenv('SPARK_HOME')
        JAVA_HOME_AIRFLOW = os.getenv('JAVA_HOME_AIRFLOW')
        env = os.environ.copy()
        env['PATH'] = f"{JAVA_HOME_AIRFLOW}/bin:{SPARK_HOME}/bin:" + env.get('PATH', '')

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

            '--conf', f'spark.executorEnv.JAVA_HOME_AIRFLOW={JAVA_HOME_AIRFLOW}',
            '--conf', f'spark.driverEnv.JAVA_HOME_AIRFLOW={JAVA_HOME_AIRFLOW}',

            '/opt/spark-apps/golden_transformation.py'
        ]

        result = subprocess.run(cmd, env=env, capture_output=True, text=True)

        print('STDOUT:\n', result.stdout)
        print('STDERR:\n', result.stderr)

        if result.returncode != 0:
            raise Exception('Spark job failed')

    golden_transformation()


gold_dag()