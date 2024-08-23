from airflow import DAG
from datetime import datetime, timedelta
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor



default_args={
    'owner': 'airflow',
    'retries': 3,
    'retry_delay': timedelta(seconds=5),       
}

with DAG(
    dag_id='minio_dag_v1',
    description='System DAG',
    default_args=default_args,
    start_date=datetime(2024, 8, 21),     
    schedule_interval=timedelta(hours=10),

) as dag:
    task_1 = S3KeySensor(
        task_id='minio_s3',
        bucket_name='airflow',
        bucket_key='data.csv',
        aws_conn_id='minio_conn',
        mode='poke',
        poke_interval=5,
        timeout=30
    )
    pass