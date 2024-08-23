from airflow import DAG
from datetime import datetime, timedelta
from airflow.providers.postgres.operators.postgres import PostgresOperator



default_args={
    'owner': 'airflow',
    'retries': 3,
    'retry_delay': timedelta(seconds=5),       
}

with DAG(
    dag_id='postgres_dag',
    description='DB connect to DAG',
    default_args=default_args,
    start_date=datetime(2024, 8, 21),     
    schedule_interval=timedelta(hours=1),

) as dag:
    
    task_1=PostgresOperator(
        task_id='create_postgres_table',
        postgres_conn_id='postgres_conn',
        sql="""CREATE TABLE IF NOT EXISTS spd (id SERIAL PRIMARY KEY, data TEXT);"""
)
    task_1
