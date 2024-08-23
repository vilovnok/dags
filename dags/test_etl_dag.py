from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.bash import BashOperator


default_args={
    'owner': 'airflow',
    'retries': 3,
    'retry_delay': timedelta(seconds=5),       
}

with DAG(
    dag_id='system_dag',
    description='System DAG',
    default_args=default_args,
    start_date=datetime(2024, 8, 20),     
    schedule_interval=timedelta(seconds=10),

) as dag:
    
    task_1=BashOperator(
        task_id='task1',
        bash_command='echo Система стабильна'    
)
    task_1