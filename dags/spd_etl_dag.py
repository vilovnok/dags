from datetime import datetime, timedelta
from airflow.decorators import dag, task
import time


default_args={
    'owner': 'r1char9', 
    'retries': 5,
    'retry_delay': timedelta(minutes=2)
}

@dag(
    dag_id='spd',
    description='ETL system for SPD',
    start_date=datetime(2024, 8, 20),     
    schedule_interval=timedelta(seconds=5),
    default_args=default_args,
    catchup=False
)
def spd_etl():
    
    @task()
    def get_data():
        print('данные получены')
        return 'данные получены'
    
    @task()
    def process_data_step1(data):
        print(f'Шаг 1 обработки данных: {data}')
        return 'данные обработаны на шаге 1'

    @task()
    def process_data_step2(data):
        print(f'Шаг 2 обработки данных: {data}')
        return 'данные обработаны на шаге 2'

    @task()
    def save_data(data):
        print(f'данные сохранены: {data}')        

    data=get_data()    
    step1=process_data_step1(data) 
    step2=process_data_step2(step1) 
    save_data(step2) 

etl_dag=spd_etl()        
