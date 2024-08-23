import os
import csv
import yaml
import logging
import requests
from utils.utils import conn_to_
from airflow.decorators import dag, task
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator

from airflow.providers.amazon.aws.hooks.s3 import S3Hook

from sqlalchemy import text, create_engine
import pandas as pd
import yaml


"""
 1) Создает таблицу если не cуществует SQL таблица
 2) парсим csv файл 
 3) write csv файл в S3
 4) получаем csv файл из S3
 5) read csv и забираем id
 6) insert data to sql tables
 7) create .txt файл и push .txt
"""

default_args={
    'owner': 'airflow',
    'retries': 3,
    'retry_delay': timedelta(seconds=5),       
}


@dag(
    dag_id='spd_s3_to_postgres_dag',
    description='ETL system for SPD (insert data.csv to S3 and  Postgres)',
    default_args=default_args,
    start_date=datetime(2024, 8, 21),     
    schedule_interval=timedelta(hours=1),
    catchup=False
)
def spd_etl():

    @task()
    def create_table_if_not_exists_postgres(dbname:str):
        value=conn_to_(dbname=dbname)

        user=value['user']['airflow']
        host=value['host']['local']
        port=value['port']['shared']
        name=value['name']['airflow']
        password=value['password']['airflow']
        DATABASE_URL = f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{name}"                
        
        try:
            engine = create_engine(DATABASE_URL)
            with engine.connect() as conn:
                stmt="""
                    CREATE TABLE IF NOT EXISTS spd_airflow (
                    order_id SERIAL PRIMARY KEY, 
                    dia varchar(255),
                    art_sp varchar(255),
                    fam_sp varchar(255))
                     """
                conn.execute(text(stmt))
                logging.info("Таблица создана")            
        except Exception as err:
            logging.error(f"Проблема модуля: {err}")
            raise            

    @task()
    def get_file(ds):
        try:
            url = "https://support.staffbase.com/hc/en-us/article_attachments/360009197031/username.csv"
            df=pd.read_csv(url, sep=';')
            df.rename(columns={"Username":"usernane",
                               "First name":"f_name",
                               "Last name":"l_name"}, inplace=True)
            path=f'/opt/airflow/data/data_{ds}.csv'
            df.to_csv(path)
            logging.info(f"Файл успешно загружен и сохранен: {ds}")
            return path
        except Exception as err:
            logging.error(f"Проблема модуля: {err}")
            raise

    @task()
    def upload_file_to_S3(path:str):
        try:
            filename=path.split('/')[-1]
            s3_hook=S3Hook(aws_conn_id='minio_conn')
            s3_hook.load_file(
                filename=path,
                key=filename,
                bucket_name='airflow',
                replace=True
            )
            logging.info("Файл успешно загружен")
            os.remove(path)
            return filename
        except Exception as err:
            logging.error(f"Проблема модуля: {err}")
            raise

    @task()
    def read_file_from_S3(filename:str):
        
        pass

    create_table_if_not_exists_postgres(dbname='postgres')
    path=get_file()
    upload_file_to_S3(path=path)    



etl_dag=spd_etl()