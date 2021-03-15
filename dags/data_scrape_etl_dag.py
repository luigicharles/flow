import sys
from datetime import timedelta, datetime

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.utils.dates import days_ago

sys.path.insert(0,"/usr/local/airflow/dags")
from jobs.data_scrape_etl import job 

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2020, 11, 8),
    'email': ['ljwcharles@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1)}

dag = DAG(
    'data_scrape_etl',
    default_args=default_args,
    description='scrapes etl',
    schedule_interval=timedelta(hours=12))


data_scrape_etl_head= BashOperator(
    task_id='scrape_etl_head',
    bash_command='echo "📦 starting scrape etl"',
    dag=dag)

data_scrape_etl_extract = PythonOperator(
    task_id='scrape_etl_extract',
    python_callable=job.extract,
    dag=dag)

data_scrape_etl_transform = PythonOperator(
    task_id='scrape_etl_transform',
    python_callable=job.transform,
    dag=dag)

data_scrape_etl_load = PythonOperator(
    task_id='scrape_etl_load',
    python_callable=job.load,
    dag=dag)


data_scrape_etl_head >> data_scrape_etl_extract >>  data_scrape_etl_transform >> data_scrape_etl_load 
