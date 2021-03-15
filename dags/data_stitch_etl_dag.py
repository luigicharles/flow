import sys
from datetime import timedelta, datetime

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.utils.dates import days_ago

sys.path.insert(0,"/usr/local/airflow/dags")
from jobs.data_stitch_etl import job 

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
    'stitch_etl',
    default_args=default_args,
    description='stitches etl',
    schedule_interval=timedelta(hours=4))

data_stitch_etl_head= BashOperator(
    task_id='data_stitch_etl_head',
    bash_command='echo "🪡 starting stitch etl"',
    dag=dag)

data_stitch_etl_extract = PythonOperator(
    task_id='data_stitch_etl_extract',
    python_callable=job.extract,
    dag=dag)

data_stitch_etl_transform = PythonOperator(
    task_id='data_stitch_etl_transform',
    python_callable=job.transform,
    dag=dag)

data_stitch_etl_load = PythonOperator(
    task_id='data_stitch_etl_load',
    python_callable=job.load,
    dag=dag)

data_stitch_etl_head >> data_stitch_etl_extract >>  data_stitch_etl_transform >> data_stitch_etl_load 