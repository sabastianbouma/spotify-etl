from datetime import timedelta
import datetime
from airflow import DAG
from airflow.operators.python_operator import PythonOperator

from spotify_etl import run_extraction, run_transformation

default_args = {
    'owner':'airflow',
    'depends_on_past':False,
    'start_date': datetime.datetime(2021,11,15),
    'email':['sabastianbouma@gmail.com'],
    'email_on_failure':True,
    'email_on_retry':False,
    'retries':1,
    'retry_delay':timedelta(minutes=1),
}

dag = DAG(
    'test',
    default_args=default_args,
    description='test dag',
    schedule_interval=timedelta(days=1),
)


def helper_function():
    print("this is the helper_function output")


extraction = PythonOperator(
    task_id='extraction',
    python_callable=run_extraction,
    dag=dag
)

transformation = PythonOperator(
    task_id='transformation',
    python_callable=run_transformation,
    dag=dag
)

extraction >> transformation