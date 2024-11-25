from airflow import DAG
from airflow.operators.dummy import DummyOperator
from datetime import datetime

default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'example_dag',
    default_args=default_args,
    description='A simple example DAG',
    schedule_interval=None,
    start_date=datetime(2023, 11, 1),
    catchup=False,
) as dag:
    start = DummyOperator(task_id='start')
    end = DummyOperator(task_id='end')

    start >> end
