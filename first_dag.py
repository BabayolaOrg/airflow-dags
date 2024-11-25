from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from datetime import datetime

default_args = {
    'owner': 'airflow',
    'retries': 1,
}

dag = DAG('first_dag', default_args=default_args, schedule_interval=None)

start_task = DummyOperator(
    task_id='start_task',
    dag=dag,
)
