from airflow import DAG
from airflow.operators.dummy import DummyOperator
from datetime import datetime

with DAG(
    dag_id="second_dag",
    schedule_interval="0 0 * * *",
    start_date=datetime(2023, 11, 25),
    catchup=False
) as dag:
    start = DummyOperator(task_id="start")
