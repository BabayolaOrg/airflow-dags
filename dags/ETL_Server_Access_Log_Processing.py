# import the libraries

from datetime import timedelta
# The DAG object; we'll need this to instantiate a DAG
from airflow.models import DAG
# Operators; you need this to write tasks!
from airflow.operators.bash_operator import BashOperator
# This makes scheduling easy
from airflow.utils.dates import days_ago

#defining DAG arguments

# You can override them on a per-task basis during operator initialization
default_args = {
    'owner': 'Bouba',
    'start_date': days_ago(0),
    'email': ['bayolaismaila@gmail.com'],
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# defining the DAG

# define the DAG
dag = DAG(
    'my-first-dag',
    default_args=default_args,
    description='My first DAG',
    schedule_interval=timedelta(hours=3),
)

# define the tasks
# Define the first task
download = BashOperator( task_id = 'download', bash_command = 'curl = "https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBM-DB0250EN-SkillsNetwork/labs/Apache%20Airflow/Build%20a%20DAG%20using%20Airflow/web-server-access-log.txt" -o web-server-access-log.txt', 
dag=dag,
)                      
# define the second task

extract = BashOperator(
    task_id='extract',
    bash_command='cut -f1,4 -d"#" web-server-access-log.txt > /Babayola/airflow-dags/dags/extracted.txt',
    dag=dag,
)

# define the third task
transform = BashOperator(
    task_id='transform',
    bash_command='tr "[a-z]" "[A-Z]" < /Babayola/airflow-dags/dags/extracted.txt > /Babayola/airflow-dags/dags/capitalized.txt',
    dag=dag,
)

# define the fourth task
load = BashOperator(
    task_id='load',
    bash_command='zip log.zip capitalized.txt' ,
    dag=dag,
)

# task pipeline
download >> extract >> transform >> load