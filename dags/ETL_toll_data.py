# import the libraries

from datetime import timedelta
from airflow.models import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.utils.dates import days_ago

#defining DAG arguments

# You can override them on a per-task basis during operator initialization
default_args = {
    'owner': 'Bouba_Ismaila',
    'start_date': days_ago(0),
    'email': ['bayolaismaila@gmail.com'],
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'email on failure = true',
    'email_on_retry = true',
}

# defining the DAG

dag = DAG(
    'ETL_toll_data.py',
    default_args=default_args,
    description='Apache Airflow Final Assignment',
    schedule_interval=timedelta(days=1),
)

# Defining the tasks
Defining download task
download = BashOperator (task_id = 'download',
    Bash_command = 'curl "https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBM-DB0250EN-SkillsNetwork/labs/Final%20Assignment/tolldata.tgz" -o /Babayola/airflow-dags/dags/finalassignment/tolldata.tgz',
    dag=dag,
)
# Defining unzip task
unzip = BashOperator(task_id='unzip_data',
    bash_command = 'tar -xzf Babayola/airflow-dags/dags/finalassignment/tolldata.tgz > /Babayola/airflow-dags/dags/finalassignment/txt_data.txt,
)
# Defining extract task
extract = BashOperator (task_id = 'extract_data_from_csv',
    bash_command = 'cut -d',' -f1-4 /Babayola/airflow-dags/dags/finalassignment/txt_data.txt > 
                                    /Babayola/airflow-dags/dags/finalassignment/csv_data.csv,
                       )
download >> unzip >> extract
