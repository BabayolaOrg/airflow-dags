# import the libraries

from datetime import timedelta
from airflow.models import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.utils.dates import days_ago

#defining DAG arguments
default_args = {
    'owner': 'Bouba_Ismaila',
    'start_date': days_ago(0),
    'email': ['bayolaismaila@gmail.com'],
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
    'email on failure':true,
    'email_on_retry' true,
}

# defining the DAG
dag = DAG(
    'ETL_toll_data.py',
    default_args=default_args,
    description='Apache Airflow Final Assignment',
    schedule_interval=timedelta(minutes=5),
)

# Defining the tasks
# Defining download task
download = BashOperator (task_id = 'download',
    Bash_command = ('curl "https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBM-DB0250EN-SkillsNetwork/labs/Final%20Assignment/tolldata.tgz" ' 
    '-o /Babayola/airflow-dags/dags/finalassignment/tolldata.tgz'
),
    dag=dag,
)

# Defining unzip task
unzip_data = BashOperator(task_id='unzip_data',
    bash_command = ('tar -xzf Babayola/airflow-dags/dags/finalassignment/tolldata.tgz 
    '-C /Babayola/airflow-dags/dags/finalassignment/'
),
dag=dag,
)

# Defining csv data extract task
extract_data_from_csv = BashOperator (task_id = 'extract_data_from_csv',
    bash_command = ("cut -d',' -f1-4 /Babayola/airflow-dags/dags/finalassignment/vehicle_data.csv >"  
                                    "/Babayola/airflow-dags/dags/finalassignment/csv_data.csv"
),
dag=dag,
)

# defining tsv data extract task
extract_data_from_tsv = BashOperator(task_id = 'extract_data_from_tsv',
bash_command = ("cut -f2,4-5 /Babayola/airflow-dags/dags/finalassignment/tollplaza-data.tsv >"
                                 "/Babayola/airflow-dags/dags/finalassignment/tsv_data.csv" 
),
dag=dag,
)

# Defining extract fixed-width data task using awk
extract_data_from_fixed_width = BashOperator(
    task_id='extract_data_from_fixed_width',
    bash_command=(
        "echo 'Type of Payment code,Vehicle Code' > /Babayola/airflow-dags/dags/finalassignment/fixed_width_data.csv && "
        "awk '{print substr($0, 44, 9), substr($0, 58, 5)}' /Babayola/airflow-dags/dags/finalassignment/payment-data.txt >>"
        "/Babayola/airflow-dags/dags/finalassignment/fixed_width_data.csv"
    ),
    dag=dag,
)

# Defining consolidate data
consolidate_data = BashOperator(
    task_id='consolidate_data',
    bash_command= ( 
    "paste -d ',' /Babayola/airflow-dags/dags/finalassignment/csv_data.csv"
                 "/Babayola/airflow-dags/dags/finalassignment/tsv_data.csv"
                 "/Babayola/airflow-dags/dags/finalassignment/fixed_width_data.csv >"
                "/Babayola/airflow-dags/dags/finalassignment/extracted_data.csv"
    ),
    dag=dag,
)

#defining transform data task
transform_data = BashOperator(
    task_id='transform_data',
    bash_command=(
        "awk -F ',' '{ $4=toupper($4); print $0 }' /Babayola/airflow-dags/dags/finalassignment/extracted_data.csv "
        "> /Babayola/airflow-dags/dags/finalassignment/staging/transformed_data.csv"
    ),
    dag=dag,
)                             

# task pipeline

download >> unzip_data >> extract_data_from_csv 
>> extract_data_from_tsv >> extract_data_from_fixed_width 
>> consolidate_data >> transform_data
