from datetime import timedelta
from airflow.models import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago

# Default arguments
default_args = {
    'owner': 'Bouba_Ismaila',
    'start_date': days_ago(0),
    'email': ['bayolaismaila@gmail.com'],
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'email_on_failure': True,
    'email_on_retry': True,
}

# Define the DAG
dag = DAG(
    'ETL_toll_data',
    default_args=default_args,
    description='Apache Airflow Final Assignment',
    schedule_interval=timedelta(days=1),
)

# Define file paths
base_path = '/usr/local/airflow/dags/finalassignment'

# Define tasks
download = BashOperator(
    task_id='download',
    bash_command=(
        'mkdir -p /usr/local/airflow/dags/finalassignment || true && '
        'curl -o /usr/local/airflow/dags/finalassignment/tolldata.tgz '
        '"https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBM-DB0250EN-SkillsNetwork/labs/Final%20Assignment/tolldata.tgz"'
    ),
    email_on_failure=False,
    dag=dag,
)

unzip_data = BashOperator(
    task_id='unzip_data',
    bash_command=f'tar -xzf {base_path}/tolldata.tgz -C {base_path}/',
    dag=dag,
)

extract_data_from_csv = BashOperator(
    task_id='extract_data_from_csv',
    bash_command=f"cut -d',' -f1-4 {base_path}/vehicle_data.csv > {base_path}/csv_data.csv",
    dag=dag,
)

extract_data_from_tsv = BashOperator(
    task_id='extract_data_from_tsv',
    bash_command=f"cut -f2,4-5 {base_path}/tollplaza-data.tsv > {base_path}/tsv_data.csv",
    dag=dag,
)

extract_data_from_fixed_width = BashOperator(
    task_id='extract_data_from_fixed_width',
    bash_command=(
        f"echo 'Type of Payment code,Vehicle Code' > {base_path}/fixed_width_data.csv && "
        f"awk '{{print substr($0, 44, 9), substr($0, 58, 5)}}' {base_path}/payment-data.txt >> "
        f"{base_path}/fixed_width_data.csv"
    ),
    dag=dag,
)

consolidate_data = BashOperator(
    task_id='consolidate_data',
    bash_command=(
        f"paste -d ',' {base_path}/csv_data.csv {base_path}/tsv_data.csv {base_path}/fixed_width_data.csv > "
        f"{base_path}/extracted_data.csv"
    ),
    dag=dag,
)

transform_data = BashOperator(
    task_id='transform_data',
    bash_command=(
        f"cut -d',' -f4 {base_path}/extracted_data.csv | tr '[a-z]' '[A-Z]' > {base_path}/transformed_data.csv"
    ),
    dag=dag,
)

# Task pipeline
download >> unzip_data >> extract_data_from_csv >> extract_data_from_tsv >> extract_data_from_fixed_width >> consolidate_data >> transform_data
