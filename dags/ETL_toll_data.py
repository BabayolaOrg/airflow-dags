from datetime import timedelta
from airflow.models import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
import pandas as pd

# Default arguments
default_args = {
    'owner': 'Bouba_Ismaila',
    'start_date': days_ago(0),
    'email': ['bayolaismaila@gmail.com'],
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'email_on_failure': False,
    'email_on_retry': False,
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

# Conflicting directory or file cleanup
cleanup = BashOperator(
    task_id='cleanup',
    bash_command=f'rm -rf {base_path}',
    dag=dag,
)

# Define tasks
download = BashOperator(
    task_id='download',
    bash_command=(
        f'mkdir -p {base_path} || true && '
        f'curl -o {base_path}/tolldata.tgz '
        f'"https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBM-DB0250EN-SkillsNetwork/labs/Final%20Assignment/tolldata.tgz"'
    ),
    email_on_failure=False,
    dag=dag,
)

unzip_data = BashOperator(
    task_id='unzip_data',
    bash_command=f'tar -xzvf {base_path}/tolldata.tgz -C {base_path}/',
    dag=dag,
)

# Python function to extract data from CSV
def extract_data_from_csv_func():
    df = pd.read_csv(f'{base_path}/vehicle-data.csv', usecols=[0, 1, 2, 3])  # Adjust columns as needed
    df.to_csv(f'{base_path}/csv_data.csv', index=False)

extract_data_from_csv = PythonOperator(
    task_id='extract_data_from_csv',
    python_callable=extract_data_from_csv_func,
    dag=dag,
)

# Python function to extract data from TSV
def extract_data_from_tsv_func():
    df = pd.read_csv(f'{base_path}/tollplaza-data.tsv', sep='\t', usecols=[1, 3, 4])  # Adjust columns as needed
    df.to_csv(f'{base_path}/tsv_data.csv', index=False)

extract_data_from_tsv = PythonOperator(
    task_id='extract_data_from_tsv',
    python_callable=extract_data_from_tsv_func,
    dag=dag,
)

# Python function to extract data from fixed-width file
def extract_data_from_fixed_width_func():
    # Fixed-width columns based on your provided substr positions
    colspecs = [(43, 52), (57, 62)]  # Columns: (start, end) positions
    df = pd.read_fwf(f'{base_path}/payment-data.txt', colspecs=colspecs, names=['Type of Payment code', 'Vehicle Code'])
    df.to_csv(f'{base_path}/fixed_width_data.csv', index=False)

extract_data_from_fixed_width = PythonOperator(
    task_id='extract_data_from_fixed_width',
    python_callable=extract_data_from_fixed_width_func,
    dag=dag,
)

# Consolidate data from all sources
consolidate_data = BashOperator(
    task_id='consolidate_data',
    bash_command=(
        f"paste -d ',' {base_path}/csv_data.csv {base_path}/tsv_data.csv {base_path}/fixed_width_data.csv > "
        f"{base_path}/extracted_data.csv"
    ),
    dag=dag,
)

# Transform data (e.g., converting to uppercase)
transform_data = BashOperator(
    task_id='transform_data',
    bash_command=(
        f"cut -d',' -f4 {base_path}/extracted_data.csv | tr '[a-z]' '[A-Z]' > {base_path}/transformed_data.csv"
    ),
    dag=dag,
)

# Task pipeline
cleanup >> download >> unzip_data >> extract_data_from_csv >> extract_data_from_tsv >> extract_data_from_fixed_width >> consolidate_data >> transform_data
