### Data_Analytic Airflow project
Apache Airflow Project

from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
from airflow.providers.standard.operators.bash import BashOperator
import datetime as dt
import pandas as pd

#### DAG Arguments
#### Define DAG Arg:
default_args = {
    'owner' : 'Andre Garner',
    'start_date':datetime.today(),
    'email':'andre.t.garner2@outlook.com',
    'email_on_failure':True,
    'email_on_retry': True,
    'retries':1,
    'retry_delay': timedelta(minutes=5),
}

# DAG Definition
# Define DAG
dag = DAG(
    dag_id ='ETL_toll_data',
    schedule=timedelta(days=1),
    default_args=default_args,
    description='Apache Airflow Final Assignment',
)

# Tasks Modules
# Create a task to unzip data
unzip_data = BashOperator(
    task_id="unzip data",
    bash_command='tar -xvf airflow/dags/finalassignment/tolldata.tgz -C /home/project/airflow/dags/finalassignment/'
    dag=dag,
)
# Extract Data
extract_data_from_csv = BashOperator(
    task_id='extract_data',
    bash_command='cut -d',' -f1,2,3,4 home/project/airflow/dags/vehicle-data.csv > /home/project/airflow/dags/finalassignment/csv_data.csv',
    dag=dag,
)

# Create a taks to extract data from tsv file
extract_data_from_tsv = BashOperator(
    task_id='extract_data_from_tsv',
    bash_command='cut -f5-7 home/project/airflow/dags/tollplaza-data.tsv | tr "\t" "," > /home/project/airflow/dags/finalassignment/tsv_data.csv',
    dag=dag,
)
# Create a task to extract data from fixed width file
extract_data_from_fixed_width=bash_command(
    task_id='fixed_width_extraction',
    bash_command='cut -c59-67 home/project/airflow/dags/payment-data.txt | tr " " "," > /home/project/airflow/dags/finalassignment/fixed_width_data.csv',
    dag=dag,
)

# Create a task to consoidate data extracted
consolidated_data=bash_command(
    task_id='consolidate varying data structures',
    bash_command='paste -d /home/project/airflow/dags/finalassignment/csv_data.csv /home/project/airflow/dags/finalassignment/tsv_data.csv /home/project/airflow/dags/finalassignment/fixed_width_data.csv > /home/project/airflow/dags/finalassignment/extracted_data.csv',
    dag=dag,
)

# Transform the vehicle_type field to uppercase
transform_data = BashOperator(
    task_id='transform_data',
    bash_command='awk -F"," \'BEGIN {OFS=","} {$4=toupper($4); print}\' /home/project/airflow/dags/finalassignment/extracted_data.csv > /home/project/airflow/dags/finalassignment/transformed_data.csv',
    dag=dag,
)

#  Define the task pipeline
unzip_data >> extract_data_from_csv >> extract_data_from_tsv >> extract_data_from_fixed_width >> consolidate_data >> transform_data

