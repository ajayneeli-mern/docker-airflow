from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash_operator import BashOperator

# Define the default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 4, 3),
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
dag = DAG(
    'my_etl_dag',
    default_args=default_args,
    description='A simple DAG for ETL process',
    schedule_interval=timedelta(hours=1),
)

# Define tasks for each step in the ETL process

# Task to extract data
extract_task = BashOperator(
    task_id='extract_data',
    bash_command='echo "Data extracted successfully"',
    dag=dag,
)

# Task to transform data
transform_task = BashOperator(
    task_id='transform_data',
    bash_command='echo "Data transformed successfully"',
    dag=dag,
)

# Task to load transformed data
load_task = BashOperator(
    task_id='load_data',
    bash_command='echo "Data loaded successfully"',
    dag=dag,
)

# Define the dependencies between tasks
extract_task >> transform_task >> load_task
