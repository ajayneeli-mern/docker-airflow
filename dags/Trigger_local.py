from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import datetime, timedelta

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
dag = DAG(
    'trigger_python_script',
    default_args=default_args,
    description='A simple DAG to trigger a local Python script',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 1, 1),
    catchup=False,
)

# Define the task to run the Python script
run_python_script = BashOperator(
    task_id='run_python_script',
    bash_command='python \\C:\\Users\\ajayn\\Documents\\covid_data\\covid.py',
    dag=dag,
)

# If you have more tasks, define them here and set their dependencies

# Example: run_python_script >> another_task
