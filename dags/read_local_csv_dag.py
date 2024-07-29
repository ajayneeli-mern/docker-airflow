from datetime import datetime, timedelta
import pandas as pd
from airflow import DAG
from airflow.operators.python_operator import PythonOperator

# Function to read local CSV data
def read_csv_data():
    data = {
        'Name': ['Alice', 'Bob', 'Charlie', 'David'],
        'Age': [25, 30, 35, 40],
        'City': ['New York', 'Los Angeles', 'Chicago', 'Houston']
    }

    df = pd.DataFrame(data)

    print(df)

# Define the default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 4, 3),
    'email':['ajayneeli6464@gmail.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
dag = DAG(
    'read_local_csv_dag',
    default_args=default_args,
    description='A simple DAG to read local CSV data',
    schedule_interval=timedelta(hours=1),
)

# Define the task to read local CSV data
read_csv_task = PythonOperator(
    task_id='read_csv_task',
    python_callable=read_csv_data,
    dag=dag,
)

read_csv_task
