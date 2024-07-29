from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.email_operator import EmailOperator

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 4, 3),
    'email_on_failure': True,  # Disable email on failure
    'email_on_retry': False,  # Disable email on retry
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'email_on_success',
    default_args=default_args,
    description='A DAG to send email on successful run',
    schedule_interval=timedelta(days=1),  # Run the DAG daily
)

start_task = DummyOperator(task_id='start', dag=dag)

end_task = DummyOperator(task_id='end', dag=dag)

email_task = EmailOperator(
    task_id='send_email',
    to='ajayneeli6464@gmail.com',  # Change this to your email
    subject='Airflow DAG Succeeded',
    html_content='<p>Your Airflow DAG has succeeded!</p>',
    dag=dag,
)

start_task >> end_task
end_task >> email_task
