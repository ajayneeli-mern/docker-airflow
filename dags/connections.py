from airflow.models import DAG
from airflow.operators.mysql_operator import MySqlOperator
from datetime import datetime

dag = DAG(
    'mysql_dag_example',
    schedule_interval='@daily',
    start_date=datetime(2024, 5, 15)
)

mysql_task = MySqlOperator(
    task_id='mysql_task',
    mysql_conn_id='my_mysql_conn',
    sql='SELECT * FROM food',
    dag=dag
)