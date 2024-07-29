from airflow.operators.bash import BashOperator
from airflow.operators.python import Python

...

task_a = PythonOperator(
  task_id='task_a',
  python_callable=my_python_function
)

task_b = BashOperator(
  task_id='task_b',
  bash_command='echo "hello"'
)