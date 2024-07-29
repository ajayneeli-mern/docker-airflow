from datetime import datetime, timedelta
import pandas as pd
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
import requests
import json


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
    'my_api_dag',
    default_args=default_args,
    description='Read api and write data to json',
    schedule_interval=timedelta(hours=1),
)


# Function to read local CSV data
def read_api():
    url = "https://edamam-food-and-grocery-database.p.rapidapi.com/api/food-database/v2/parser"

    querystring = {"nutrition-type":"cooking","category[0]":"generic-foods","health[0]":"alcohol-free"}

    headers = {
        "X-RapidAPI-Key": "238b19a261msh6faff61eaf7274ap1c5b74jsn29182f756a83",
        "X-RapidAPI-Host": "edamam-food-and-grocery-database.p.rapidapi.com"
    }


    try:
        response = requests.get(url, headers=headers, params=querystring)

        # Check if the request was successful (status code 200)
        if response.status_code == 200:
            #data = json.dumps(response.json(),indent=4) # Parse JSON response  , never to reponse ,dumps for gettiing value from json
            data=response.json()['hints']
            return data
            
            
        else:
            print(f"Request failed with status code: {response.status_code}")
    except requests.exceptions.RequestException as e:
        print(f"An error occurred: {e}")

def write_to_json(**kwargs):
    ti=kwargs['ti']
    data=ti.xcom_pull(task_ids="read_api_task")#get data from task A
    food = [item["food"] for item in data]
    print(food) 

    json_file_path = 'food.json'

    # Write data to the JSON file
    with open(json_file_path, 'w') as json_file:
        json.dump(food, json_file, indent=4)  # indent for pretty formatting

    print(f"Data written to {json_file_path}")

    


# Define the task to read local CSV data
read_api_task = PythonOperator(
    task_id='read_api_task',
    python_callable=read_api,
    #bash_command='echo "########Read successfully############"',
    dag=dag,
)

write_to_json_task = PythonOperator(
    task_id='write_to_json_task',
    python_callable=write_to_json,
    #bash_command='echo "########writen_to_jsonsuccessfully############"',
    dag=dag,
    provide_context=True,#allow the task to access the context
)

sucess_task = BashOperator(
    task_id='sucess_task',
    bash_command='echo "sucess_task-done"',
    dag=dag,
)



read_api_task >> write_to_json_task >>sucess_task