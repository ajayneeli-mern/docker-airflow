from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python import PythonOperator
import mysql.connector
import pandas as pd
from airflow.hooks.mysql_hook import MySqlHook
import base64
import pickle,logging
current_date = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)


# Configure logging
logging.basicConfig(level=logging.INFO)

# Define the default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': current_date,
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
dag = DAG(
    'etl_pipe_line_mysql_postgres',
    default_args=default_args,
    description='A simple DAG to extract data from mysql and load data to postgres',
    schedule_interval='0 9 * * *', # Run daily at 9 AM
    catchup=False,
)

# function to extract data from mysql
def extract_data_from_mysql(**kwargs):
    try:
            mysql_hook = MySqlHook(mysql_conn_id='my_mysql_conn')
        
                # Define the SQL query
            sql_query = 'SELECT * FROM food'
            
                # Run the query
            connection = mysql_hook.get_conn()
            cursor = connection.cursor()
            cursor.execute(sql_query)
            myresult = cursor.fetchall()
                
                # Process the results
            # for row in myresult:
            #     print(row)

            # Step 5: Get column names
            column_names = [desc[0] for desc in cursor.description]

            # Step 6: Load data into a Pandas DataFrame
            df = pd.DataFrame(myresult, columns=column_names)
            # Display the DataFrame
            print(df)
                # Serialize the DataFrame using pickle and encode with Base64
            df_serialized = base64.b64encode(pickle.dumps(df)).decode()
            
            # Push the serialized DataFrame to XCom
            kwargs['ti'].xcom_push(key='extracted_data', value=df_serialized)
            logging.info("Extracted data pushed to XCom")
            logging.info(f"Serialized DataFrame: {df_serialized}")

    except connection.connector.Error as err:
        print(f"Error: {err}")

    else:
        print("every thing running fine")

    finally:
        # Step 7: Close the cursor and connection
        if cursor:
            connection.close()
            print("MySQL connection is closed")

#function to transform data
def transform(**kwargs):
    # Pull the serialized DataFrame from XCom
    ti = kwargs['ti']
    df_serialized = ti.xcom_pull(key='extracted_data', task_ids='extract_task')
    print(df_serialized)
    
    if df_serialized is None:
        raise ValueError("No data received from extract_task")
    
    # Decode and deserialize the DataFrame
    df = pickle.loads(base64.b64decode(df_serialized))
    
    # Log the DataFrame to ensure it's correctly deserialized
    logging.info(f"Deserialized DataFrame in transform task:\n{df}")
    
    # Add an extra column
    df['check'] = 'good'
    print(df)
    # Serialize and encode the transformed DataFrame
    df_transformed_serialized = base64.b64encode(pickle.dumps(df)).decode()
    
    # Push the transformed DataFrame to XCom
    ti.xcom_push(key='transformed_data', value=df_transformed_serialized)
    logging.info("Transformed data pushed to XCom")
    logging.info(f"Serialized transformed DataFrame: {df_transformed_serialized}")

def load(**kwargs):
    # Pull the serialized transformed DataFrame from XCom
    ti = kwargs['ti']
    df_transformed_serialized = ti.xcom_pull(key='transformed_data', task_ids='transform_task')
    
    if df_transformed_serialized is None:
        raise ValueError("No data received from transform_task")
    
    # Decode and deserialize the DataFrame
    df = pickle.loads(base64.b64decode(df_transformed_serialized))
    
    # Log the DataFrame to ensure it's correctly deserialized
    logging.info("Loading data...")
    logging.info(f"Deserialized DataFrame in load task:\n{df}")
    # Add your loading logic here

#Task to extract data
extract_task = PythonOperator(
    task_id='extract_data_from_mysql',
    python_callable=extract_data_from_mysql,
    provide_context=True,
    dag=dag,
)



transform_task = PythonOperator(
    task_id='transform_task',
    python_callable=transform,
    provide_context=True,
    dag=dag,
)

load_task = PythonOperator(
    task_id='load_task',
    python_callable=load,
    provide_context=True,
    dag=dag,
)

# Set up task dependencies
extract_task >> transform_task >> load_task
