from airflow import DAG # To define the DAG
from datetime import datetime, timedelta # To work with date and time
from airflow.operators.bash import BashOperator # Importing the BashOperator to execute bash commands in the DAG
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

# Define default arguments for the DAG
default_args = {
    'owner': 'airflow', # The owner of the DAG
    'depends_on_past': False, # Whether the DAG depends on past runs
    'retries': 1, # Number of retries in case of failure
    'retry_delay': timedelta(minutes=5), # Delay between retries
}

# Define the DAG with a unique name, default arguments, and a schedule interval
with DAG(
    dag_id='kaggle_to_snowflake_bronze', # Unique name for the DAG
    default_args=default_args, # Default arguments for the DAG
    schedule_interval='@daily', # Schedule to run the DAG daily
    start_date=datetime(2026, 1, 1), # The start date of the DAG
    catchup=False, # Whether to catch up on missed runs
) as dag:
    
    # Define a task using BashOperator to run the extract_load_kaggle.py script
    extract_load_task = BashOperator(
        task_id='extract_load_kaggle', # Unique task ID
        bash_command='python3 /opt/elt/extract_load_kaggle.py' # Command to execute the Python script
    )

    trigger_silver = TriggerDagRunOperator(
        task_id='trigger_silver_transformation',
        trigger_dag_id='silver_transformation',
        wait_for_completion=True,   # wait for silver to finish before marking this DAG done
        poke_interval=30,
    )

    # Set the task dependencies
    extract_load_task >> trigger_silver
    