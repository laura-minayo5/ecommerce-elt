from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

# Define default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG with a unique name, default arguments, and a schedule interval
with DAG(
    dag_id='silver_to_postgres',
    default_args=default_args,
    description='Loads cleaned Silver data from Snowflake into Postgres 3NF tables',
    schedule_interval=None,   # triggered by silver_transformation
    start_date=datetime(2026, 1, 1),
    catchup=False,
) as dag:

    # Define the task
    load_silver_to_postgres = BashOperator(
        task_id='load_silver_to_postgres',
        bash_command='python3 /opt/elt/silver_to_postgres.py'
    )

    # Set task dependencies
    load_silver_to_postgres


