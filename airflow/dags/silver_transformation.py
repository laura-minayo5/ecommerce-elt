from airflow import DAG # To define the DAG
from datetime import datetime, timedelta # To work with date and time
from airflow.providers.docker.operators.docker import DockerOperator # To run Docker containers as tasks
from airflow.operators.trigger_dagrun import TriggerDagRunOperator #
from docker.types import Mount # for mounting volumes into the container.
from pathlib import Path
import os

# Define the Docker image for dbt
DBT_IMAGE = 'dbt-snowflake:1.9.0'


# define paths 
PROJECT_DIR = os.getenv('PROJECT_DIR', '/home/user/projects/ecommerce-elt')
DBT_DIR = f'{PROJECT_DIR}/ecommerce_dbt'

# Define the path to the dbt profiles directory, which can be set via an environment variable or defaults to ~/.dbt
DBT_PROFILES = os.getenv('DBT_PROFILES_DIR', '/home/user/.dbt')
# DBT_PROFILES = str(Path.home() / '.dbt') --root/.dbt
# DBT_PROFILES = f"{os.getenv('HOME')}/.dbt"

# Define the Docker network to ensure the container can communicate with Snowflake (if needed)
DBT_NETWORK = 'ecommerce-elt-network'

# Define the Docker URL to connect to the Docker daemon
DOCKER_URL = 'unix://var/run/docker.sock'

# Define the mounts for the Docker container to access the dbt project and profiles.yml from the host machine
DBT_MOUNTS = [
    Mount(source=DBT_DIR, target='/opt/ecommerce_dbt', type='bind'),
    Mount(source=DBT_PROFILES, target='/root/.dbt', type='bind'),
]


# Define default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG with a unique name, default arguments, and a schedule interval
with DAG(
    dag_id='silver_transformation', # Unique name for the DAG
    default_args=default_args, # Default arguments for the DAG
    description='Runs dbt Silver models and tests', # Description of the DAG
    schedule_interval=None, # triggered by kaggle_to_snowflake_bronze
    start_date=datetime(2026, 1, 1), # The start date of the DAG
    catchup=False,  # Whether to catch up on missed runs
) as dag:
    # Define the task to run dbt models in the Silver layer using a Docker container
    dbt_run = DockerOperator(
        task_id='dbt_run_silver',
        image=DBT_IMAGE,
        command='run --select silver --project-dir /opt/ecommerce_dbt --profiles-dir /root/.dbt', # Command to run dbt models in the Silver layer
        mounts=DBT_MOUNTS, # Mount the dbt project and profiles.yml into the container
        network_mode=DBT_NETWORK,
        auto_remove="success", # Automatically remove the container after it finishes to save resources
        docker_url=DOCKER_URL, # Specify the Docker URL to connect to the Docker daemon
    )

    dbt_test = DockerOperator(
        task_id='dbt_test_silver',
        image=DBT_IMAGE,
        command='test --select silver --project-dir /opt/ecommerce_dbt --profiles-dir /root/.dbt',
        mounts=DBT_MOUNTS,
        network_mode=DBT_NETWORK,
        auto_remove="success",
        docker_url=DOCKER_URL,
    )

    trigger_silver_to_postgres = TriggerDagRunOperator(
        task_id='trigger_silver_to_postgres',
        trigger_dag_id='silver_to_postgres',
        wait_for_completion=True, # wait for silver_to_postgres to finish before marking this DAG done
        poke_interval=30,
    )

    trigger_gold = TriggerDagRunOperator(
        task_id='trigger_gold_transformation',
        trigger_dag_id='gold_transformation',
        wait_for_completion=True, # wait for gold_transformation to finish before marking this DAG done
        poke_interval=30,
    )

    # Set the task dependencies
    dbt_run >> dbt_test >> trigger_silver_to_postgres >> trigger_gold