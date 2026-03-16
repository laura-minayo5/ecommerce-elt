from airflow import DAG # To define the DAG
from datetime import datetime, timedelta # To work with date and time
from airflow.providers.docker.operators.docker import DockerOperator # To run Docker containers as tasks
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
    dag_id='gold_transformation',
    default_args=default_args,
    description='Runs dbt Gold models and tests',
    schedule_interval=None, # triggered by silver_transformation
    start_date=datetime(2026, 1, 1),
    catchup=False
) as dag:

    dbt_run = DockerOperator(
        task_id='dbt_run_gold',
        image=DBT_IMAGE,
        command='run --select gold.* marts.* --project-dir /opt/ecommerce_dbt --profiles-dir /root/.dbt', # Command to run dbt models in the Gold layer
        mounts=DBT_MOUNTS, # Mount the dbt project and profiles.yml into the container
        network_mode=DBT_NETWORK, 
        auto_remove="success", # Automatically remove the container after it finishes to save resources
        docker_url=DOCKER_URL, # Specify the Docker URL to connect to the Docker daemon
    )

    dbt_test = DockerOperator(
        task_id='dbt_test_gold',
        image=DBT_IMAGE,
        command='test --select gold.* marts.* --project-dir /opt/ecommerce_dbt --profiles-dir /root/.dbt',
        mounts=DBT_MOUNTS,
        network_mode=DBT_NETWORK,
        auto_remove="success",
        docker_url=DOCKER_URL,
    )

    dbt_run >> dbt_test