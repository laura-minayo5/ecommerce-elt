# Dockerfile for dbt transformations, used by Airflow's DockerOperator to run dbt commands in a containerized environment
FROM python:3.11-slim

# Switch to root for system-level installs
USER root

# sets the working directory in the container to /opt/ecommerce_dbt
WORKDIR /opt/ecommerce_dbt

# Copy requirements.txt first to leverage Docker cache for dependencies
COPY ecommerce_dbt/requirements.txt .

# Install dbt and Snowflake adapter dependencies
RUN pip install --no-cache-dir --no-compile -r requirements.txt

# entrypoint set to dbt, so when container runs, it executes dbt commands passed by Airflow's DockerOperator
# eg Docker splits that into:
# ENTRYPOINT  +  command
# ["dbt"]     +  "run --select silver --project-dir /opt/ecommerce_dbt"
ENTRYPOINT ["dbt"]