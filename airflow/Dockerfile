# Use the official Airflow image as the base image
FROM apache/airflow:2.7.0-python3.10

# Switch to root for system-level installs
USER root

# Install system dependencies for Airflow and PostgreSQL
RUN apt-get update && apt-get install -y --no-install-recommends \
        gcc libpq-dev \
    && rm -rf /var/lib/apt/lists/*

# sets the working directory in the container to /opt/airflow
WORKDIR /opt/airflow

# Copy requirements as root
COPY airflow/requirements.txt .

# Switch to airflow user BEFORE pip install to avoid permission issues with installed packages
USER airflow

RUN pip install --no-cache-dir --no-compile -r requirements.txt