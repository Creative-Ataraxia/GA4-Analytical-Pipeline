# build custom image based on this official image
FROM apache/airflow:3.0.2

# install core build tools for the container; libpq-dev is postgres header
USER root
RUN apt-get update && apt-get install -y build-essential libpq-dev && apt-get clean && rm -rf /var/lib/apt/lists/*

# Switch back to airflow user (important!)
USER airflow

# then install the desp listed in requirements.txt
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt