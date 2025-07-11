FROM python:3.10-slim

WORKDIR /workspace

# against build context from devcontainer.json and dont' forget the destination too!
COPY .devcontainer/requirements.txt .

RUN apt-get update && apt-get install -y build-essential git libpq-dev \
 && pip install --upgrade pip \
 && pip install -r requirements.txt \
     --constraint https://raw.githubusercontent.com/apache/airflow/constraints-3.0.2/constraints-3.10.txt \
 && apt-get clean && rm -rf /var/lib/apt/lists/*
