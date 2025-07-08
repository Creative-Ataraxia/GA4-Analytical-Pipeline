FROM python:3.10-slim

WORKDIR /workspace

COPY requirements.txt .

RUN apt-get update && apt-get install -y build-essential git libpq-dev \
 && pip install --upgrade pip \
 && pip install -r requirements.txt \
 && apt-get clean && rm -rf /var/lib/apt/lists/*
