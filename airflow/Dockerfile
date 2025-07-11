# build custom image based on this official image
FROM apache/airflow:3.0.2

# install core build tools for the container; libpq-dev is postgres header
USER root
RUN apt-get update \
&& apt-get install -y build-essential libpq-dev openjdk-17-jdk \
&& apt-get clean && rm -rf /var/lib/apt/lists/*

# Switch back to airflow user (important!)
USER airflow
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH=$JAVA_HOME/bin:$PATH

# then install the desp listed in requirements.txt
COPY requirements.txt .
RUN pip install -r requirements.txt --constraint https://raw.githubusercontent.com/apache/airflow/constraints-3.0.2/constraints-3.10.txt