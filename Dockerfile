FROM apache/airflow:3.0.0-python3.11

USER root
# Install AWS CLI v2
RUN apt-get update && apt-get install -y awscli
COPY requirements.txt /requirements.txt

USER airflow
RUN pip install --no-cache-dir -r /requirements.txt