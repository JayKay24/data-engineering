FROM apache/airflow:2.9.3-python3.10

USER root
RUN apt-get update && \
    apt-get install -y --no-install-recommends gcc python3-dev && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

USER airflow
COPY 3rdparty/requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt
