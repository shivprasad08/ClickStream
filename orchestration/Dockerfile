FROM apache/airflow:2.10.3-python3.11

USER root
# Install Java (required by PySpark)
RUN apt-get update && \
    apt-get install -y default-jre procps && \
    rm -rf /var/lib/apt/lists/*

USER airflow
# Install PySpark so PythonOperators can run local Spark maintenance jobs
RUN pip install --no-cache-dir pyspark==3.5.3
