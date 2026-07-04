FROM python:3.11-slim

# Install Java (required by PySpark)
RUN apt-get update && \
    apt-get install -y default-jre procps && \
    rm -rf /var/lib/apt/lists/*

# Install pip packages (including pyspark)
COPY spark_jobs/requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

# Copy the spark_jobs source tree
COPY spark_jobs/ /app/spark_jobs/

# Copy ingestion module (bronze_layer imports file_stream_reader)
COPY ingestion/ /app/ingestion/

WORKDIR /app/spark_jobs

# Pre-download Delta Lake and Hadoop AWS JARs so there is no runtime download delay.
ENV SPARK_SUBMIT_ARGS="--packages io.delta:delta-spark_2.12:3.2.1,org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262"

CMD ["spark-submit", \
     "--packages", "io.delta:delta-spark_2.12:3.2.1,org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262", \
     "bronze_layer.py"]
