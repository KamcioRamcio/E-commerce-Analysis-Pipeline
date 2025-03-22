FROM quay.io/astronomer/astro-runtime:12.7.1

USER root
RUN apt-get update && \
    apt-get install -y default-jre wget curl && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/default-java

RUN python -m venv dbt_venv && \
    . dbt_venv/bin/activate && \
    pip install --no-cache-dir dbt-bigquery && \
    deactivate

RUN pip install --no-cache-dir \
    pyspark==3.5.0 \
    pandas-gbq \
    matplotlib \
    seaborn \
    google-cloud-bigquery \
    google-cloud-storage

RUN mkdir -p /opt/spark_jars && \
    cd /opt/spark_jars && \
    wget https://storage.googleapis.com/spark-lib/bigquery/spark-3.5-bigquery-0.42.1.jar && \
    wget https://storage.googleapis.com/hadoop-lib/gcs/gcs-connector-hadoop3-latest.jar
