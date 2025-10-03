FROM apache/airflow:2.8.0-python3.11

USER root
RUN apt-get update && apt-get install -y --no-install-recommends \
    jq \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# Ensure directories exist and have proper permissions
RUN mkdir -p /opt/airflow/dags /opt/airflow/include /opt/airflow/logs && \
    chmod -R 755 /opt/airflow

USER airflow

# Ensure airflow user can write to logs directory
RUN mkdir -p /opt/airflow/logs/scheduler /opt/airflow/logs/webserver /opt/airflow/logs/dag_processor_manager

# Install required Python packages
RUN pip install --no-cache-dir \
    awscli==1.* \
    clickhouse-connect==0.7.18 \
    apache-airflow-providers-amazon==8.5.0 \
    boto3==1.34.162 \
    opensearch-py==2.6.0

# Install s2 decompressor
RUN pip install --no-cache-dir \
    s2==0.1.9

# Verify airflow CLI is working
RUN which airflow && airflow version
