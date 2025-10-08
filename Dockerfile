FROM apache/airflow:2.8.0-python3.11

USER root
RUN apt-get update && apt-get install -y --no-install-recommends \
    jq \
    ca-certificates \
    wget \
    && rm -rf /var/lib/apt/lists/*

# Install newer Go version (1.22+) to build s2
RUN wget -O /tmp/go.tar.gz https://go.dev/dl/go1.22.0.linux-amd64.tar.gz && \
    tar -C /usr/local -xzf /tmp/go.tar.gz && \
    rm /tmp/go.tar.gz

# Install s2 compression tool
ENV PATH="/usr/local/go/bin:/root/go/bin:${PATH}"
ENV GOBIN=/usr/local/bin
RUN /usr/local/go/bin/go install github.com/klauspost/compress/s2/cmd/...@v1.17.0

# Ensure directories exist and have proper permissions
RUN mkdir -p /opt/airflow/dags /opt/airflow/include /opt/airflow/logs && \
    chmod -R 755 /opt/airflow

USER airflow

# Ensure s2 is in PATH for airflow user
ENV PATH="/usr/local/bin:/usr/local/go/bin:${PATH}"

# Ensure airflow user can write to logs directory
RUN mkdir -p /opt/airflow/logs/scheduler /opt/airflow/logs/webserver /opt/airflow/logs/dag_processor_manager

# Install required Python packages
RUN pip install --no-cache-dir \
    awscli==1.* \
    clickhouse-connect==0.7.18 \
    apache-airflow-providers-amazon==8.5.0 \
    boto3==1.34.162 \
    opensearch-py==2.6.0

# Note: s2 Python package is for geometry, not compression
# For S2 compression, we need to build/install s2 CLI tool or use alternative

# Verify airflow CLI is working
RUN which airflow && airflow version
