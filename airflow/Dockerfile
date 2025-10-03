# build s2 decoder
FROM golang:1.22-alpine AS s2build
RUN go install github.com/klauspost/compress/s2/cmd/s2d@latest

# airflow + cli tools
FROM apache/airflow:2.9.2-python3.11
USER root
RUN mkdir -p /opt/airflow/dags /opt/airflow/include /opt/airflow/logs
RUN apt-get update && apt-get install -y --no-install-recommends jq ca-certificates && rm -rf /var/lib/apt/lists/*
USER airflow
ENV PATH="/home/airflow/.local/bin:${PATH}"

# awscli via pip (lighter than distro python)
COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir \
    awscli==1.* \
    clickhouse-connect==0.7.18 \
    apache-airflow-providers-amazon==9.1.0 \
    boto3==1.34.162

# add decoder
COPY --from=s2build /go/bin/s2d /usr/local/bin/s2dec

