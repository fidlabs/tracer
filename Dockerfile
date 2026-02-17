FROM apache/airflow:2.8.0-python3.11

USER root
RUN apt-get update && apt-get install -y --no-install-recommends \
    jq \
    ca-certificates \
    wget \
    git \
    build-essential \
    libhwloc-dev \
    ocl-icd-opencl-dev \
    pocl-opencl-icd \
    pkg-config \
    && rm -rf /var/lib/apt/lists/*

# Install newer Go version (1.25+) to build s2
RUN ARCH=$(dpkg --print-architecture) && \
    case "$ARCH" in \
    amd64) GOARCH=amd64 ;; \
    arm64) GOARCH=arm64 ;; \
    *) echo "Unsupported arch: $ARCH" && exit 1 ;; \
    esac && \ 
    wget -O /tmp/go.tar.gz https://go.dev/dl/go1.25.0.linux-${GOARCH}.tar.gz && \
    tar -C /usr/local -xzf /tmp/go.tar.gz && \
    rm /tmp/go.tar.gz

# Install s2 compression tool
ENV PATH="/usr/local/go/bin:/root/go/bin:${PATH}"
ENV GOBIN=/usr/local/bin
RUN /usr/local/go/bin/go install github.com/klauspost/compress/s2/cmd/...@v1.17.0

# Install Rust toolchain (required for filecoin-ffi)
RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y
ENV PATH="/root/.cargo/bin:${PATH}"

# Build and install filecoin-ffi
RUN mkdir -p /opt/filecoin-ffi
RUN git clone https://github.com/filecoin-project/filecoin-ffi.git /opt/filecoin-ffi
RUN cd /opt/filecoin-ffi/ && make

# Build decode_params Go executable for the correct architecture
# This must happen as root before switching to airflow user
RUN mkdir -p /opt/airflow/plugins/goExecutables
COPY plugins/goExecutables/*.go /opt/airflow/plugins/goExecutables/
COPY plugins/goExecutables/go.mod /opt/airflow/plugins/goExecutables/
COPY plugins/goExecutables/go.sum /opt/airflow/plugins/goExecutables/
RUN ARCH=$(dpkg --print-architecture) && \
    case "$ARCH" in \
    amd64) GOARCH=amd64 ;; \
    arm64) GOARCH=arm64 ;; \
    *) echo "Unsupported arch: $ARCH" && exit 1 ;; \
    esac && \
    cd /opt/airflow/plugins/goExecutables && \
    /usr/local/go/bin/go mod tidy && \
    /usr/local/go/bin/go mod download && \
    GOOS=linux GOARCH=${GOARCH} /usr/local/go/bin/go build -o decode_params main.go && \
    chmod +x decode_params && \
    # Clean up source files to reduce image size (keep binary and go.sum for reference)
    rm -f *.go go.mod

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
    opensearch-py==2.6.0 \
    web3==7.14.0 \
    filecoin-address==0.1.2 \
    multiformats==0.3.1.post4 \
    cbor2==5.7.1 \
    "psycopg[binary]"

# Note: s2 Python package is for geometry, not compression
# For S2 compression, we need to build/install s2 CLI tool or use alternative

# Verify airflow CLI is working
RUN which airflow && airflow version
