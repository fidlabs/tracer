# Tracer

Dev infrastructure and transformers for taking insights from Filecoin traces.

## System Requirements

Running the full dockerised stack is relatively modest. Just don't go crazy with downloading all the Traces at once!

## Install & test

### Pre-requisites

Copy `.env-example` to `.env` and fill in your R2 access credentials:

```env
# Compose project name. Do not change this
COMPOSE_PROJECT_NAME=fidlabs-tracer

# R2 sources config
R2_ACCOUNT_ID=<32-character R2 account ID>
R2_ACCESS_KEY_ID=<32-character access key ID>
R2_SECRET_ACCESS_KEY=<64-character access key>
R2_BUCKET=filecoin-files   # is 'filecoin-files' for the Zondax traces
R2_PREFIX=                 # optional; is empty for the Zondax traces
R2_GLOB=*.json.s2          # all Zondax traces are published s2 coded

# Airflow admin UI
AIRFLOW_ADMIN_USERNAME=admin  # hard coded for local test only!
AIRFLOW_ADMIN_PASSWORD=admin  # hard coded for local test only!
```

### Bring up the containers

First bring up the infrastructure services:
```bash
docker compose up -d postgres postgres-airflow clickhouse airflow-init
```

**Existing deployment:** migrate Airflow history off the shared postgres before starting the scheduler (see `scripts/migrate-airflow-metadata.sh`).

Wait for initialization to complete, then start Airflow services:
```bash
docker compose up -d airflow-scheduler airflow-webserver
```

### First time only: create admin user

Create the admin user (this command can be run multiple times):
```bash
docker compose exec airflow-webserver bash -lc 'airflow users create --role Admin --username admin --password admin --firstname admin --lastname admin --email admin@example.com'
```

### Tear-down

If you want to destory all state and start again:
```bash
docker compose down -v --remove-orphans
docker image rm zondax-airflow-webserver:latest zondax-airflow-scheduler:latest zondax-airflow-init:latest 2>/dev/null || true
```
