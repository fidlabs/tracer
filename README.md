# Tracer

Dev infrastructure and transformers for taking insights from Filecoin traces.

## System Requirements

Running the full dockerised stack is relatively modest. Just don't go crazy with downloading all the Traces at once!

## Install & test

### Pre-requisites

Copy `.env-example` to `.env` and fill in your R2 access credentials:

```env
# ---- fill these ----
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

First bring up the underlying stores:
```bash
docker compose -f docker-compose.yml -f docker-compose.airflow.yml up -d postgres clickhouse
```

Now WAIT for them to be healthy (check with `docker ps` - should only take a few seconds) then start the rest:
```bash
docker compose -f docker-compose.yml -f docker-compose.airflow.yml up -d airflow-scheduler airflow-webserver
```

### First time only: initialise airflow

Initialise airflow and create admin user. Note the user create rune can be run many times if you want more, but do not re-run `airflow-init` unless you actually change the schema or blow away your containers:
```bash
docker compose -f docker-compose.yml -f docker-compose.airflow.yml up -d airflow-init
docker compose -f docker-compose.yml -f docker-compose.airflow.yml exec airflow-webserver bash -lc 'airflow users create --role Admin --username admin --password admin --firstname admin --lastname admin --email admin@example.com'
```

### Tear-down

If you want to destory all state and start again:
```bash
docker compose down -v --remove-orphans
docker image rm zondax-airflow-webserver:latest zondax-airflow-scheduler:latest zondax-airflow-init:latest 2>/dev/null || true
```
