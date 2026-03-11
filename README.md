# Bank Feature Pipeline

A Dagster pipeline that computes daily aggregate features for each client from the MBD-mini transaction dataset.

## Overview

The pipeline reads transaction parquet files and computes two assets:
- `trx` — loads raw transactions into DuckDB for a given date range
- `fetch_daily_data` — computes rolling 1-month features per client and event type

Features computed:
- `avg_amount` — rolling 1-month average transaction amount per client
- `sum_amount` — rolling 1-month total transaction amount per client
- `txn_count` — rolling 1-month transaction count per client

Supports two modes:
- **Daily mode** — materializes one day's partition via the scheduler
- **Backfill mode** — materializes the full date range in a single run using `BackfillPolicy.single_run()`

## Project Structure

```
Ameriabank/
├── Dockerfile
├── docker-compose.yml
├── k8s/
│   ├── configmap.yaml
│   ├── deployment.yaml
│   ├── pvc.yaml
│   └── service.yaml
├── helm/
│   └── values.yaml
├── src/
│   └── Ameriabank/
│       ├── definitions.py
│       ├── defs/
│       │   ├── assets/
│       │   │   ├── assets.py
│       │   │   └── constants.py
│       │   ├── jobs.py
│       │   ├── resources.py
│       │   └── schedules.py
│       └── data/
│           └── detail/
│               ├── trx/
│               ├── geo/
│               └── dialog/
└── tests/
```

## Requirements

- Python 3.12
- [uv](https://github.com/astral-sh/uv)
- Docker
- kubectl, minikube, helm (for Kubernetes deployment)

## Running
```
chmod +x ameriabank-ml
sudo ln -s /home/chemi_t/Documents/sci_fi/Programming/Job/Ameriabank/ameriabank-ml /usr/local/bin/
```
Now you can run it from anywhere using th `ameriabank-ml` command from the terminal:
```
ameriabank-ml
```
Choose either 1. Local or 2. Docker running option.

## Local Development

Create a `.env` file in the project root:
D

```
DAGSTER_HOME=/path/to/project/.dg
DUCKDB_DATABASE=/path/to/project/data/bank.db
```

```bash
uv sync
source .env && dagster dev
```

Open http://localhost:3000

## Docker

```bash
docker build -t ameriabank-dagster:latest .
docker compose up
```

Open http://localhost:3000

## Kubernetes (minikube)

```bash
minikube start --memory=4096 --cpus=2
eval $(minikube docker-env)
docker build -t ameriabank-dagster:latest .

kubectl apply -f k8s/configmap.yaml
kubectl apply -f k8s/pvc.yaml
kubectl apply -f k8s/deployment.yaml
kubectl apply -f k8s/service.yaml

kubectl get pods -w
minikube service dagster-service
```

## Helm

```bash
helm repo add dagster https://dagster-io.github.io/helm
helm repo update

eval $(minikube docker-env)
docker build -t ameriabank-dagster:latest .

helm install dagster dagster/dagster \
  -f helm/values.yaml \
  --namespace dagster \
  --create-namespace

kubectl port-forward svc/dagster-dagit 3000:80 -n dagster
```

Open http://localhost:3000

## Running the Pipeline

### Backfill (all partitions)

1. Open the Dagster UI
2. Go to **Assets** → select `trx` → **Materialize selected** → All partitions
3. Once complete, select `fetch_daily_data` → **Materialize selected** → All partitions

### Daily (single partition)

The pipeline runs automatically every day at midnight via the built-in schedule. To trigger manually, select a single partition date in the UI.

## Testing

```bash
uv run pytest tests/ -v
```

Tests cover:
- Partition definition validity
- Definitions load correctly