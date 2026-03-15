FROM python:3.12-slim

WORKDIR /app

RUN pip install uv

COPY pyproject.toml uv.lock ./
COPY src/ ./src/

RUN uv sync --frozen

ENV PYTHONPATH=/app/src
ENV DAGSTER_HOME=/dagster_home
ENV DUCKDB_DATABASE=/dagster_home/bank.db

RUN mkdir -p /dagster_home
COPY dagster.yaml /dagster_home/dagster.yaml
CMD ["uv", "run", "dagster", "dev", "-h", "0.0.0.0", "-p", "3000", "-m", "Ameriabank"]