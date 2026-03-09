import dagster as dg
from dagster_duckdb import DuckDBResource
from pathlib import Path

_db_path = dg.EnvVar("DUCKDB_DATABASE").get_value()
Path(_db_path).parent.mkdir(parents=True, exist_ok=True)

duckdb_resource = DuckDBResource(database=_db_path)