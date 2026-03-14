import dagster as dg
from dagster_duckdb import DuckDBResource
from pathlib import Path

duckdb_resource = DuckDBResource(database=dg.EnvVar("DUCKDB_DATABASE"))
@dg.definitions
def resources():
    return dg.Definitions({"database": duckdb_resource})