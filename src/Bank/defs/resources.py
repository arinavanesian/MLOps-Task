import dagster as dg
from dagster import DuckDBResource

duckdb_resource = DuckDBResource(database=dg.EnvVar("DUCKDB_DATABASE"))
@dg.definitions
def resources() -> dg.Definitions:
    return dg.Definitions(resources={"database": duckdb_resource})