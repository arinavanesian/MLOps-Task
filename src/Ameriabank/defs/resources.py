import dagster as dg
from dagster import DuckDBResource, Definitions

duckdb_resource = DuckDBResource(database="ameriabank.duckdb")
@dg.definitions
def resources() -> dg.Definitions:
    return dg.Definitions(resources={"duckdb": duckdb_resource})