from pathlib import Path
import dagster as dg 
from dagster import definitions, load_from_defs_folder
from  .defs.jobs import daily_job
from .defs.schedules import daily_schedule
from .defs.resources import duckdb_resource
from .defs.assets.assets import trx, fetch_daily_data

defs = dg.Definitions(
    assets=[trx, fetch_daily_data],
    jobs=[daily_job],
    schedules=[daily_schedule],
    resources={"database": duckdb_resource},
)
