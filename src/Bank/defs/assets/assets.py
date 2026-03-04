import dagster as dg
from dagster import AssetExecutionContext, MaterializeResult
import polars as pl
from .constants import TRX_PARQ_PATH, DATE_FORMAT

# TODO: seperate the loader from the asset
daily_partition_def = dg.DailyPartitionsDefinition(start_date="2024-01-01")
@dg.asset(partition_def = daily_partition_def)
def trx(context: dg.AssetExecutionContext, database: dg.DuckDBResource) -> dg.MaterializeResult:
    trx_parq_path = TRX_PARQ_PATH
    partition_date_str = context.asset_partition_key
    tx_lazy = pl.scan_parquet(trx_parq_path).filter(pl.col("event_time").dt.date()==pl.date(partition_date_str, format=DATE_FORMAT))
    table_name = "trx"
    trx_df = tx_lazy.collect()
    # TODO: make it effecient and lose the unnecessary 
    with database.get_connection() as conn:
        conn.execute(f"CREATE TABLE IF NOT EXISTS {table_name} AS SELECT * FROM tx_df WHERE 1=0")
        conn.execute(f"DELETE FROM trx WHERE CAST(event_time AS DATE) = '{partition_date_str}'")
        conn.execute("INSERT INTO trx SELECT * FROM trx_df")
    context.log.info(f"Processing transactions for partition: {context.asset_partition_key}")
    
    # read the parqs to polars and load to duckdb

   
    row_count = tx_lazy.height
    context.add_output_metadata({"partition": partition_date_str,
                                 "row_count": row_count})
    return MaterializeResult(metadata ={"table": table_name})
    
@dg.asset(partition_def = daily_partition_def)
def fetch_daily_data(
    context: AssetExecutionContext,
    database: dg.DuckDBResource,
    trx_data: pl.DataFrame) -> MaterializeResult:
    context.log.info("Fetching daily data")
    features_df = (
        trx_data.filter(pl.col("date") == context.asset_partition_key)
        .group_by("client_id")
        .agg(pl.col("amount").mean().alias("avg_daily_spend"))
    )
    target_date = context.asset_partition_key
   
    row_count = features_df.height
    context.add_output_metadata({"partition": target_date,
                                 "row_count": row_count})
    with database.get_connection() as conn:
        conn.execute("INSERT INTO features SELECT * FROM features_df")
@dg.asset
def trx_file(context: AssetExecutionContext) -> MaterializeResult:
    context.log.info("Processing trx file")
    
@dg.asset
def geo(context: AssetExecutionContext) -> MaterializeResult:
    context.log.info("Processing geo data")


@dg.asset
def dialog(context: AssetExecutionContext) -> MaterializeResult:
    context.log.info("Processing dialog data")