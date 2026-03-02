import dagster as dg
from dagster import AssetExecutionContext, MaterializeResult
import polars as pl

# TODO: seperate the loader from the asset
daily_partition_def = dg.DailyPartitionsDefinition(start_date="2024-01-01")
@dg.asset(partition_def = daily_partition_def)
def trx(context: dg.AssetExecutionContext, duckdb_resource: dg.DuckDBResource) -> dg.MaterializeResult:
    data_path = "src/Ameriabank/defs/data/"
    trx_parq_path = f"{data_path}detail/trx/**/*.parquet"
    partition_date_str = context.asset_partition_key
    tx_lazy = pl.scan_parquet(trx_parq_path).filter(pl.col("event_time").dt.date()==pl.date(partition_date_str))
    table_name = "trx"
    trx_df = tx_lazy.collect()

    with duckdb_resource.get_connection() as conn:
        conn.execute(f"CREATE TABLE IF NOT EXISTS {table_name} AS SELECT * FROM tx_df WHERE 1=0")
        conn.execute(f"DELETE FROM trx WHERE CAST(event_time AS DATE) = '{partition_date_str}'")
        conn.execute("INSERT INTO trx SELECT * FROM df")
    context.log.info(f"Processing transactions for partition: {context.asset_partition_key}")
    
    # read the parqs to polars and load to duckdb

   
    row_count = tx_lazy.height
    context.add_output_metadata({"partition": partition_date_str,
                                 "row_count": row_count})
    return MaterializeResult(metadata ={"table": table_name})
    
@dg.asset
def fetch_daily_data(
    context: AssetExecutionContext,
    duckdb_resource: dg.DuckDBResource,
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
    with duckdb_resource.get_connection() as conn:
        conn.execute("INSERT INTO features SELECT * FROM features_df")

@dg.asset
def geo(context: AssetExecutionContext) -> MaterializeResult:
    context.log.info("Processing geo data")


@dg.asset
def dialog(context: AssetExecutionContext) -> MaterializeResult:
    context.log.info("Processing dialog data")