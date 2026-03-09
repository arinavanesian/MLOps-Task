import dagster as dg
from dagster import AssetExecutionContext, MaterializeResult
from dagster_duckdb import DuckDBResource
import polars as pl
from .constants import TRX_PARQ_PATH, DATE_FORMAT


# TODO: seperate the loader from the asset
daily_partition_def = dg.DailyPartitionsDefinition(
    start_date="2021-01-01",
    end_date="2022-12-31")

@dg.asset(partitions_def = daily_partition_def,
          backfill_policy = dg.BackfillPolicy.single_run())
def trx(context: dg.AssetExecutionContext, database: DuckDBResource) -> dg.MaterializeResult:
    trx_parq_path = TRX_PARQ_PATH
    partition_range = context.partition_key_range
    start_date = partition_range.start
    end_date = partition_range.end
    # TODO: partition_date_str = partition_date_str[-2:]
    tx_lazy = pl.scan_parquet(trx_parq_path).filter((pl.col("event_time").cast(pl.Date)>=pl.lit(start_date)
    .str.to_date(format=DATE_FORMAT)) & 
    (pl.col("event_time").cast(pl.Date)<=pl.lit(end_date)
    .str.to_date(format=DATE_FORMAT)))
    table_name = "trx"
    trx_df = tx_lazy.collect()
    # TODO: make it effecient and lose the unnecessary 
    with database.get_connection() as conn:
        conn.execute(f"CREATE TABLE IF NOT EXISTS {table_name} AS SELECT * FROM trx_df LIMIT 0")
        # TODO:log the number of rows before deletion
        conn.execute(f"DELETE FROM {table_name} WHERE CAST(event_time AS DATE) BETWEEN '{start_date}' AND '{end_date}'")
        # TODO:log inserting the data for this partition
        conn.execute(f"INSERT INTO {table_name} SELECT * FROM trx_df")
    context.log.info(f"Processing transactions for partition: {start_date} - {end_date}")
    
    # read the parqs to polars and load to duckdb

   
    row_count = trx_df.height
    context.add_output_metadata({"partition_start": start_date,
                                 "partition_end": end_date,
                                 "row_count": row_count})
    return MaterializeResult(metadata ={"table": table_name})
    
@dg.asset(partitions_def=daily_partition_def,
          backfill_policy=dg.BackfillPolicy.single_run(),
          deps=["trx"])
def fetch_daily_data(context: AssetExecutionContext, database: DuckDBResource) -> MaterializeResult:
    partition_range = context.partition_key_range
    start_date = partition_range.start
    end_date = partition_range.end

    with database.get_connection() as conn:
        trx_df = conn.execute(f"""
            SELECT * FROM trx 
            WHERE CAST(event_time AS DATE) BETWEEN '{start_date}' AND '{end_date}'
        """).pl()


    features_df = (
    trx_df
    .sort("event_time")
    .select(
        pl.col("event_time").dt.date().alias("event_time"),  # cast to date
        pl.col("client_id"),
        pl.col("amount"),
        pl.col("event_type"),
    )
    .rolling(index_column="event_time", period="1mo", group_by="client_id")
    .agg([
        pl.col("amount").mean().alias("avg_amount"),
        pl.col("amount").sum().alias("sum_amount"),
        pl.col("event_type").count().alias("txn_count"),
    ])
    )

    with database.get_connection() as conn:
        # Create from explicit schema, not from features_df reference
        conn.execute("""
            CREATE TABLE IF NOT EXISTS features (
                client_id VARCHAR,
                event_type VARCHAR,
                date DATE,
                avg_daily_amount DOUBLE,
                daily_txn_count BIGINT
            )
        """)
        # Delete full range not just start_date
        conn.execute(f"""
            DELETE FROM features 
            WHERE date BETWEEN '{start_date}' AND '{end_date}'
        """)
        conn.execute("INSERT INTO features SELECT * FROM features_df")

    row_count = features_df.height
    context.add_output_metadata({"partition_start": start_date,
                                 "partition_end": end_date,
                                 "row_count": row_count})
    return MaterializeResult(metadata={"partition_start": start_date, "row_count": row_count})