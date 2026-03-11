from datetime import datetime, timedelta

import dagster as dg
from dagster import AssetExecutionContext, MaterializeResult
from dagster_duckdb import DuckDBResource
import polars as pl
from .constants import TRX_PARQ_PATH, DATE_FORMAT


# TODO: seperate the loader from the asset
daily_partition_def = dg.DailyPartitionsDefinition(
    start_date="2020-12-31",
    end_date="2023-01-01")

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

    # Fetch with 30-day lookback for rolling window
    start_dt = datetime.strptime(start_date, DATE_FORMAT)
    lookback_start = (start_dt - timedelta(days=30)).strftime(DATE_FORMAT)

    with database.get_connection() as conn:
        trx_df = conn.execute(f"""
            SELECT event_time, client_id, amount, event_type
            FROM trx
            WHERE CAST(event_time AS DATE) BETWEEN '{lookback_start}' AND '{end_date}'
        """).pl()

    features_df = (
        trx_df
        .sort("event_time")
        .with_columns(pl.col("event_time").dt.date().alias("event_time"))
        .rolling(index_column="event_time", period="1mo", group_by="client_id")
        .agg([
            pl.col("amount").mean().alias("avg_amount"),
            pl.col("amount").sum().alias("sum_amount"),
            pl.col("event_type").count().alias("txn_count"),
        ])
        # trim to actual partition range, exclude lookback rows
        .filter(
            (pl.col("event_time") >= pl.lit(start_date).str.to_date(DATE_FORMAT)) &
            (pl.col("event_time") <= pl.lit(end_date).str.to_date(DATE_FORMAT))
        )
    )

    with database.get_connection() as conn:
        # Schema matches actual features_df columns: client_id, event_time, avg_amount, sum_amount, txn_count
        conn.execute("""
            CREATE TABLE IF NOT EXISTS features (
                client_id VARCHAR,
                event_time DATE,
                avg_amount DOUBLE,
                sum_amount DOUBLE,
                txn_count BIGINT
            )
        """)
        conn.execute(f"""
            DELETE FROM features
            WHERE event_time BETWEEN '{start_date}' AND '{end_date}'
        """)
        conn.register("features_df", features_df)
        conn.execute("INSERT INTO features SELECT * FROM features_df")
        conn.unregister("features_df")

    row_count = features_df.height
    context.add_output_metadata({"partition_start": start_date,
                                 "partition_end": end_date,
                                 "row_count": row_count})
    return MaterializeResult(metadata={"partition_start": start_date, "row_count": row_count})