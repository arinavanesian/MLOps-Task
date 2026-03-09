from pathlib import Path
import polars as pl

data_path = Path(__file__).parent.parent / "data" / "detail" / "trx" / "fold=*" / "*.parquet"
df = pl.scan_parquet(str(data_path))
print(data_path)
print(df.select(
    pl.col("event_time").min().alias("min_date"),
    pl.col("event_time").max().alias("max_date")
).collect())