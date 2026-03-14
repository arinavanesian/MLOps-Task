import dagster as dg
from .assets.assets import daily_partition_def
from .partitions import monthly_partition_def

fetch_daily_data = dg.AssetSelection.assets(["fetch_daily_data"])
daily_job = dg.define_asset_job("daily_job",
 selection = dg.AssetSelection.assets("trx", "fetch_daily_data") 
)

other_job = dg.define_asset_job("other_job", 
                                selection = dg.AssetSelection.all()-fetch_daily_data)
