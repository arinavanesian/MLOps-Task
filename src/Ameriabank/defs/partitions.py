import dagster as dg
from .assets.constants import START_DATE, END_DATE, DATE_FORMAT

monthly_partition_def = dg.MonthlyPartitionsDefinition(
    start_date=START_DATE, 
    end_date=END_DATE, 
    fmt=DATE_FORMAT)