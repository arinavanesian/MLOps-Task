import dagster as dg

from .jobs import daily_job

daily_schedule =dg.build_schedule_from_partitioned_job(daily_job)

