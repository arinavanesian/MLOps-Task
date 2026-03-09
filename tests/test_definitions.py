import dagster as dg
from dagster import build_asset_context
from Ameriabank.defs.assets.assets import daily_partition_def

def test_partition_def():
    partitions = daily_partition_def.get_partition_keys()
    assert "2021-01-01" in partitions
    assert "2022-12-31" in partitions

def test_definitions_load():
    from Ameriabank.definitions import defs
    assert defs is not None