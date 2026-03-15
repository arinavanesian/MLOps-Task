from pathlib import Path

_DOCKER_DATA = Path("/data/detail")
_LOCAL_DATA = Path(__file__).parent.parent.parent / "data" / "detail"
_DATA_ROOT = _DOCKER_DATA if _DOCKER_DATA.exists() else _LOCAL_DATA

START_DATE = "2020-12-31"
END_DATE = "2022-01-01"
DATE_FORMAT = "%Y-%m-%d"

TRX_PARQ_PATH = str(_DATA_ROOT / "trx" / "fold=*" / "*.parquet")
GEO_PARQ_PATH = str(_DATA_ROOT / "geo" / "fold=*" / "*.parquet")
DIALOG_PARQ_PATH = str(_DATA_ROOT / "dialog" / "fold=*" / "*.parquet")