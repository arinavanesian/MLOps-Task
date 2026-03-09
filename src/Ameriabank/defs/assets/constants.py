from pathlib import Path

_DATA_ROOT = Path(__file__).parent.parent.parent / "data" / "detail"

TRX_PARQ_PATH = str(_DATA_ROOT / "trx" / "fold=*" / "*.parquet")
GEO_PARQ_PATH = str(_DATA_ROOT / "geo" / "fold=*" / "*.parquet")
DIALOG_PARQ_PATH = str(_DATA_ROOT / "dialog" / "fold=*" / "*.parquet")
DATE_FORMAT = "%Y-%m-%d"