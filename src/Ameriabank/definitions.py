from pathlib import Path
from dagster import load_from_defs_folder

defs = load_from_defs_folder(path_within_project=Path(__file__).parent)