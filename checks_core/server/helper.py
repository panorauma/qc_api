import polars as pl
import tempfile
from typing import List, Dict, Any


def _format_bytes(num_bytes: float) -> str:
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if abs(num_bytes) < 1024.0:
            return f"{num_bytes:.2f}{unit}"
        num_bytes /= 1024.0
    return f"{num_bytes:.2f}PB"


def json_rows_to_df(rows: List[Dict[str, Any]]) -> pl.DataFrame:
    """
    Convert json as list of dicts into polars dataframe.
    """
    if not rows:
        return pl.DataFrame([])  # empty df, no cols

    return pl.DataFrame(rows)


def df_to_temp_csv(df: pl.DataFrame) -> str:
    """
    Write polars dataframe to temporary CSV file and return its path. Caller is responsible for deleting file.
    """
    tmp = tempfile.NamedTemporaryFile(delete=False, suffix=".csv")
    tmp.close()
    df.write_csv(tmp.name)

    return tmp.name
