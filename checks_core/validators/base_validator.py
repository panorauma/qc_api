import polars as pl
from typing import Union


class BaseValidator:
    def _collect(self, df: Union[pl.DataFrame, pl.LazyFrame]) -> pl.DataFrame:
        return df.collect() if isinstance(df, pl.LazyFrame) else df

    @staticmethod
    def is_blank_expr(col: str) -> pl.Expr:
        """
        Blank detection: NULL, "", "na" (case-insensitive).
        """
        s = pl.col(col).cast(pl.Utf8, strict=False)
        return s.is_null() | (s == "") | (s.str.to_lowercase() == "na")

    @staticmethod
    def export_long_table(results: dict, as_json: bool = False):
        import json

        rows = [
            {
                "check": check,
                "issue_type": "count",
                "count": payload["count"],
                "issue": payload["issues"],
            }
            for check, payload in results.items()
        ]

        if as_json:
            return json.dumps(rows, indent=4)

        return pl.DataFrame(rows)
