import polars as pl
import hashlib
from .base_validator import BaseValidator
from . import MINIMAL_VARS


# aggressive memory management: keep hashes, drop concatenated strings immediately
def detect_duplicate_rows(df: pl.DataFrame):
    row_hash = df.select(
        pl.concat_str(pl.all(), separator="|").hash().alias("row_hash")
    )
    duplicated_mask = row_hash["row_hash"].is_duplicated()
    dup_count: int = duplicated_mask.sum()
    dup_hashes = row_hash.filter(duplicated_mask).to_series().to_list()

    del row_hash, duplicated_mask  # intermediate objects

    return {"count": dup_count, "issues": dup_hashes}


class StructureValidator(BaseValidator):
    def validate_csv(
        self, file_path: str, str_checks: str = "all", as_json: bool = False
    ):
        ldf = pl.scan_csv(file_path)
        schema = ldf.collect_schema()
        columns = schema.names()

        checks = (
            {
                "blank_header",
                "dup_header",
                "blank_row",
                "blank_column",
                "dup_row",
                "dup_column",
                "min_vars",
            }
            if str_checks == "all"
            else {
                "blank_header",
                "dup_header",
                "blank_row",
                "blank_column",
                "dup_row",
                "dup_column",
            }
        )

        results = {}

        # Blank header
        if "blank_header" in checks:
            idx = [i for i, c in enumerate(columns) if c == ""]
            results["blank_header"] = {"count": len(idx), "issues": idx}

        # Duplicated headers
        if "dup_header" in checks:
            dup = (
                pl.Series("header", columns)
                .value_counts()
                .filter(pl.col("count") > 1)
                .select("header")
                .to_series()
                .to_list()
            )
            results["duplicated_header"] = {"count": len(dup), "issues": dup}

        # Only collect if any content-based checks are required
        content_checks = {"blank_row", "blank_column", "dup_row", "dup_column"}
        if checks.intersection(content_checks):
            df = ldf.collect(streaming=True)

            # Blank rows
            if "blank_row" in checks:
                mask = df.select(
                    pl.all_horizontal([self.is_blank_expr(c) for c in columns])
                )
                count = mask.filter(pl.all()).height
                results["blank_row"] = {"count": count, "issues": []}
                del mask

            # Blank columns
            if "blank_column" in checks:
                blank_cols = [
                    c for c in columns if df.select(self.is_blank_expr(c).all()).item()
                ]
                results["blank_column"] = {
                    "count": len(blank_cols),
                    "issues": blank_cols,
                }

            # Duplicated rows
            if "dup_row" in checks:
                results["duplicated_row"] = detect_duplicate_rows(df)

            # Duplicated columns
            if "dup_column" in checks:
                col_hashes = {
                    c: hashlib.md5("".join(map(str, df[c])).encode()).hexdigest()
                    for c in columns
                }
                seen = {}
                dup_cols = []
                for c, h in col_hashes.items():
                    if h in seen:
                        dup_cols.append(c)
                    else:
                        seen[h] = c
                results["duplicated_column"] = {
                    "count": len(dup_cols),
                    "issues": dup_cols,
                }

            del df

        # Minimal required variables
        if "min_vars" in checks:
            missing = [v for v in MINIMAL_VARS if v not in columns]
            results["minimal_var"] = {"count": len(missing), "issues": missing}

        del schema  # no longer needed

        return self.export_long_table(results, as_json)
