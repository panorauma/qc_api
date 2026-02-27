import polars as pl

from .base_validator import BaseValidator
from . import MINIMAL_VARS


class SchemaValidator(BaseValidator):
    STANDARD_HEADERS = [
        "VariableName",
        "Title",
        "Unit_of_Measure",
        "Description",
        "Comments",
        "PermittedValues",
        "DataType",
        "MaximumValue",
        "MinimumValue",
    ]

    def validate_csv(
        self, datadic_path: str, sch_checks: str = "all", as_json: bool = False
    ):
        ldf = pl.scan_csv(datadic_path)
        headers = ldf.collect_schema().names()

        checks = (
            {
                "dic_header",
                "min_vars",
                "miss_title",
                "miss_description",
                "other_symbols",
                "pos1_char",
                "over_60char",
            }
            if sch_checks == "all"
            else {
                "dic_header",
                "miss_title",
                "miss_description",
                "other_symbols",
                "pos1_char",
                "over_60char",
            }
        )

        results = {}

        # Required headers
        if "dic_header" in checks:
            missing = [h for h in self.STANDARD_HEADERS if h not in headers]
            results["data_dic_headers"] = {
                "count": len(missing),
                "issues": missing,
            }

        if "VariableName" not in headers:
            return self.export_long_table(results, as_json)

        # Collect required columns only
        df = ldf.select(
            pl.col("VariableName"),
            pl.col("Title") if "Title" in headers else pl.lit(None).alias("Title"),
            pl.col("Description")
            if "Description" in headers
            else pl.lit(None).alias("Description"),
        ).collect(streaming=True)

        vars_df = df.select("VariableName").drop_nulls()
        var_list = vars_df["VariableName"].to_list()

        # Minimal variables
        if "min_vars" in checks:
            missing = [v for v in MINIMAL_VARS if v not in var_list]
            results["missing_minimal_var"] = {
                "count": len(missing),
                "issues": missing,
            }

        # Missing Title
        if "miss_title" in checks:
            bad = (
                df.filter(self.is_blank_expr("Title"))
                .select("VariableName")
                .drop_nulls()
                .to_series()
                .to_list()
            )
            results["missing_title"] = {
                "count": len(bad),
                "issues": bad,
            }

        # Missing Description
        if "miss_description" in checks:
            bad = (
                df.filter(self.is_blank_expr("Description"))
                .select("VariableName")
                .drop_nulls()
                .to_series()
                .to_list()
            )
            results["missing_description"] = {
                "count": len(bad),
                "issues": bad,
            }

        # Invalid characters in VariableName
        if "other_symbols" in checks:
            bad = (
                vars_df.filter(pl.col("VariableName").str.contains(r"[^A-Za-z0-9_.]"))
                .to_series()
                .to_list()
            )
            results["other_symbols"] = {
                "count": len(bad),
                "issues": bad,
            }

        # First character must be a letter (FIXED)
        if "pos1_char" in checks:
            bad = (
                vars_df.filter(~pl.col("VariableName").str.contains(r"^[A-Za-z]"))
                .to_series()
                .to_list()
            )
            results["pos1_char"] = {
                "count": len(bad),
                "issues": bad,
            }

        # Variable name too long
        if "over_60char" in checks:
            bad = (
                vars_df.filter(pl.col("VariableName").str.len_chars() > 60)
                .to_series()
                .to_list()
            )
            results["over_60char"] = {
                "count": len(bad),
                "issues": bad,
            }

        return self.export_long_table(results, as_json)
