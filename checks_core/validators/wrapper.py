import json
import os

from server.models import DatasetRequest, DataDictionaryRequest
from server.helper import json_rows_to_df, df_to_temp_csv
from validators.structure_validator import StructureValidator
from validators.schema_validator import SchemaValidator


async def run_both_validations(
    dataset: DatasetRequest, datadic: DataDictionaryRequest
) -> dict:
    """
    Wrapper function run structure and schema checks.

    expect dataset and datadic in json format
    """
    dataset_df = json_rows_to_df(dataset.rows)
    datadic_df = json_rows_to_df(datadic.rows)

    dataset_csv = df_to_temp_csv(dataset_df)
    datadic_csv = df_to_temp_csv(datadic_df)

    try:
        structure_validator = StructureValidator()
        schema_validator = SchemaValidator()

        structure_json = structure_validator.validate_csv(dataset_csv, as_json=True)
        schema_json = schema_validator.validate_csv(datadic_csv, as_json=True)

        structure_data = json.loads(structure_json)
        schema_data = json.loads(schema_json)

        # return a plain dict, TaskInfo.result can store this
        return {
            "structure": structure_data,
            "schema": schema_data,
        }

    finally:
        for path in (dataset_csv, datadic_csv):
            try:
                os.remove(path)
            except OSError:
                pass
