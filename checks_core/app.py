import os
import uvicorn
import json
import uuid
import asyncio
import psutil
from contextlib import asynccontextmanager
from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException

from validators.schema_validator import SchemaValidator
from validators.structure_validator import StructureValidator
from server.models import ValidationResponse, DatasetRequest, DataDictionaryRequest
from server.helper import json_rows_to_df, df_to_temp_csv
from validators.wrapper import run_both_validations
from server.mariadb import (
    init_db_pool,
    close_db_pool,
    create_task,
    update_task,
    get_task,
    create_table,
)

# configs
load_dotenv()


@asynccontextmanager
async def lifespan(_app: FastAPI):
    await init_db_pool()
    await create_table()
    yield
    await close_db_pool()


app = FastAPI(title="QC Tool API", lifespan=lifespan)


# routes start
@app.get("/")
async def intro():
    return "QC Tool API entry point"


@app.get("/health")
async def health():
    return {"status": 200, "message": "Service online"}


@app.post("/v1/validate")
async def create_validation_task(
    dataset: DatasetRequest, datadic: DataDictionaryRequest
):
    task_id = str(uuid.uuid4())
    await create_task(task_id)

    dataset_data = dataset.model_dump()
    datadic_data = datadic.model_dump()

    async def run_validation():
        try:
            await update_task(task_id, "RUNNING")
            dataset_obj = DatasetRequest(**dataset_data)
            datadic_obj = DataDictionaryRequest(**datadic_data)
            result = await run_both_validations(dataset_obj, datadic_obj)
            await update_task(task_id, "DONE", result=result)
        except Exception as e:
            await update_task(task_id, "ERROR", error=str(e))
        finally:
            dataset_data.clear()
            datadic_data.clear()

    asyncio.create_task(run_validation())
    return {"id": task_id}


@app.get("/v1/validate/{task_id}")
async def get_validation_task(task_id: str):
    task = await get_task(task_id)
    if not task:
        raise HTTPException(status_code=404, detail="Task not found")
    return {
        "id": task["id"],
        "status": task["status"],
        "result": json.loads(task["result_json"]) if task["result_json"] else None,
        "error": task["error"],
    }


@app.post("/v1/validate/core", response_model=ValidationResponse)
async def validate_both(dataset: DatasetRequest, datadic: DataDictionaryRequest):
    try:
        result = await run_both_validations(dataset, datadic)
        return ValidationResponse(**result)
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@app.post("/v1/validate/structure")
async def validate_structure(dataset: DatasetRequest):
    dataset_df = json_rows_to_df(dataset.rows)
    dataset_csv = df_to_temp_csv(dataset_df)
    try:
        validator = StructureValidator()
        return json.loads(validator.validate_csv(dataset_csv, as_json=True))
    finally:
        try:
            os.remove(dataset_csv)
        except OSError:
            pass


@app.post("/v1/validate/schema")
async def validate_schema(datadic: DataDictionaryRequest):
    datadic_df = json_rows_to_df(datadic.rows)
    datadic_csv = df_to_temp_csv(datadic_df)
    try:
        validator = SchemaValidator()
        return json.loads(validator.validate_csv(datadic_csv, as_json=True))
    finally:
        try:
            os.remove(datadic_csv)
        except OSError:
            pass


if __name__ == "__main__":
    uvicorn.run(
        "app:app",
        host="0.0.0.0",
        port=int(os.getenv("PORT", 8000)),
        reload=False,
    )
