from fastapi import FastAPI, HTTPException, Request
import os
import uvicorn
import json
import uuid
import asyncio
import logging
from dotenv import load_dotenv
import psutil

from validators.schema_validator import SchemaValidator
from validators.structure_validator import StructureValidator
from server.models import (
    ValidationResponse,
    DatasetRequest,
    DataDictionaryRequest,
    TASKS,
    TaskInfo,
    TaskStatus,
)
from server.helper import json_rows_to_df, df_to_temp_csv
from validators.wrapper import run_both_validations
from server.models import MAX_TASKS

from logs import setup_otel_logging

app = FastAPI(title="QC Tool API")
logger = logging.getLogger()


# log memory usage per request
_process = psutil.Process(os.getpid())


def _format_bytes(num_bytes: float) -> str:
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if abs(num_bytes) < 1024.0:
            return f"{num_bytes:.2f}{unit}"
        num_bytes /= 1024.0
    return f"{num_bytes:.2f}PB"


@app.middleware("http")
async def memory_logging_middleware(request: Request, call_next):
    before_bytes = _process.memory_info().rss
    response = await call_next(request)
    after_bytes = _process.memory_info().rss

    delta_bytes = after_bytes - before_bytes

    # always log at DEBUG
    logger.debug(
        "[memory][request] %s %s | status=%s | before=%s after=%s delta=%+s",
        request.method,
        request.url.path,
        getattr(response, "status_code", "NA"),
        _format_bytes(before_bytes),
        _format_bytes(after_bytes),
        _format_bytes(delta_bytes),
    )

    # INFO only if memory increased
    if delta_bytes > 0:
        logger.info(
            "[memory][request] MEMORY INCREASED %s %s | status=%s | before=%s after=%s delta=%+s",
            request.method,
            request.url.path,
            getattr(response, "status_code", "NA"),
            _format_bytes(before_bytes),
            _format_bytes(after_bytes),
            _format_bytes(delta_bytes),
        )

    return response


@app.get("/")
async def intro():
    logger.debug("Debug: Received request at root endpoint '/'")
    logger.info("Info: Received request at root endpoint '/'")
    logger.warning("warn: Received request at root endpoint '/'")
    return "QC Tool API entry point"


@app.get("/health")
async def health():
    logger.debug("Health check endpoint called")
    return {"status": 200, "message": "Service online"}


@app.get("/log-test")
async def log_test():
    logger.info("TEST INFO log")
    logger.debug("TEST DEBUG log")
    logger.warning("TEST WARNING log")
    logger.error("TEST ERROR log")
    return {"status": 200}


@app.post("/v1/validate")
async def create_validation_task(
    dataset: DatasetRequest,
    datadic: DataDictionaryRequest,
):
    """Validate structure and schema asynchronously."""
    if len(TASKS) >= MAX_TASKS:
        logger.warning(
            f"[create_validation_task] TASKS exceeded {MAX_TASKS}, clearing store"
        )
        TASKS.clear()  # full reset

    task_id = str(uuid.uuid4())
    TASKS[task_id] = TaskInfo(status=TaskStatus.PENDING)
    logger.info(f"[create_validation_task] Created new task | task_id={task_id}")

    dataset_data = dataset.model_dump()
    datadic_data = datadic.model_dump()
    logger.debug(
        f"[create_validation_task] Frozen dataset keys={list(dataset_data.keys())}, "
        f"datadic keys={list(datadic_data.keys())}"
    )

    async def run_validation():
        logger.info(f"[run_validation] Started async validation | task_id={task_id}")
        TASKS[task_id].status = TaskStatus.RUNNING
        try:
            dataset_obj = DatasetRequest(**dataset_data)
            datadic_obj = DataDictionaryRequest(**datadic_data)
            logger.debug(f"[run_validation] Reconstructed models | task_id={task_id}")

            logger.debug(
                f"[run_validation] Starting run_both_validations for task_id={task_id}"
            )
            result = await run_both_validations(dataset_obj, datadic_obj)
            logger.debug(f"[run_validation] Validation completed | task_id={task_id}")

            TASKS[task_id].status = TaskStatus.DONE
            TASKS[task_id].result = result

            summary = (
                f"dict_keys={list(result.keys())}"
                if isinstance(result, dict)
                else f"type={type(result)}"
            )
            logger.info(
                f"[run_validation] Completed successfully | task_id={task_id} | {summary}"
            )
        except Exception as e:
            TASKS[task_id].status = TaskStatus.ERROR
            TASKS[task_id].error = str(e)
            logger.exception(
                f"[run_validation] ERROR during validation | task_id={task_id}"
            )
        finally:
            logger.debug(f"[run_validation] Task cleanup complete | task_id={task_id}")

            # free memory
            dataset_data.clear()
            datadic_data.clear()
            logger.debug(
                f"[run_validation] Cleared frozen request payloads | task_id={task_id}"
            )

    asyncio.create_task(run_validation())
    logger.info(
        f"[create_validation_task] Background validation task started | task_id={task_id}"
    )
    return {"id": task_id}


@app.get("/v1/validate/{task_id}")
async def get_validation_task(task_id: str):
    """Retrieve results of a prior validation."""
    logger.debug(f"[get_validation_task] Fetching task info | task_id={task_id}")
    task = TASKS.get(task_id)
    if task is None:
        logger.warning(f"[get_validation_task] Task not found | task_id={task_id}")
        raise HTTPException(status_code=404, detail="Task not found")
    logger.debug(
        f"[get_validation_task] Returning status={task.status} | task_id={task_id}"
    )
    return {
        "id": task_id,
        "status": task.status,
        "result": task.result,
        "error": task.error,
    }


@app.post("/v1/validate/core", response_model=ValidationResponse)
async def validate_both(dataset: DatasetRequest, datadic: DataDictionaryRequest):
    """Validate structure and schema synchronously."""
    logger.info("[validate/core] Starting core validation")
    try:
        logger.debug("[validate/core] Running run_both_validations")
        result = await run_both_validations(dataset, datadic)
        logger.info("[validate/core] Validation successful")
        return ValidationResponse(**result)
    except Exception as e:
        logger.exception("[validate/core] Error during validation")
        raise HTTPException(status_code=400, detail=str(e))


@app.post("/v1/validate/structure")
async def validate_structure(dataset: DatasetRequest):
    """Validate structure."""
    logger.info("[validate/structure] Starting structure validation")
    dataset_df = json_rows_to_df(dataset.rows)
    logger.debug(
        f"[validate/structure] Converted dataset to DataFrame with {len(dataset_df)} rows"
    )
    dataset_csv = df_to_temp_csv(dataset_df)
    logger.debug(f"[validate/structure] Temporary CSV created at {dataset_csv}")

    try:
        structure_validator = StructureValidator()
        structure_json = structure_validator.validate_csv(dataset_csv, as_json=True)
        logger.info("[validate/structure] Structure validation complete")
        return json.loads(structure_json)
    except Exception as e:
        logger.exception("[validate/structure] Error during structure validation")
        raise HTTPException(status_code=400, detail=str(e))
    finally:
        try:
            os.remove(dataset_csv)
            logger.debug(f"[validate/structure] Temporary CSV deleted: {dataset_csv}")
        except OSError as e:
            logger.warning(f"[validate/structure] Unable to delete temp file: {e}")


@app.post("/v1/validate/schema")
async def validate_schema(datadic: DataDictionaryRequest):
    """Validate schema."""
    logger.info("[validate/schema] Starting schema validation")
    datadic_df = json_rows_to_df(datadic.rows)
    logger.debug(
        f"[validate/schema] Converted data dictionary to DataFrame with {len(datadic_df)} rows"
    )
    datadic_csv = df_to_temp_csv(datadic_df)
    logger.debug(f"[validate/schema] Temporary CSV created at {datadic_csv}")

    try:
        schema_validator = SchemaValidator()
        schema_json = schema_validator.validate_csv(datadic_csv, as_json=True)
        logger.info("[validate/schema] Schema validation complete")
        return json.loads(schema_json)
    except Exception as e:
        logger.exception("[validate/schema] Error during schema validation")
        raise HTTPException(status_code=400, detail=str(e))
    finally:
        try:
            os.remove(datadic_csv)
            logger.debug(f"[validate/schema] Temporary CSV deleted: {datadic_csv}")
        except OSError as e:
            logger.warning(f"[validate/schema] Unable to delete temp file: {e}")


if __name__ == "__main__":
    load_dotenv()

    setup_otel_logging()
    log_level_name = os.getenv("LOG_LEVEL", "INFO").upper()
    log_level = getattr(logging, log_level_name, logging.INFO)

    logging.basicConfig(level=log_level)
    logging.getLogger().setLevel(log_level)

    logger.info(
        f"Launching uvicorn server for QC Tool API with log level {log_level_name}"
    )
    uvicorn.run(
        "app:app",
        host="0.0.0.0",
        port=8000,
        # reload=True, #reload causes fork error, hides all other logs
    )
