import polars as pl
import requests
import json
from typing import Any, Dict, List
from pathlib import Path
import time
from dotenv import load_dotenv  # local only
import os

load_dotenv()

deploy_env_raw: str = os.getenv("DEPLOY_ENVIRONMENT")
if deploy_env_raw not in ["dev", "prod"]:
    DEPLOY_ENVIRONMENT = "dev"
else:
    DEPLOY_ENVIRONMENT = deploy_env_raw
# print(DEPLOY_ENVIRONMENT)

API_BASE: str = (
    os.getenv("API_BASE") if DEPLOY_ENVIRONMENT == "prod" else "http://localhost:8000"
)
# print(API_BASE)

# async is default
ASYNC_URL = f"{API_BASE}/v1/validate"
ASYNC_STATUS_URL = f"{API_BASE}/v1/validate/{{task_id}}"
SYNC_URL = f"{API_BASE}/v1/validate/core"


def file_to_rows(file_path: str | Path) -> List[Dict[str, Any]]:
    """
    Read csv or json and return list of row dicts. Auto-detects format by file extension.
    """
    path = Path(file_path)

    if path.suffix.lower() == ".csv":
        df = pl.read_csv(path, null_values="NA")
        return df.to_dicts()

    elif path.suffix.lower() == ".json":
        # handle both JSON Lines and array-of-objects
        with open(path, "r") as f:
            data = json.load(f)

        if isinstance(data, list):
            # array of objects
            if data and isinstance(data[0], dict):
                return data
            else:
                raise ValueError("JSON array must contain objects")
        elif isinstance(data, dict) and "rows" in data:
            return data["rows"]
        else:
            # single object -> list with one object
            return [data]

    else:
        raise ValueError(
            f"Unsupported file extension: {path.suffix}. Use .csv or .json"
        )


def build_request_body(dataset_file: str, datadic_file: str) -> Dict[str, Any]:
    """
    Build request body from two files (csv or json).
    """
    dataset_rows = file_to_rows(dataset_file)
    datadic_rows = file_to_rows(datadic_file)

    return {
        "dataset": {"rows": dataset_rows},
        "datadic": {"rows": datadic_rows},
    }


def call_async_validation(dataset_path: str, datadic_path: str) -> str:
    """
    Call POST /v1/validate and return task_id.
    """
    payload = build_request_body(dataset_path, datadic_path)
    resp = requests.post(ASYNC_URL, json=payload)

    print("Create task status:", resp.status_code)
    try:
        data = resp.json()
    except Exception:
        print("Non-JSON response:", resp.text)
        raise

    if resp.status_code != 200:
        raise RuntimeError(f"Error creating task: {data}")

    task_id = data.get("id")
    if not task_id:
        raise RuntimeError(f"No task id returned: {data}")

    print(f"Created task id: {task_id}")
    return task_id


def poll_task(
    task_id: str, interval_seconds: float = 1.0, max_attempts: int = 60
) -> Dict[str, Any]:
    """
    Poll GET /v1/validate/{task_id} until DONE or ERROR, or until max_attempts.
    """
    for attempt in range(max_attempts):
        url = ASYNC_STATUS_URL.format(task_id=task_id)
        resp = requests.get(url)
        print(f"[poll {attempt + 1}] status code:", resp.status_code)

        if resp.status_code == 404:
            print("Task not found")
            return {"error": "Task not found"}

        try:
            data = resp.json()
        except Exception:
            print("Non-JSON response:", resp.text)
            raise

        status = data.get("status")
        print("Task status:", status)

        if status in ("DONE", "ERROR"):
            return data

        time.sleep(interval_seconds)

    return {"error": "Polling timed out", "task_id": task_id}


def call_sync_validation(dataset_path: str, datadic_path: str) -> Dict[str, Any]:
    """
    Call POST /v1/validate/core and return the result directly.
    """
    payload = build_request_body(dataset_path, datadic_path)
    resp = requests.post(SYNC_URL, json=payload)

    print("Sync call status:", resp.status_code)
    try:
        data = resp.json()
    except Exception:
        print("Non-JSON response:", resp.text)
        raise

    if resp.status_code != 200:
        raise RuntimeError(f"Error from sync validate/core: {data}")

    return data


def main(mode: str, dataset_path: str, datadic_path: str):
    try:
        if mode == "async":
            # create task
            task_id = call_async_validation(dataset_path, datadic_path)
            # poll for result
            result = poll_task(task_id, interval_seconds=1.0, max_attempts=60)

            print("Final task response JSON:")
            print(json.dumps(result, indent=2))

        elif mode == "sync":
            result = call_sync_validation(dataset_path, datadic_path)
            print("Sync validation response JSON:")
            print(json.dumps(result, indent=2))

        else:
            print(f"Unknown mode '{mode}', use 'async' or 'sync'")

    except FileNotFoundError as e:
        print(f"File not found: {e}")
    except Exception as e:
        # if resp was defined, show raw response to help debugging
        print("Error:", str(e))


if __name__ == "__main__":
    # very small dataset
    main(
        "async", "./data/incorrect_dataset.csv", "./data/incorrect_data_dictionary.csv"
    )

    # large dataset (~220 MB)
    main("async", "./data/metropt.csv", "./data/incorrect_data_dictionary.csv")

    # empty dataset
    # main("async", "./data/empty.csv", "./data/incorrect_data_dictionary.csv")
