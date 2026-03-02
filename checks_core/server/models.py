from pydantic import BaseModel
from typing import List, Dict, Any
from enum import Enum


class DatasetRequest(BaseModel):
    rows: List[Dict[str, Any]]


class DataDictionaryRequest(BaseModel):
    rows: List[Dict[str, Any]]


class ValidationResponse(BaseModel):
    structure: Any
    schema: Any


class TaskStatus(str, Enum):
    PENDING = "PENDING"
    RUNNING = "RUNNING"
    DONE = "DONE"
    ERROR = "ERROR"


# final response
class TaskInfo(BaseModel):
    status: TaskStatus
    result: dict | None = None
    error: str | None = None
