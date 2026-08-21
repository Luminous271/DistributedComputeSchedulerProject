from datetime import datetime
from enum import Enum
from typing import Any

from pydantic import BaseModel, Field

# class defining what stage of life cycle job is in using enums. 
class JobStatus(str, Enum):
    QUEUED = "queued"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    RETRYING = "retrying"
    TIMED_OUT = "timed_out"

class JobRequest(BaseModel):
    type: str
    payload: dict[str, Any]
    priority: int = Field(default=0, ge=0, le=10)
    timeout_seconds: int | None = Field(
        default=60,
        gt=0,
        le=3600,
    )

# class defining what a Job has/is 
class Job(BaseModel):
    id: str
    type: str
    payload: dict[str, Any]
    priority: int = 0
    status: JobStatus = JobStatus.QUEUED
    created_at: datetime
    started_at: datetime | None = None
    completed_at: datetime | None = None
    worker_id: str | None = None
    retry_count: int = 0
    result: Any | None = None
    max_retries: int = 3
    retry_count: int = 0
    retry_at: datetime | None = None
    message_id: str | None = None
    timeout_seconds: int | None = Field(
            default=60,
            gt=0,
            le=3600,
        )