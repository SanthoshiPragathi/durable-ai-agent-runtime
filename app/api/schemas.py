"""Request and response models for the public API."""

import uuid
from datetime import datetime
from typing import Any

from pydantic import BaseModel, ConfigDict, Field


class CreateRunRequest(BaseModel):
    input: dict[str, Any] = Field(default_factory=dict, description="Keyword arguments for the workflow")


class RunOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    run_id: uuid.UUID
    workflow_name: str
    workflow_version: int
    status: str
    input_data: dict[str, Any]
    result_data: Any = None
    current_step: str | None = None
    parent_run_id: uuid.UUID | None = None
    error_message: str | None = None
    created_at: datetime
    started_at: datetime | None = None
    completed_at: datetime | None = None


class EventOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    event_id: int
    run_id: uuid.UUID
    sequence_number: int
    event_type: str
    step_name: str | None = None
    input_hash: str | None = None
    result_data: Any = None
    created_at: datetime


class WorkflowOut(BaseModel):
    name: str
    version: int


class ErrorOut(BaseModel):
    detail: str
