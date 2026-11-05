"""Request and response models for the public API."""

import uuid
from datetime import datetime
from typing import Any, Literal

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
    lease_owner: str | None = None
    lease_expires_at: datetime | None = None
    fencing_token: int = 0
    waiting_signal: str | None = None


class WorkerOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    worker_id: str
    hostname: str
    pid: int
    status: str
    started_at: datetime
    last_heartbeat_at: datetime
    alive: bool = Field(description="ACTIVE and heard from within one lease period")


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


class RefundOut(BaseModel):
    refund_id: str
    order_id: str
    amount: int
    status: str
    idempotency_key: str | None = None
    reference: str | None = None


class EmailOut(BaseModel):
    email_id: str
    to: str
    subject: str
    idempotency_key: str | None = None


class ResolveStepRequest(BaseModel):
    outcome: Literal["completed", "failed", "not_executed"] = Field(
        description="completed: the effect happened (give `result`); failed: it did not happen and "
        "must not be retried; not_executed: it did not happen, safe to run now"
    )
    result: Any = Field(default=None, description="Required when outcome is 'completed'")
    message: str | None = None
    resolved_by: str = "api"


class ExternalOperationOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    idempotency_key: str
    run_id: uuid.UUID
    step_name: str
    status: str
    attempts: int
    result_data: Any = None
    created_at: datetime
    updated_at: datetime


class SignalIn(BaseModel):
    payload: Any = None
    idempotency_key: str | None = Field(default=None, max_length=200)


class SignalReceiptOut(BaseModel):
    signal_id: int
    duplicate: bool
    woke_run: bool


class SignalOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    signal_id: int
    name: str
    payload: Any = None
    idempotency_key: str | None = None
    received_at: datetime
    consumed_by_step: str | None = None
    consumed_at: datetime | None = None


class TimerOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    timer_id: int
    step_name: str
    kind: str
    fire_at: datetime
    status: str
    created_at: datetime
    fired_at: datetime | None = None
