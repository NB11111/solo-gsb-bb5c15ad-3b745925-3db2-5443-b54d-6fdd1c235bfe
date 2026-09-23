"""Pydantic request/response models."""
from __future__ import annotations

import math
from datetime import datetime, timezone
from typing import Literal, Optional

from pydantic import BaseModel, Field, field_validator


def _finite(name: str):
    @field_validator(name)
    @classmethod
    def _check(cls, v: float) -> float:
        if not math.isfinite(v):
            raise ValueError(f"{name} must be a finite number")
        return v

    return _check


class DatasetRegister(BaseModel):
    epsilon_cap: float = Field(
        ...,
        gt=0.0,
        le=1_000.0,
        examples=[1.0],
        description="Total epsilon budget allowed for this dataset.",
    )
    delta_cap: float = Field(
        ...,
        ge=1e-20,
        lt=1.0,
        examples=[1e-6],
        description="Total delta budget; also the target delta used when the "
        "composed RDP curve is converted back to (epsilon, delta)-DP.",
    )
    description: Optional[str] = Field(default=None, max_length=2000)

    _eps_finite = _finite("epsilon_cap")
    _delta_finite = _finite("delta_cap")


class AnalysisRequest(BaseModel):
    request_id: str = Field(
        ...,
        min_length=1,
        max_length=200,
        examples=["req-20260923-0001"],
        description="Client-generated idempotency key; unique per dataset.",
    )
    query_type: str = Field(
        ...,
        min_length=1,
        max_length=100,
        examples=["gaussian_mean"],
        description="Free-form label of the analysis, e.g. "
        "'gaussian_mean', 'histogram', 'sql_count'.",
    )
    epsilon: float = Field(..., gt=0.0, le=1_000.0, examples=[0.1])
    delta: float = Field(
        ...,
        ge=0.0,
        lt=1.0,
        examples=[1e-7],
        description="Per-request delta; use 0 for pure epsilon-DP.",
    )

    _eps_finite = _finite("epsilon")
    _delta_finite = _finite("delta")


class BudgetView(BaseModel):
    epsilon_cap: float
    delta_cap: float
    epsilon_consumed: float
    epsilon_remaining: float
    rdp_best_order: float
    granted_requests: int
    rdp_curve: dict[str, float]


class DatasetInfo(BaseModel):
    dataset_id: str
    description: Optional[str]
    created_at: datetime
    budget: BudgetView


class Verdict(BaseModel):
    """Result of an analysis submission.

    A refusal is returned with HTTP 200 and ``allowed = false``: the request
    was processed correctly, it simply does not fit in the remaining budget.
    """

    dataset_id: str
    request_id: str
    query_type: str
    epsilon: float
    delta: float
    allowed: bool
    status: Literal["granted", "rejected"]
    reason: Optional[str] = None
    epsilon_before: float
    epsilon_after: float
    epsilon_remaining: float
    rdp_best_order: float
    idempotent_replay: bool = False
    created_at: datetime


class RequestRecord(BaseModel):
    dataset_id: str
    request_id: str
    query_type: str
    epsilon: float
    delta: float
    status: Literal["granted", "rejected"]
    reason: Optional[str]
    epsilon_before: float
    epsilon_after: float
    created_at: datetime


class RequestList(BaseModel):
    dataset_id: str
    count: int
    requests: list[RequestRecord]


def utcnow() -> datetime:
    return datetime.now(timezone.utc)
