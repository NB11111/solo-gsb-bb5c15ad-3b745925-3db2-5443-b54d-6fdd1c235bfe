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
    epsilon_consumed: float = Field(
        description="Composed epsilon of granted analysis requests only."
    )
    epsilon_reserved: float = Field(
        description="Composed epsilon of active (unexpired) reservations only."
    )
    epsilon_committed: float = Field(
        description="Composed epsilon of granted requests + active "
        "reservations; this is the value admission control checks against "
        "epsilon_cap."
    )
    epsilon_remaining: float = Field(
        description="epsilon_cap - epsilon_consumed."
    )
    epsilon_available: float = Field(
        description="epsilon_cap - epsilon_committed; what new requests or "
        "reservations can still draw from."
    )
    rdp_best_order: float
    granted_requests: int
    active_reservations: int
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


# ------------------------------------------------------------- reservations --

ReservationStatus = Literal["active", "confirmed", "released", "expired", "rejected"]

#: query_type used when a confirmation does not name one.
DEFAULT_CONFIRM_QUERY_TYPE = "reserved_analysis"


class ReservationCreate(BaseModel):
    reservation_id: str = Field(
        ...,
        min_length=1,
        max_length=200,
        pattern=r"^[A-Za-z0-9._\-]+$",
        examples=["rsv-20260923-0001"],
        description="Client-generated idempotency key; unique per dataset. "
        "Restricted to URL-safe characters so it can address sub-resources.",
    )
    epsilon: float = Field(..., gt=0.0, le=1_000.0, examples=[0.2])
    delta: float = Field(
        ...,
        ge=0.0,
        lt=1.0,
        examples=[1e-8],
        description="Per-reservation delta; use 0 for pure epsilon-DP.",
    )
    ttl_seconds: float = Field(
        ...,
        gt=0.0,
        le=31_536_000.0,
        examples=[600.0],
        description="Time-to-live of the hold in seconds (max 1 year). "
        "After expiry the reservation no longer occupies budget.",
    )

    _eps_finite = _finite("epsilon")
    _delta_finite = _finite("delta")
    _ttl_finite = _finite("ttl_seconds")


class ReservationConfirm(BaseModel):
    """Optional body for the confirm call.

    Replaying a confirmation must use the same values (explicit or
    defaulted) as the first call, otherwise it is rejected with 409.
    """

    request_id: Optional[str] = Field(
        default=None,
        min_length=1,
        max_length=200,
        description="request_id of the resulting analysis request; "
        "defaults to the reservation_id.",
    )
    query_type: Optional[str] = Field(
        default=None,
        min_length=1,
        max_length=100,
        description=f"Defaults to '{DEFAULT_CONFIRM_QUERY_TYPE}'.",
    )


class ReservationVerdict(BaseModel):
    """Result of a reservation submission.

    ``allowed`` tells whether the reservation was granted budget; ``status``
    is the reservation's *current* lifecycle state (a replayed reservation
    may since have been confirmed, released or expired).
    """

    dataset_id: str
    reservation_id: str
    epsilon: float
    delta: float
    ttl_seconds: float
    allowed: bool
    status: ReservationStatus
    reason: Optional[str] = None
    epsilon_before: float
    epsilon_after: float
    epsilon_remaining: float
    rdp_best_order: float
    idempotent_replay: bool = False
    created_at: datetime
    expires_at: datetime


class ReservationRecord(BaseModel):
    dataset_id: str
    reservation_id: str
    epsilon: float
    delta: float
    ttl_seconds: float
    status: ReservationStatus
    reason: Optional[str]
    request_id: Optional[str]
    created_at: datetime
    expires_at: datetime
    confirmed_at: Optional[datetime]
    released_at: Optional[datetime]


class ReservationList(BaseModel):
    dataset_id: str
    count: int
    reservations: list[ReservationRecord]


class ReservationMutation(BaseModel):
    """Result of releasing a reservation."""

    dataset_id: str
    reservation_id: str
    status: ReservationStatus
    epsilon: float
    delta: float
    idempotent_replay: bool = False
    created_at: datetime
    expires_at: datetime
    released_at: Optional[datetime]


class ConfirmVerdict(BaseModel):
    """Result of confirming a reservation into an analysis request."""

    dataset_id: str
    reservation_id: str
    request_id: str
    query_type: str
    epsilon: float
    delta: float
    status: Literal["granted"]
    epsilon_before: float
    epsilon_after: float
    epsilon_remaining: float
    rdp_best_order: float
    idempotent_replay: bool = False
    created_at: datetime
    confirmed_at: datetime


def utcnow() -> datetime:
    return datetime.now(timezone.utc)
