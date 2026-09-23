"""Ledger business logic: atomic admission control and idempotency.

Budget holds (reservations)
---------------------------
Besides granted analysis requests, *reservations* hold budget for a limited
time (TTL).  Admission control — for direct requests as well as for new
reservations — always composes **granted requests + unexpired reservations**
and rejects whatever would push the composed epsilon over the cap.

Confirming a reservation converts its hold into a granted request with the
same ``(epsilon, delta)``, which is budget-neutral, so a confirmation of a
live reservation never needs a fresh admission check.  Releasing a
reservation (or letting its TTL expire) frees the hold.  All state changes
happen inside the same serialised write section used for requests, so
concurrent clients can never overspend.
"""
from __future__ import annotations

import asyncio
from datetime import datetime, timedelta
from typing import Any, Optional

import aiosqlite

from . import db, rdp
from .config import settings
from .schemas import (
    DEFAULT_CONFIRM_QUERY_TYPE,
    AnalysisRequest,
    DatasetRegister,
    ReservationConfirm,
    ReservationCreate,
    utcnow,
)

# Serialises all writers inside this process.  Combined with
# BEGIN IMMEDIATE + busy_timeout, two concurrent submissions can never both
# observe the same pre-update balance, even across multiple processes.
# Locks are per-event-loop (asyncio.Lock binds to the loop of its first use),
# so they are created lazily and cached by loop.
_write_locks: dict[asyncio.AbstractEventLoop, asyncio.Lock] = {}


def _write_lock() -> asyncio.Lock:
    loop = asyncio.get_running_loop()
    lock = _write_locks.get(loop)
    if lock is None:
        lock = asyncio.Lock()
        _write_locks[loop] = lock
    return lock


# ----------------------------------------------------------------- helpers --


def _parse_ts(value: str) -> datetime:
    return datetime.fromisoformat(value)


def _effective_reservation_status(row: aiosqlite.Row, now: datetime) -> str:
    """Stored status with lazy TTL expiry applied (reads never write)."""
    if row["status"] == "active" and _parse_ts(row["expires_at"]) <= now:
        return "expired"
    return row["status"]


async def _unexpired_reservation_entries(
    conn: aiosqlite.Connection, dataset_id: str, now: datetime
) -> list[tuple[float, float]]:
    """``(epsilon, delta)`` pairs of reservations still holding budget."""
    rows = await db.list_active_reservations(conn, dataset_id)
    return [
        (row["epsilon"], row["delta"])
        for row in rows
        if _parse_ts(row["expires_at"]) > now
    ]


async def _sweep_expired_reservations(
    conn: aiosqlite.Connection, dataset_id: str, now: datetime
) -> None:
    """Persist the active -> expired transition for due reservations.

    Called inside write transactions so the recorded state matches what
    admission control already enforced; read paths compute expiry lazily.
    """
    for row in await db.list_active_reservations(conn, dataset_id):
        if _parse_ts(row["expires_at"]) <= now:
            await db.mark_reservation_expired(conn, row["id"])


async def _budget(db_conn: aiosqlite.Connection, dataset: aiosqlite.Row) -> dict[str, Any]:
    dataset_id = dataset["dataset_id"]
    now = utcnow()
    entries = await db.list_granted_entries(db_conn, dataset_id)
    holds = await _unexpired_reservation_entries(db_conn, dataset_id, now)
    consumed_curve, consumed, best_order = rdp.evaluate_entries(
        entries, dataset["delta_cap"]
    )
    _, reserved, _ = rdp.evaluate_entries(holds, dataset["delta_cap"])
    _, committed, _ = rdp.evaluate_entries(entries + holds, dataset["delta_cap"])
    cap = dataset["epsilon_cap"]
    return {
        "epsilon_cap": cap,
        "delta_cap": dataset["delta_cap"],
        "epsilon_consumed": consumed,
        "epsilon_reserved": reserved,
        "epsilon_committed": committed,
        "epsilon_remaining": max(0.0, cap - consumed),
        "epsilon_available": max(0.0, cap - committed),
        "rdp_best_order": best_order,
        "granted_requests": len(entries),
        "active_reservations": len(holds),
        "rdp_curve": rdp.sample_curve(consumed_curve),
    }


async def register_dataset(dataset_id: str, payload: DatasetRegister) -> dict[str, Any]:
    created_at = utcnow().isoformat()
    async with _write_lock():
        await db.create_dataset(
            dataset_id,
            payload.epsilon_cap,
            payload.delta_cap,
            payload.description,
            created_at,
        )
    async with db.connect() as conn:
        dataset = await db.require_dataset(conn, dataset_id)
        budget = await _budget(conn, dataset)
    return {
        "dataset_id": dataset["dataset_id"],
        "description": dataset["description"],
        "created_at": dataset["created_at"],
        "budget": budget,
    }


async def get_dataset_info(dataset_id: str) -> dict[str, Any]:
    async with db.connect() as conn:
        dataset = await db.require_dataset(conn, dataset_id)
        budget = await _budget(conn, dataset)
    return {
        "dataset_id": dataset["dataset_id"],
        "description": dataset["description"],
        "created_at": dataset["created_at"],
        "budget": budget,
    }


# --------------------------------------------------------------- submission --


async def submit_analysis(
    dataset_id: str, payload: AnalysisRequest
) -> tuple[dict[str, Any], int]:
    """Idempotently submit an analysis request.

    Returns ``(verdict_dict, http_status)``.  http_status is 201 for a newly
    stored verdict and 200 for an idempotent replay / conflict-free lookup.
    """
    now = utcnow()
    created_at = now.isoformat()

    async with _write_lock():
        async with db.connect() as conn:
            await db.begin_immediate(conn)
            try:
                dataset = await db.require_dataset(conn, dataset_id)

                existing = await db.get_request(conn, dataset_id, payload.request_id)
                if existing is not None:
                    # Idempotent replay: same request_id must be identical.
                    same = (
                        existing["query_type"] == payload.query_type
                        and existing["epsilon"] == payload.epsilon
                        and existing["delta"] == payload.delta
                    )
                    if not same:
                        raise db.RequestConflict(payload.request_id)
                    verdict = _verdict_from_row(existing, dataset, replay=True)
                    await conn.commit()
                    return verdict, 200

                # --- admission check inside the same atomic transaction ---
                # Active reservations hold budget too: the projection runs
                # over granted requests + unexpired reservations.
                await _sweep_expired_reservations(conn, dataset_id, now)
                entries = await db.list_granted_entries(conn, dataset_id)
                holds = await _unexpired_reservation_entries(conn, dataset_id, now)

                _, committed, _ = rdp.evaluate_entries(
                    entries + holds, dataset["delta_cap"]
                )
                projected, _ = rdp.project_entries(
                    entries + holds,
                    payload.epsilon,
                    payload.delta,
                    dataset["delta_cap"],
                )

                # The stored before/after track granted consumption only.
                _, consumed, best_order = rdp.evaluate_entries(
                    entries, dataset["delta_cap"]
                )
                granted_projected, granted_order = rdp.project_entries(
                    entries,
                    payload.epsilon,
                    payload.delta,
                    dataset["delta_cap"],
                )

                cap = dataset["epsilon_cap"]
                if projected <= cap + settings.epsilon_tolerance:
                    status = "granted"
                    reason = None
                    after = granted_projected
                    order_used = granted_order
                else:
                    status = "rejected"
                    reason = (
                        f"budget exceeded: projected epsilon {projected:.6g} "
                        f"would surpass cap {cap:.6g} "
                        f"(committed {max(0.0, committed):.6g} across "
                        f"{len(entries)} granted requests and "
                        f"{len(holds)} active reservations)"
                    )
                    # A rejection consumes nothing.
                    after = max(0.0, consumed)
                    order_used = best_order

                await db.insert_request(
                    conn,
                    dataset_id=dataset_id,
                    request_id=payload.request_id,
                    query_type=payload.query_type,
                    epsilon=payload.epsilon,
                    delta=payload.delta,
                    status=status,
                    reason=reason,
                    epsilon_before=max(0.0, consumed),
                    epsilon_after=after,
                    rdp_best_order=order_used,
                    created_at=created_at,
                )
                await conn.commit()
            except Exception:
                await conn.rollback()
                raise

            row = await db.get_request(conn, dataset_id, payload.request_id)
            verdict = _verdict_from_row(row, dataset, replay=False)
            return verdict, 201


def _verdict_from_row(row: aiosqlite.Row, dataset: aiosqlite.Row, *, replay: bool) -> dict[str, Any]:
    remaining = max(0.0, dataset["epsilon_cap"] - row["epsilon_after"])
    return {
        "dataset_id": row["dataset_id"],
        "request_id": row["request_id"],
        "query_type": row["query_type"],
        "epsilon": row["epsilon"],
        "delta": row["delta"],
        "allowed": row["status"] == "granted",
        "status": row["status"],
        "reason": row["reason"],
        "epsilon_before": row["epsilon_before"],
        "epsilon_after": row["epsilon_after"],
        "epsilon_remaining": remaining,
        "rdp_best_order": row["rdp_best_order"],
        "idempotent_replay": replay,
        "created_at": row["created_at"],
    }


async def list_requests(dataset_id: str) -> dict[str, Any]:
    async with db.connect() as conn:
        await db.require_dataset(conn, dataset_id)
        rows = await db.list_requests(conn, dataset_id)
    return {
        "dataset_id": dataset_id,
        "count": len(rows),
        "requests": [
            {
                "dataset_id": r["dataset_id"],
                "request_id": r["request_id"],
                "query_type": r["query_type"],
                "epsilon": r["epsilon"],
                "delta": r["delta"],
                "status": r["status"],
                "reason": r["reason"],
                "epsilon_before": r["epsilon_before"],
                "epsilon_after": r["epsilon_after"],
                "created_at": r["created_at"],
            }
            for r in rows
        ],
    }


# -------------------------------------------------------------- reservations --


async def create_reservation(
    dataset_id: str, payload: ReservationCreate
) -> tuple[dict[str, Any], int]:
    """Idempotently create a budget reservation.

    Returns ``(verdict_dict, http_status)``: 201 for a newly stored
    reservation, 200 for an idempotent replay.
    """
    now = utcnow()
    created_at = now.isoformat()
    expires_at = (now + timedelta(seconds=payload.ttl_seconds)).isoformat()

    async with _write_lock():
        async with db.connect() as conn:
            await db.begin_immediate(conn)
            try:
                dataset = await db.require_dataset(conn, dataset_id)
                await _sweep_expired_reservations(conn, dataset_id, now)

                existing = await db.get_reservation(
                    conn, dataset_id, payload.reservation_id
                )
                if existing is not None:
                    # Idempotent replay: same reservation_id must be identical.
                    same = (
                        existing["epsilon"] == payload.epsilon
                        and existing["delta"] == payload.delta
                        and existing["ttl_seconds"] == payload.ttl_seconds
                    )
                    if not same:
                        raise db.ReservationConflict(payload.reservation_id)
                    verdict = _reservation_verdict(existing, dataset, replay=True)
                    await conn.commit()
                    return verdict, 200

                # --- admission check inside the same atomic transaction ---
                entries = await db.list_granted_entries(conn, dataset_id)
                holds = await _unexpired_reservation_entries(conn, dataset_id, now)
                base = entries + holds
                _, committed, best_order = rdp.evaluate_entries(
                    base, dataset["delta_cap"]
                )
                projected, proj_order = rdp.project_entries(
                    base,
                    payload.epsilon,
                    payload.delta,
                    dataset["delta_cap"],
                )

                cap = dataset["epsilon_cap"]
                if projected <= cap + settings.epsilon_tolerance:
                    status = "active"
                    reason = None
                    after = projected
                    order_used = proj_order
                else:
                    status = "rejected"
                    reason = (
                        f"budget exceeded: projected epsilon {projected:.6g} "
                        f"would surpass cap {cap:.6g} "
                        f"(committed {max(0.0, committed):.6g} across "
                        f"{len(entries)} granted requests and "
                        f"{len(holds)} active reservations)"
                    )
                    # A rejected reservation holds nothing.
                    after = max(0.0, committed)
                    order_used = best_order

                await db.insert_reservation(
                    conn,
                    dataset_id=dataset_id,
                    reservation_id=payload.reservation_id,
                    epsilon=payload.epsilon,
                    delta=payload.delta,
                    ttl_seconds=payload.ttl_seconds,
                    status=status,
                    reason=reason,
                    epsilon_before=max(0.0, committed),
                    epsilon_after=after,
                    rdp_best_order=order_used,
                    created_at=created_at,
                    expires_at=expires_at,
                )
                await conn.commit()
            except Exception:
                await conn.rollback()
                raise

            row = await db.get_reservation(conn, dataset_id, payload.reservation_id)
            return _reservation_verdict(row, dataset, replay=False), 201


def _reservation_verdict(
    row: aiosqlite.Row, dataset: aiosqlite.Row, *, replay: bool
) -> dict[str, Any]:
    status = _effective_reservation_status(row, utcnow())
    remaining = max(0.0, dataset["epsilon_cap"] - row["epsilon_after"])
    return {
        "dataset_id": row["dataset_id"],
        "reservation_id": row["reservation_id"],
        "epsilon": row["epsilon"],
        "delta": row["delta"],
        "ttl_seconds": row["ttl_seconds"],
        # Anything not rejected was granted budget at creation time; the
        # current lifecycle state is reported separately in ``status``.
        "allowed": row["status"] != "rejected",
        "status": status,
        "reason": row["reason"],
        "epsilon_before": row["epsilon_before"],
        "epsilon_after": row["epsilon_after"],
        "epsilon_remaining": remaining,
        "rdp_best_order": row["rdp_best_order"],
        "idempotent_replay": replay,
        "created_at": row["created_at"],
        "expires_at": row["expires_at"],
    }


async def confirm_reservation(
    dataset_id: str, reservation_id: str, payload: Optional[ReservationConfirm]
) -> tuple[dict[str, Any], int]:
    """Convert an active reservation into a granted analysis request.

    The reservation already holds its ``(epsilon, delta)`` share, so the
    conversion is budget-neutral and cannot overspend.  Returns
    ``(verdict_dict, http_status)``: 201 for a fresh confirmation, 200 for
    an idempotent replay.
    """
    now = utcnow()
    confirmed_at = now.isoformat()
    want_request_id = (payload.request_id if payload else None) or reservation_id
    want_query_type = (
        payload.query_type if payload else None
    ) or DEFAULT_CONFIRM_QUERY_TYPE

    async with _write_lock():
        async with db.connect() as conn:
            await db.begin_immediate(conn)
            try:
                dataset = await db.require_dataset(conn, dataset_id)
                await _sweep_expired_reservations(conn, dataset_id, now)

                rsv = await db.get_reservation(conn, dataset_id, reservation_id)
                if rsv is None:
                    raise db.ReservationNotFound(reservation_id)

                if rsv["status"] == "confirmed":
                    # Idempotent replay: must reference the same request.
                    req = await db.get_request(conn, dataset_id, rsv["request_id"])
                    if (
                        req is None
                        or req["request_id"] != want_request_id
                        or req["query_type"] != want_query_type
                    ):
                        raise db.ReservationConflict(reservation_id)
                    verdict = _confirm_verdict(rsv, req, dataset, replay=True)
                    await conn.commit()
                    return verdict, 200
                if rsv["status"] == "released":
                    raise db.ReservationStateError(
                        reservation_id,
                        "reservation_already_released",
                        "a released reservation cannot be confirmed",
                    )
                if rsv["status"] == "expired":
                    raise db.ReservationStateError(
                        reservation_id,
                        "reservation_expired",
                        "the reservation TTL elapsed; create a new reservation",
                    )
                if rsv["status"] == "rejected":
                    raise db.ReservationStateError(
                        reservation_id,
                        "reservation_not_active",
                        "a rejected reservation never held budget and cannot "
                        "be confirmed",
                    )

                # Active and unexpired.  The derived request_id must not
                # collide with an unrelated analysis request.
                clash = await db.get_request(conn, dataset_id, want_request_id)
                if clash is not None:
                    raise db.RequestConflict(want_request_id)

                entries = await db.list_granted_entries(conn, dataset_id)
                _, consumed, _ = rdp.evaluate_entries(entries, dataset["delta_cap"])
                projected, proj_order = rdp.project_entries(
                    entries,
                    rsv["epsilon"],
                    rsv["delta"],
                    dataset["delta_cap"],
                )

                await db.insert_request(
                    conn,
                    dataset_id=dataset_id,
                    request_id=want_request_id,
                    query_type=want_query_type,
                    epsilon=rsv["epsilon"],
                    delta=rsv["delta"],
                    status="granted",
                    reason=None,
                    epsilon_before=max(0.0, consumed),
                    epsilon_after=projected,
                    rdp_best_order=proj_order,
                    created_at=confirmed_at,
                )
                await db.mark_reservation_confirmed(
                    conn,
                    dataset_id,
                    reservation_id,
                    request_id=want_request_id,
                    confirmed_at=confirmed_at,
                )
                await conn.commit()
            except Exception:
                await conn.rollback()
                raise

            rsv = await db.get_reservation(conn, dataset_id, reservation_id)
            req = await db.get_request(conn, dataset_id, want_request_id)
            return _confirm_verdict(rsv, req, dataset, replay=False), 201


def _confirm_verdict(
    rsv: aiosqlite.Row, req: aiosqlite.Row, dataset: aiosqlite.Row, *, replay: bool
) -> dict[str, Any]:
    remaining = max(0.0, dataset["epsilon_cap"] - req["epsilon_after"])
    return {
        "dataset_id": rsv["dataset_id"],
        "reservation_id": rsv["reservation_id"],
        "request_id": req["request_id"],
        "query_type": req["query_type"],
        "epsilon": req["epsilon"],
        "delta": req["delta"],
        "status": "granted",
        "epsilon_before": req["epsilon_before"],
        "epsilon_after": req["epsilon_after"],
        "epsilon_remaining": remaining,
        "rdp_best_order": req["rdp_best_order"],
        "idempotent_replay": replay,
        "created_at": req["created_at"],
        "confirmed_at": rsv["confirmed_at"],
    }


async def release_reservation(
    dataset_id: str, reservation_id: str
) -> tuple[dict[str, Any], int]:
    """Idempotently cancel an active reservation, freeing its hold."""
    now = utcnow()
    released_at = now.isoformat()

    async with _write_lock():
        async with db.connect() as conn:
            await db.begin_immediate(conn)
            try:
                dataset = await db.require_dataset(conn, dataset_id)
                await _sweep_expired_reservations(conn, dataset_id, now)

                rsv = await db.get_reservation(conn, dataset_id, reservation_id)
                if rsv is None:
                    raise db.ReservationNotFound(reservation_id)

                if rsv["status"] == "released":
                    # Idempotent replay of an earlier release.
                    result = _release_result(rsv, replay=True)
                    await conn.commit()
                    return result, 200
                if rsv["status"] == "confirmed":
                    raise db.ReservationStateError(
                        reservation_id,
                        "reservation_already_confirmed",
                        "a confirmed reservation became an analysis request "
                        "and can no longer be released",
                    )
                if rsv["status"] == "expired":
                    raise db.ReservationStateError(
                        reservation_id,
                        "reservation_expired",
                        "the reservation TTL elapsed; it no longer holds "
                        "budget",
                    )
                if rsv["status"] == "rejected":
                    raise db.ReservationStateError(
                        reservation_id,
                        "reservation_not_active",
                        "a rejected reservation never held budget",
                    )

                await db.mark_reservation_released(
                    conn, dataset_id, reservation_id, released_at=released_at
                )
                await conn.commit()
            except Exception:
                await conn.rollback()
                raise

            rsv = await db.get_reservation(conn, dataset_id, reservation_id)
            return _release_result(rsv, replay=False), 200


def _release_result(row: aiosqlite.Row, *, replay: bool) -> dict[str, Any]:
    return {
        "dataset_id": row["dataset_id"],
        "reservation_id": row["reservation_id"],
        "status": row["status"],
        "epsilon": row["epsilon"],
        "delta": row["delta"],
        "idempotent_replay": replay,
        "created_at": row["created_at"],
        "expires_at": row["expires_at"],
        "released_at": row["released_at"],
    }


def _reservation_record(row: aiosqlite.Row, now: datetime) -> dict[str, Any]:
    return {
        "dataset_id": row["dataset_id"],
        "reservation_id": row["reservation_id"],
        "epsilon": row["epsilon"],
        "delta": row["delta"],
        "ttl_seconds": row["ttl_seconds"],
        "status": _effective_reservation_status(row, now),
        "reason": row["reason"],
        "request_id": row["request_id"],
        "created_at": row["created_at"],
        "expires_at": row["expires_at"],
        "confirmed_at": row["confirmed_at"],
        "released_at": row["released_at"],
    }


async def get_reservation_info(dataset_id: str, reservation_id: str) -> dict[str, Any]:
    async with db.connect() as conn:
        await db.require_dataset(conn, dataset_id)
        row = await db.get_reservation(conn, dataset_id, reservation_id)
        if row is None:
            raise db.ReservationNotFound(reservation_id)
        return _reservation_record(row, utcnow())


async def list_reservations(dataset_id: str) -> dict[str, Any]:
    async with db.connect() as conn:
        await db.require_dataset(conn, dataset_id)
        rows = await db.list_reservations(conn, dataset_id)
    now = utcnow()
    return {
        "dataset_id": dataset_id,
        "count": len(rows),
        "reservations": [_reservation_record(r, now) for r in rows],
    }
