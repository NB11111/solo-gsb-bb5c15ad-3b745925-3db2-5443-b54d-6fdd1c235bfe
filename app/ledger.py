"""Ledger business logic: atomic admission control and idempotency."""
from __future__ import annotations

import asyncio
from typing import Any

import aiosqlite

from . import db, rdp
from .config import settings
from .schemas import AnalysisRequest, DatasetRegister, utcnow

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


async def _budget(db_conn: aiosqlite.Connection, dataset: aiosqlite.Row) -> dict[str, Any]:
    entries = await db.list_granted_entries(db_conn, dataset["dataset_id"])
    consumed_curve, consumed, best_order = rdp.evaluate_entries(
        entries, dataset["delta_cap"]
    )
    remaining = max(0.0, dataset["epsilon_cap"] - consumed)
    return {
        "epsilon_cap": dataset["epsilon_cap"],
        "delta_cap": dataset["delta_cap"],
        "epsilon_consumed": consumed,
        "epsilon_remaining": remaining,
        "rdp_best_order": best_order,
        "granted_requests": len(entries),
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
    created_at = utcnow().isoformat()

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
                entries = await db.list_granted_entries(conn, dataset_id)
                _, consumed, best_order = rdp.evaluate_entries(
                    entries, dataset["delta_cap"]
                )

                projected, proj_order = rdp.project_entries(
                    entries,
                    payload.epsilon,
                    payload.delta,
                    dataset["delta_cap"],
                )

                cap = dataset["epsilon_cap"]
                if projected <= cap + settings.epsilon_tolerance:
                    status = "granted"
                    reason = None
                    after = projected
                    order_used = proj_order
                else:
                    status = "rejected"
                    reason = (
                        f"budget exceeded: projected epsilon {projected:.6g} "
                        f"would surpass cap {cap:.6g} "
                        f"(consumed {max(0.0, consumed):.6g})"
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
            except db.RequestConflict:
                await conn.rollback()
                raise
            except db.DatasetNotFound:
                await conn.rollback()
                raise
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
