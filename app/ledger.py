"""Ledger business logic: atomic admission control and idempotency."""
from __future__ import annotations

import asyncio
from datetime import timedelta
from typing import Any, Optional

import aiosqlite

from . import db, rdp
from .config import settings
from .schemas import (
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


async def _committed_entries(
    conn: aiosqlite.Connection, dataset_id: str, now_iso: str
) -> list[tuple[float, float]]:
    """Granted analysis requests plus non-expired granted reservations.

    Calling this also lazily sweeps reservations whose TTL has elapsed, so
    expired rows never occupy budget in admission checks or queries.
    """
    granted = await db.list_granted_entries(conn, dataset_id)
    reserved = await db.list_active_reservation_entries(
        conn, dataset_id, now_iso=now_iso
    )
    return granted + reserved


async def _budget(db_conn: aiosqlite.Connection, dataset: aiosqlite.Row) -> dict[str, Any]:
    now_iso = utcnow().isoformat()
    dataset_id = dataset["dataset_id"]

    granted = await db.list_granted_entries(db_conn, dataset_id)
    reserved_entries = await db.list_active_reservation_entries(
        db_conn, dataset_id, now_iso=now_iso
    )
    # Read-only endpoints still run the lazy-expiry UPDATE above; persist it.
    if db_conn.in_transaction:
        await db_conn.commit()

    _, consumed, _ = rdp.evaluate_entries(granted, dataset["delta_cap"])
    committed_curve, committed, best_order = rdp.evaluate_entries(
        granted + reserved_entries, dataset["delta_cap"]
    )
    active = len(reserved_entries)
    cap = dataset["epsilon_cap"]
    return {
        "epsilon_cap": cap,
        "delta_cap": dataset["delta_cap"],
        "epsilon_consumed": max(0.0, consumed),
        # Extra projected epsilon locked by active reservations alone.
        "epsilon_reserved": max(0.0, committed - max(0.0, consumed)),
        "epsilon_remaining": max(0.0, cap - committed),
        "rdp_best_order": best_order,
        "granted_requests": len(granted),
        "active_reservations": active,
        "rdp_curve": rdp.sample_curve(committed_curve),
    }


def _dataset_view(dataset: aiosqlite.Row, budget: dict[str, Any]) -> dict[str, Any]:
    return {
        "dataset_id": dataset["dataset_id"],
        "description": dataset["description"],
        "created_at": dataset["created_at"],
        "budget": budget,
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
    return _dataset_view(dataset, budget)


async def get_dataset_info(dataset_id: str) -> dict[str, Any]:
    async with db.connect() as conn:
        dataset = await db.require_dataset(conn, dataset_id)
        budget = await _budget(conn, dataset)
    return _dataset_view(dataset, budget)


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
                # Outstanding reservations occupy budget too: only what is
                # left after granted requests *and* active reservations may
                # be admitted.
                now_iso = utcnow().isoformat()
                entries = await _committed_entries(conn, dataset_id, now_iso)
                _, committed, best_order = rdp.evaluate_entries(
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
                        f"(committed {max(0.0, committed):.6g})"
                    )
                    # A rejection consumes nothing.
                    after = max(0.0, committed)
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
                    epsilon_before=max(0.0, committed),
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


# ------------------------------------------------------------ reservations --


def _reservation_view(
    row: aiosqlite.Row, dataset: aiosqlite.Row, *, replay: bool
) -> dict[str, Any]:
    remaining = max(0.0, dataset["epsilon_cap"] - row["epsilon_after"])
    return {
        "dataset_id": row["dataset_id"],
        "reservation_id": row["reservation_id"],
        "epsilon": row["epsilon"],
        "delta": row["delta"],
        "ttl_seconds": row["ttl_seconds"],
        "allowed": row["status"] in ("granted", "confirmed"),
        "status": row["status"],
        "reason": row["reason"],
        "epsilon_before": row["epsilon_before"],
        "epsilon_after": row["epsilon_after"],
        "epsilon_remaining": remaining,
        "rdp_best_order": row["rdp_best_order"],
        "idempotent_replay": replay,
        "created_at": row["created_at"],
        "expires_at": row["expires_at"],
        "expired_at": row["expired_at"],
        "confirmed_at": row["confirmed_at"],
        "confirmed_request_id": row["confirmed_request_id"],
        "released_at": row["released_at"],
    }


async def _refresh_reservation_row(
    conn: aiosqlite.Connection,
    row: aiosqlite.Row,
    now_iso: str,
) -> aiosqlite.Row:
    """If row is a granted reservation past its TTL, mark it expired and refetch."""
    if (
        row["status"] == "granted"
        and row["expires_at"] is not None
        and row["expires_at"] <= now_iso
    ):
        await db.expire_reservation(
            conn,
            dataset_id=row["dataset_id"],
            reservation_id=row["reservation_id"],
            expired_at=now_iso,
        )
        refreshed = await db.get_reservation(
            conn, row["dataset_id"], row["reservation_id"]
        )
        assert refreshed is not None
        return refreshed
    return row


async def create_reservation(
    dataset_id: str, payload: ReservationCreate
) -> tuple[dict[str, Any], int]:
    """Atomically reserve budget; idempotent on ``reservation_id``."""
    now = utcnow()
    now_iso = now.isoformat()

    async with _write_lock():
        async with db.connect() as conn:
            await db.begin_immediate(conn)
            try:
                dataset = await db.require_dataset(conn, dataset_id)

                existing = await db.get_reservation(
                    conn, dataset_id, payload.reservation_id
                )
                if existing is not None:
                    # Idempotent replay: same reservation_id, same payload.
                    same = (
                        existing["epsilon"] == payload.epsilon
                        and existing["delta"] == payload.delta
                        and existing["ttl_seconds"] == payload.ttl_seconds
                    )
                    if not same:
                        raise db.ReservationConflict(payload.reservation_id)
                    existing = await _refresh_reservation_row(
                        conn, existing, now_iso
                    )
                    view = _reservation_view(existing, dataset, replay=True)
                    await conn.commit()
                    return view, 200

                entries = await _committed_entries(conn, dataset_id, now_iso)
                _, committed, best_order = rdp.evaluate_entries(
                    entries, dataset["delta_cap"]
                )
                projected, proj_order = rdp.project_entries(
                    entries,
                    payload.epsilon,
                    payload.delta,
                    dataset["delta_cap"],
                )

                cap = dataset["epsilon_cap"]
                expires_at = (now + timedelta(seconds=payload.ttl_seconds)).isoformat()
                if projected <= cap + settings.epsilon_tolerance:
                    status = "granted"
                    reason: Optional[str] = None
                    after = projected
                    order_used = proj_order
                else:
                    status = "rejected"
                    reason = (
                        f"budget exceeded: projected epsilon {projected:.6g} "
                        f"would surpass cap {cap:.6g} "
                        f"(committed {max(0.0, committed):.6g})"
                    )
                    # A rejected reservation locks nothing and never expires.
                    after = max(0.0, committed)
                    order_used = best_order
                    expires_at = None  # type: ignore[assignment]

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
                    created_at=now_iso,
                    expires_at=expires_at,
                )
                await conn.commit()
            except Exception:
                await conn.rollback()
                raise

            row = await db.get_reservation(conn, dataset_id, payload.reservation_id)
            return _reservation_view(row, dataset, replay=False), 201


async def confirm_reservation(
    dataset_id: str, reservation_id: str, payload: ReservationConfirm
) -> tuple[dict[str, Any], int]:
    """Turn an active reservation into an analysis request under RDP accounting.

    The reserved (epsilon, delta) becomes a granted request with the client
    supplied ``request_id`` / ``query_type``; the locked budget is not charged
    twice (the reservation leaves the active set as soon as it is confirmed).
    """
    now_iso = utcnow().isoformat()

    async with _write_lock():
        async with db.connect() as conn:
            await db.begin_immediate(conn)
            try:
                dataset = await db.require_dataset(conn, dataset_id)
                row = await db.get_reservation(conn, dataset_id, reservation_id)
                if row is None:
                    raise db.ReservationNotFound(reservation_id)
                row = await _refresh_reservation_row(conn, row, now_iso)

                # Idempotent re-confirmation: the reservation has already been
                # turned into a request.  Replay that verdict when the same
                # request_id is supplied; a different request_id is a 409.
                if row["status"] == "confirmed":
                    if payload.request_id != row["confirmed_request_id"]:
                        raise db.ReservationStateConflict(
                            "reservation_confirmed",
                            "reservation was already confirmed as request "
                            f"{row['confirmed_request_id']!r}",
                        )
                    stored = await db.get_request(
                        conn, dataset_id, payload.request_id
                    )
                    if (
                        stored is not None
                        and stored["query_type"] == payload.query_type
                    ):
                        verdict = _verdict_from_row(stored, dataset, replay=True)
                        await conn.commit()
                        return verdict, 200
                    raise db.RequestConflict(payload.request_id)

                if row["status"] != "granted":
                    raise db.ReservationStateConflict(
                        f"reservation_{row['status']}",
                        f"reservation is {row['status']} and cannot be confirmed",
                    )

                existing = await db.get_request(conn, dataset_id, payload.request_id)
                if existing is not None:
                    # While the reservation is still merely granted, no request
                    # may own this request_id yet; re-confirms are handled in
                    # the 'confirmed' branch above.
                    raise db.RequestConflict(payload.request_id)

                # Project committed-with-self: other live reservations and all
                # granted requests, plus the (epsilon, delta) this reservation
                # locked at creation.
                active = await _committed_entries(conn, dataset_id, now_iso)
                # The reservation being confirmed is itself in the active set.
                # Remove exactly its entry (by identity position) to obtain the
                # "before" composition; after confirmation the same composition
                # is represented by the new granted request, so total
                # committed epsilon is unchanged and the budget is never
                # charged twice.
                target = (row["epsilon"], row["delta"])
                others = list(active)
                try:
                    others.remove(target)
                except ValueError:
                    # Defensive: active set and row state diverged.
                    others = active
                _, epsilon_before, _ = rdp.evaluate_entries(
                    others, dataset["delta_cap"]
                )
                _, epsilon_after, best_order = rdp.evaluate_entries(
                    active, dataset["delta_cap"]
                )
                epsilon_before = max(0.0, epsilon_before)
                epsilon_after = max(0.0, epsilon_after)

                await db.insert_request(
                    conn,
                    dataset_id=dataset_id,
                    request_id=payload.request_id,
                    query_type=payload.query_type,
                    epsilon=row["epsilon"],
                    delta=row["delta"],
                    status="granted",
                    reason=None,
                    epsilon_before=epsilon_before,
                    epsilon_after=epsilon_after,
                    rdp_best_order=best_order,
                    created_at=now_iso,
                )
                await db.confirm_reservation(
                    conn,
                    dataset_id=dataset_id,
                    reservation_id=reservation_id,
                    confirmed_request_id=payload.request_id,
                    confirmed_at=now_iso,
                    epsilon_before=epsilon_before,
                    epsilon_after=epsilon_after,
                    rdp_best_order=best_order,
                )
                await conn.commit()
            except Exception:
                await conn.rollback()
                raise

            request_row = await db.get_request(conn, dataset_id, payload.request_id)
            return _verdict_from_row(request_row, dataset, replay=False), 201


async def release_reservation(
    dataset_id: str, reservation_id: str
) -> tuple[dict[str, Any], int]:
    """Idempotently cancel a reservation; its budget is freed immediately.

    Releasing an already released / expired reservation replays the final
    state.  Confirmed reservations are consumed and cannot be released;
    rejected reservations never held any budget.
    """
    now_iso = utcnow().isoformat()

    async with _write_lock():
        async with db.connect() as conn:
            await db.begin_immediate(conn)
            try:
                dataset = await db.require_dataset(conn, dataset_id)
                row = await db.get_reservation(conn, dataset_id, reservation_id)
                if row is None:
                    raise db.ReservationNotFound(reservation_id)
                row = await _refresh_reservation_row(conn, row, now_iso)

                if row["status"] in ("released", "expired"):
                    view = _reservation_view(row, dataset, replay=True)
                    await conn.commit()
                    return view, 200
                if row["status"] != "granted":
                    raise db.ReservationStateConflict(
                        f"reservation_{row['status']}",
                        f"reservation is {row['status']} and cannot be released",
                    )

                await db.release_reservation(
                    conn,
                    dataset_id=dataset_id,
                    reservation_id=reservation_id,
                    released_at=now_iso,
                )
                await conn.commit()
            except Exception:
                await conn.rollback()
                raise

            row = await db.get_reservation(conn, dataset_id, reservation_id)
            return _reservation_view(row, dataset, replay=False), 200


async def get_reservation(dataset_id: str, reservation_id: str) -> dict[str, Any]:
    now_iso = utcnow().isoformat()
    async with db.connect() as conn:
        dataset = await db.require_dataset(conn, dataset_id)
        row = await db.get_reservation(conn, dataset_id, reservation_id)
        if row is None:
            raise db.ReservationNotFound(reservation_id)
        row = await _refresh_reservation_row(conn, row, now_iso)
        if conn.in_transaction:
            await conn.commit()
        return _reservation_view(row, dataset, replay=False)


async def list_reservations(dataset_id: str) -> dict[str, Any]:
    now_iso = utcnow().isoformat()
    async with db.connect() as conn:
        await db.require_dataset(conn, dataset_id)
        # Sweep expired rows before listing.
        await db.list_active_reservation_entries(
            conn, dataset_id, now_iso=now_iso
        )
        if conn.in_transaction:
            await conn.commit()
        rows = await db.list_reservations(conn, dataset_id)
        dataset = await db.require_dataset(conn, dataset_id)
    return {
        "dataset_id": dataset_id,
        "count": len(rows),
        "reservations": [
            _reservation_view(r, dataset, replay=False) for r in rows
        ],
    }
