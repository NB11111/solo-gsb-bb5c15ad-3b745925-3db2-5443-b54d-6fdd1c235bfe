"""SQLite persistence layer.

Concurrency model
-----------------
* The database runs in WAL mode with a busy timeout so readers never block
  writers and concurrent writers queue instead of failing immediately.
* Every state-changing operation takes the process-wide ``write_lock``
  (an asyncio.Lock) and opens an explicit ``BEGIN IMMEDIATE`` transaction.
  The check (does the request fit / does request_id already exist?) and the
  insert therefore happen in one serialised, atomic critical section.
* Each event-loop task gets its own aiosqlite connection; nothing is shared
  across threads.
"""
from __future__ import annotations

import os
from contextlib import asynccontextmanager
from typing import AsyncIterator, Optional

import aiosqlite

from .config import settings

# v2 migration — budget reservations with TTL.
#
# A reservation holds (epsilon, delta) budget until it is confirmed into
# an analysis request, released by the client, or expires (expires_at).
# ``epsilon_before``/``epsilon_after`` record the committed composition
# (granted requests + active reservations) around the admission decision.
CREATE_RESERVATIONS_SQL = """
CREATE TABLE IF NOT EXISTS reservations (
    id             INTEGER PRIMARY KEY AUTOINCREMENT,
    dataset_id     TEXT NOT NULL REFERENCES datasets(dataset_id),
    reservation_id TEXT NOT NULL,
    epsilon        REAL NOT NULL,
    delta          REAL NOT NULL,
    ttl_seconds    REAL NOT NULL,
    status         TEXT NOT NULL CHECK (status IN
                   ('active', 'confirmed', 'released', 'expired', 'rejected')),
    reason         TEXT,
    request_id     TEXT,
    epsilon_before REAL NOT NULL,
    epsilon_after  REAL NOT NULL,
    rdp_best_order REAL NOT NULL,
    created_at     TEXT NOT NULL,
    expires_at     TEXT NOT NULL,
    confirmed_at   TEXT,
    released_at    TEXT,
    UNIQUE (dataset_id, reservation_id)
);

CREATE INDEX IF NOT EXISTS idx_reservations_dataset ON reservations (dataset_id, id);
"""


# Versioned schema migrations, applied in order inside ``init_db``.  The
# current version is tracked in SQLite's ``PRAGMA user_version``; databases
# created by older releases (user_version = 0) are upgraded step by step, and
# every statement is idempotent so re-running a migration is harmless.
MIGRATIONS: tuple[str, ...] = (
    # v1 — base ledger: datasets + analysis requests.
    """
    CREATE TABLE IF NOT EXISTS datasets (
        dataset_id    TEXT PRIMARY KEY,
        epsilon_cap   REAL NOT NULL,
        delta_cap     REAL NOT NULL,
        description   TEXT,
        created_at    TEXT NOT NULL
    );

    CREATE TABLE IF NOT EXISTS requests (
        id             INTEGER PRIMARY KEY AUTOINCREMENT,
        dataset_id     TEXT NOT NULL REFERENCES datasets(dataset_id),
        request_id     TEXT NOT NULL,
        query_type     TEXT NOT NULL,
        epsilon        REAL NOT NULL,
        delta          REAL NOT NULL,
        status         TEXT NOT NULL CHECK (status IN ('granted', 'rejected')),
        reason         TEXT,
        epsilon_before REAL NOT NULL,
        epsilon_after  REAL NOT NULL,
        rdp_best_order REAL NOT NULL,
        created_at     TEXT NOT NULL,
        UNIQUE (dataset_id, request_id)
    );

    CREATE INDEX IF NOT EXISTS idx_requests_dataset ON requests (dataset_id, id);
    """,
    # v2 — budget reservations with TTL.
    CREATE_RESERVATIONS_SQL,
)


class DatasetExists(Exception):
    """Raised when registering a dataset_id that is already present."""


class DatasetNotFound(Exception):
    """Raised when a dataset_id is missing."""


class RequestConflict(Exception):
    """Same request_id reused with a different payload (HTTP 409)."""


class ReservationNotFound(Exception):
    """Raised when a (dataset_id, reservation_id) pair is missing."""


class ReservationConflict(Exception):
    """Same reservation_id reused with a different payload (HTTP 409)."""


class ReservationStateError(Exception):
    """Invalid state transition for a reservation (HTTP 409).

    Carries a machine-readable ``code`` such as ``reservation_expired`` or
    ``reservation_already_confirmed`` plus a human-readable ``detail``.
    """

    def __init__(self, reservation_id: str, code: str, detail: str) -> None:
        super().__init__(reservation_id, code, detail)
        self.reservation_id = reservation_id
        self.code = code
        self.detail = detail


@asynccontextmanager
async def _connect() -> AsyncIterator[aiosqlite.Connection]:
    conn = aiosqlite.connect(settings.db_path)
    # row_factory must be set on the live connection, i.e. after __aenter__.
    async with conn as db:
        db.row_factory = aiosqlite.Row
        yield db


async def init_db() -> None:
    directory = os.path.dirname(os.path.abspath(settings.db_path))
    os.makedirs(directory, exist_ok=True)
    async with _connect() as db:
        await db.execute(f"PRAGMA busy_timeout = {int(settings.busy_timeout_ms)}")
        await db.execute("PRAGMA journal_mode = WAL")
        await db.execute("PRAGMA foreign_keys = ON")
        async with db.execute("PRAGMA user_version") as cur:
            row = await cur.fetchone()
            version = int(row[0]) if row is not None else 0
        for migration_version, statement in enumerate(MIGRATIONS, start=1):
            if migration_version > version:
                await db.executescript(statement)
                await db.execute(f"PRAGMA user_version = {migration_version}")
        await db.commit()


@asynccontextmanager
async def connect() -> AsyncIterator[aiosqlite.Connection]:
    """Connection with Row factory; usage: ``async with db.connect() as c``."""
    conn = aiosqlite.connect(settings.db_path)
    async with conn as c:
        c.row_factory = aiosqlite.Row
        yield c


async def begin_immediate(db: aiosqlite.Connection) -> None:
    await db.execute(f"PRAGMA busy_timeout = {int(settings.busy_timeout_ms)}")
    await db.execute("BEGIN IMMEDIATE")


# ---------------------------------------------------------------- datasets --


async def create_dataset(
    dataset_id: str,
    epsilon_cap: float,
    delta_cap: float,
    description: Optional[str],
    created_at: str,
) -> None:
    async with _connect() as db:
        await begin_immediate(db)
        try:
            await db.execute(
                "INSERT INTO datasets "
                "(dataset_id, epsilon_cap, delta_cap, description, created_at) "
                "VALUES (?, ?, ?, ?, ?)",
                (dataset_id, epsilon_cap, delta_cap, description, created_at),
            )
            await db.commit()
        except aiosqlite.IntegrityError:
            await db.rollback()
            raise DatasetExists(dataset_id)


async def get_dataset_row(db: aiosqlite.Connection, dataset_id: str) -> Optional[aiosqlite.Row]:
    async with db.execute(
        "SELECT dataset_id, epsilon_cap, delta_cap, description, created_at "
        "FROM datasets WHERE dataset_id = ?",
        (dataset_id,),
    ) as cur:
        return await cur.fetchone()


async def require_dataset(db: aiosqlite.Connection, dataset_id: str) -> aiosqlite.Row:
    row = await get_dataset_row(db, dataset_id)
    if row is None:
        raise DatasetNotFound(dataset_id)
    return row


# ---------------------------------------------------------------- requests --


async def list_granted_entries(
    db: aiosqlite.Connection, dataset_id: str
) -> list[tuple[float, float]]:
    """All granted mechanisms as ``(epsilon, delta)`` pairs, in grant order."""
    async with db.execute(
        "SELECT epsilon, delta FROM requests "
        "WHERE dataset_id = ? AND status = 'granted' ORDER BY id",
        (dataset_id,),
    ) as cur:
        rows = await cur.fetchall()
    return [(row[0], row[1]) for row in rows]


async def insert_request(
    db: aiosqlite.Connection,
    *,
    dataset_id: str,
    request_id: str,
    query_type: str,
    epsilon: float,
    delta: float,
    status: str,
    reason: Optional[str],
    epsilon_before: float,
    epsilon_after: float,
    rdp_best_order: float,
    created_at: str,
) -> None:
    await db.execute(
        "INSERT INTO requests "
        "(dataset_id, request_id, query_type, epsilon, delta, status, reason, "
        "epsilon_before, epsilon_after, rdp_best_order, created_at) "
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        (
            dataset_id, request_id, query_type, epsilon, delta, status, reason,
            epsilon_before, epsilon_after, rdp_best_order, created_at,
        ),
    )


async def get_request(
    db: aiosqlite.Connection, dataset_id: str, request_id: str
) -> Optional[aiosqlite.Row]:
    async with db.execute(
        "SELECT dataset_id, request_id, query_type, epsilon, delta, status, "
        "reason, epsilon_before, epsilon_after, rdp_best_order, created_at "
        "FROM requests WHERE dataset_id = ? AND request_id = ?",
        (dataset_id, request_id),
    ) as cur:
        return await cur.fetchone()


async def list_requests(db: aiosqlite.Connection, dataset_id: str) -> list[aiosqlite.Row]:
    async with db.execute(
        "SELECT dataset_id, request_id, query_type, epsilon, delta, status, "
        "reason, epsilon_before, epsilon_after, rdp_best_order, created_at "
        "FROM requests WHERE dataset_id = ? ORDER BY id",
        (dataset_id,),
    ) as cur:
        return await cur.fetchall()


# ------------------------------------------------------------ reservations --

_RESERVATION_COLS = (
    "id, dataset_id, reservation_id, epsilon, delta, ttl_seconds, status, "
    "reason, request_id, epsilon_before, epsilon_after, rdp_best_order, "
    "created_at, expires_at, confirmed_at, released_at"
)


async def insert_reservation(
    db: aiosqlite.Connection,
    *,
    dataset_id: str,
    reservation_id: str,
    epsilon: float,
    delta: float,
    ttl_seconds: float,
    status: str,
    reason: Optional[str],
    epsilon_before: float,
    epsilon_after: float,
    rdp_best_order: float,
    created_at: str,
    expires_at: str,
) -> None:
    await db.execute(
        "INSERT INTO reservations "
        "(dataset_id, reservation_id, epsilon, delta, ttl_seconds, status, "
        "reason, epsilon_before, epsilon_after, rdp_best_order, created_at, "
        "expires_at) "
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        (
            dataset_id, reservation_id, epsilon, delta, ttl_seconds, status,
            reason, epsilon_before, epsilon_after, rdp_best_order, created_at,
            expires_at,
        ),
    )


async def get_reservation(
    db: aiosqlite.Connection, dataset_id: str, reservation_id: str
) -> Optional[aiosqlite.Row]:
    async with db.execute(
        f"SELECT {_RESERVATION_COLS} FROM reservations "
        "WHERE dataset_id = ? AND reservation_id = ?",
        (dataset_id, reservation_id),
    ) as cur:
        return await cur.fetchone()


async def list_reservations(db: aiosqlite.Connection, dataset_id: str) -> list[aiosqlite.Row]:
    async with db.execute(
        f"SELECT {_RESERVATION_COLS} FROM reservations "
        "WHERE dataset_id = ? ORDER BY id",
        (dataset_id,),
    ) as cur:
        return await cur.fetchall()


async def list_active_reservations(
    db: aiosqlite.Connection, dataset_id: str
) -> list[aiosqlite.Row]:
    """Rows still marked ``active``; TTL expiry is evaluated by the caller."""
    async with db.execute(
        f"SELECT {_RESERVATION_COLS} FROM reservations "
        "WHERE dataset_id = ? AND status = 'active' ORDER BY id",
        (dataset_id,),
    ) as cur:
        return await cur.fetchall()


async def mark_reservation_confirmed(
    db: aiosqlite.Connection,
    dataset_id: str,
    reservation_id: str,
    *,
    request_id: str,
    confirmed_at: str,
) -> None:
    await db.execute(
        "UPDATE reservations SET status = 'confirmed', request_id = ?, "
        "confirmed_at = ? WHERE dataset_id = ? AND reservation_id = ?",
        (request_id, confirmed_at, dataset_id, reservation_id),
    )


async def mark_reservation_released(
    db: aiosqlite.Connection,
    dataset_id: str,
    reservation_id: str,
    *,
    released_at: str,
) -> None:
    await db.execute(
        "UPDATE reservations SET status = 'released', released_at = ? "
        "WHERE dataset_id = ? AND reservation_id = ?",
        (released_at, dataset_id, reservation_id),
    )


async def mark_reservation_expired(db: aiosqlite.Connection, row_id: int) -> None:
    await db.execute(
        "UPDATE reservations SET status = 'expired' "
        "WHERE id = ? AND status = 'active'",
        (row_id,),
    )
