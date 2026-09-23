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

SCHEMA = """
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
"""


class DatasetExists(Exception):
    """Raised when registering a dataset_id that is already present."""


class DatasetNotFound(Exception):
    """Raised when a dataset_id is missing."""


class RequestConflict(Exception):
    """Same request_id reused with a different payload (HTTP 409)."""


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
        await db.executescript(SCHEMA)
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
