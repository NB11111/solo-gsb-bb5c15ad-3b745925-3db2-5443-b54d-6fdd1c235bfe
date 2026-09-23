"""FastAPI application: differential-privacy budget ledger."""
from __future__ import annotations

from typing import Annotated, Optional

from contextlib import asynccontextmanager

from fastapi import FastAPI, Path, Request, status
from fastapi.responses import JSONResponse

from . import db, ledger
from .schemas import (
    AnalysisRequest,
    ConfirmVerdict,
    DatasetInfo,
    DatasetRegister,
    RequestList,
    ReservationConfirm,
    ReservationCreate,
    ReservationList,
    ReservationMutation,
    ReservationRecord,
    ReservationVerdict,
    Verdict,
    BudgetView,
)

DATASET_ID = Annotated[
    str,
    Path(
        min_length=1,
        max_length=200,
        pattern=r"^[A-Za-z0-9._\-]+$",
        description="Client-chosen dataset identifier.",
    ),
]

RESERVATION_ID = Annotated[
    str,
    Path(
        min_length=1,
        max_length=200,
        pattern=r"^[A-Za-z0-9._\-]+$",
        description="Client-chosen reservation identifier.",
    ),
]

@asynccontextmanager
async def lifespan(_: FastAPI):
    await db.init_db()
    yield


app = FastAPI(
    title="Differential-Privacy Budget Ledger",
    version="1.1.0",
    lifespan=lifespan,
    description=(
        "Register epsilon/delta caps per dataset and submit analysis "
        "requests.  Composition is computed on a grid of Rényi-DP orders and "
        "converted back to (epsilon, delta)-DP.  Repeated request_ids are "
        "idempotent; over-budget submissions are atomically rejected under "
        "concurrency.  Budget reservations hold (epsilon, delta) for a TTL "
        "and can be confirmed into analysis requests or released; expired "
        "holds free their budget automatically."
    ),
)


@app.exception_handler(db.DatasetNotFound)
async def _not_found(_: Request, exc: db.DatasetNotFound) -> JSONResponse:
    return JSONResponse(
        status_code=status.HTTP_404_NOT_FOUND,
        content={"error": "dataset_not_found", "dataset_id": exc.args[0]},
    )


@app.exception_handler(db.DatasetExists)
async def _exists(_: Request, exc: db.DatasetExists) -> JSONResponse:
    return JSONResponse(
        status_code=status.HTTP_409_CONFLICT,
        content={"error": "dataset_already_exists", "dataset_id": exc.args[0]},
    )


@app.exception_handler(db.RequestConflict)
async def _conflict(_: Request, exc: db.RequestConflict) -> JSONResponse:
    return JSONResponse(
        status_code=status.HTTP_409_CONFLICT,
        content={
            "error": "request_id_conflict",
            "request_id": exc.args[0],
            "detail": (
                "request_id was already used for a different request payload"
            ),
        },
    )


@app.exception_handler(db.ReservationNotFound)
async def _reservation_not_found(
    _: Request, exc: db.ReservationNotFound
) -> JSONResponse:
    return JSONResponse(
        status_code=status.HTTP_404_NOT_FOUND,
        content={
            "error": "reservation_not_found",
            "reservation_id": exc.args[0],
        },
    )


@app.exception_handler(db.ReservationConflict)
async def _reservation_conflict(
    _: Request, exc: db.ReservationConflict
) -> JSONResponse:
    return JSONResponse(
        status_code=status.HTTP_409_CONFLICT,
        content={
            "error": "reservation_id_conflict",
            "reservation_id": exc.args[0],
            "detail": (
                "reservation_id was already used for a different payload"
            ),
        },
    )


@app.exception_handler(db.ReservationStateError)
async def _reservation_state(
    _: Request, exc: db.ReservationStateError
) -> JSONResponse:
    return JSONResponse(
        status_code=status.HTTP_409_CONFLICT,
        content={
            "error": exc.code,
            "reservation_id": exc.reservation_id,
            "detail": exc.detail,
        },
    )


@app.get("/health", tags=["meta"])
async def health() -> dict[str, str]:
    return {"status": "ok"}


@app.post(
    "/datasets/{dataset_id}",
    response_model=DatasetInfo,
    status_code=status.HTTP_201_CREATED,
    tags=["datasets"],
)
async def register_dataset(dataset_id: DATASET_ID, payload: DatasetRegister) -> dict:
    return await ledger.register_dataset(dataset_id, payload)


@app.get(
    "/datasets/{dataset_id}",
    response_model=DatasetInfo,
    tags=["datasets"],
)
async def get_dataset(dataset_id: DATASET_ID) -> dict:
    return await ledger.get_dataset_info(dataset_id)


@app.get(
    "/datasets/{dataset_id}/budget",
    response_model=BudgetView,
    tags=["datasets"],
)
async def get_budget(dataset_id: DATASET_ID) -> dict:
    info = await ledger.get_dataset_info(dataset_id)
    return info["budget"]


@app.post(
    "/datasets/{dataset_id}/requests",
    response_model=Verdict,
    tags=["requests"],
)
async def submit_request(dataset_id: DATASET_ID, payload: AnalysisRequest):
    verdict, http_status = await ledger.submit_analysis(dataset_id, payload)
    return JSONResponse(status_code=http_status, content=verdict)


@app.get(
    "/datasets/{dataset_id}/requests",
    response_model=RequestList,
    tags=["requests"],
)
async def get_requests(dataset_id: DATASET_ID) -> dict:
    return await ledger.list_requests(dataset_id)


# -------------------------------------------------------------- reservations --


@app.post(
    "/datasets/{dataset_id}/reservations",
    response_model=ReservationVerdict,
    tags=["reservations"],
)
async def create_reservation(dataset_id: DATASET_ID, payload: ReservationCreate):
    verdict, http_status = await ledger.create_reservation(dataset_id, payload)
    return JSONResponse(status_code=http_status, content=verdict)


@app.get(
    "/datasets/{dataset_id}/reservations",
    response_model=ReservationList,
    tags=["reservations"],
)
async def get_reservations(dataset_id: DATASET_ID) -> dict:
    return await ledger.list_reservations(dataset_id)


@app.get(
    "/datasets/{dataset_id}/reservations/{reservation_id}",
    response_model=ReservationRecord,
    tags=["reservations"],
)
async def get_reservation(dataset_id: DATASET_ID, reservation_id: RESERVATION_ID) -> dict:
    return await ledger.get_reservation_info(dataset_id, reservation_id)


@app.post(
    "/datasets/{dataset_id}/reservations/{reservation_id}/confirm",
    response_model=ConfirmVerdict,
    tags=["reservations"],
)
async def confirm_reservation(
    dataset_id: DATASET_ID,
    reservation_id: RESERVATION_ID,
    payload: Optional[ReservationConfirm] = None,
):
    verdict, http_status = await ledger.confirm_reservation(
        dataset_id, reservation_id, payload
    )
    return JSONResponse(status_code=http_status, content=verdict)


@app.post(
    "/datasets/{dataset_id}/reservations/{reservation_id}/release",
    response_model=ReservationMutation,
    tags=["reservations"],
)
async def release_reservation(dataset_id: DATASET_ID, reservation_id: RESERVATION_ID):
    result, http_status = await ledger.release_reservation(dataset_id, reservation_id)
    return JSONResponse(status_code=http_status, content=result)
