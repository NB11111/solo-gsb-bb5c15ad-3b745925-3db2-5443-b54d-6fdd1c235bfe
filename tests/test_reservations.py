"""End-to-end tests for budget reservations (create/confirm/release/expire)."""
from __future__ import annotations

import asyncio
import itertools
import os
import tempfile

import pytest
import pytest_asyncio

# Point the ledger at an isolated database before importing the app.
_tmp = tempfile.mkdtemp(prefix="dpledger-rsv-test-")
os.environ["DATABASE_PATH"] = os.path.join(_tmp, "test.db")

from httpx import ASGITransport, AsyncClient  # noqa: E402

from app import db, rdp  # noqa: E402
from app.main import app  # noqa: E402


@pytest_asyncio.fixture()
async def client():
    # ASGITransport does not run lifespan/startup events automatically.
    await db.init_db()
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as ac:
        yield ac


_counter = itertools.count()


def _new_dataset_id(name: str) -> str:
    # Unique per call so a shared DB never leaks state between tests.
    return f"ds-rsv-{name}-{next(_counter)}"


async def _register(client, dataset_id: str, eps: float = 1.0, delta: float = 1e-6):
    return await client.post(
        f"/datasets/{dataset_id}",
        json={"epsilon_cap": eps, "delta_cap": delta,
              "description": "reservation test dataset"},
    )


async def _reserve(client, ds, rid, eps=0.2, delta=1e-8, ttl=600.0):
    return await client.post(
        f"/datasets/{ds}/reservations",
        json={"reservation_id": rid, "epsilon": eps, "delta": delta,
              "ttl_seconds": ttl},
    )


async def _budget(client, ds):
    return (await client.get(f"/datasets/{ds}/budget")).json()


# ------------------------------------------------------------- basic flows --


@pytest.mark.asyncio
async def test_create_reservation_granted_and_budget_reflects_hold(client):
    ds = _new_dataset_id("create")
    await _register(client, ds, eps=1.0, delta=1e-6)

    r = await _reserve(client, ds, "rsv-1", eps=0.2, delta=1e-8, ttl=600.0)
    assert r.status_code == 201
    body = r.json()
    assert body["allowed"] is True
    assert body["status"] == "active"
    assert body["idempotent_replay"] is False
    assert body["ttl_seconds"] == 600.0
    assert body["expires_at"] > body["created_at"]
    assert body["epsilon_after"] > body["epsilon_before"] == 0.0

    budget = await _budget(client, ds)
    assert budget["granted_requests"] == 0
    assert budget["active_reservations"] == 1
    assert budget["epsilon_consumed"] == 0.0
    assert budget["epsilon_reserved"] > 0.0
    # No grants yet: committed == reserved, available = cap - committed.
    assert budget["epsilon_committed"] == pytest.approx(
        budget["epsilon_reserved"], abs=1e-12
    )
    assert budget["epsilon_available"] == pytest.approx(
        1.0 - budget["epsilon_committed"], abs=1e-12
    )
    assert budget["epsilon_remaining"] == 1.0


@pytest.mark.asyncio
async def test_reservation_over_budget_is_rejected_and_holds_nothing(client):
    ds = _new_dataset_id("toobig")
    await _register(client, ds, eps=0.3, delta=1e-6)

    r = await _reserve(client, ds, "rsv-huge", eps=0.5, delta=1e-8)
    assert r.status_code == 201
    body = r.json()
    assert body["allowed"] is False
    assert body["status"] == "rejected"
    assert "budget exceeded" in body["reason"]

    budget = await _budget(client, ds)
    assert budget["active_reservations"] == 0
    assert budget["epsilon_reserved"] == 0.0
    assert budget["epsilon_available"] == pytest.approx(0.3)


@pytest.mark.asyncio
async def test_reservation_on_missing_dataset_404(client):
    r = await client.post(
        "/datasets/ds-rsv-missing-none/reservations",
        json={"reservation_id": "x", "epsilon": 0.1, "delta": 1e-8,
              "ttl_seconds": 60.0},
    )
    assert r.status_code == 404
    assert r.json()["error"] == "dataset_not_found"

    r2 = await client.get("/datasets/ds-rsv-missing-none/reservations")
    assert r2.status_code == 404


@pytest.mark.asyncio
async def test_reservation_validation_422(client):
    ds = _new_dataset_id("valid")
    await _register(client, ds)
    bad_ttl = await client.post(
        f"/datasets/{ds}/reservations",
        json={"reservation_id": "bad", "epsilon": 0.1, "delta": 1e-8,
              "ttl_seconds": -5},
    )
    assert bad_ttl.status_code == 422
    bad_eps = await client.post(
        f"/datasets/{ds}/reservations",
        json={"reservation_id": "bad2", "epsilon": 0.0, "delta": 1e-8,
              "ttl_seconds": 60},
    )
    assert bad_eps.status_code == 422
    bad_id = await client.post(
        f"/datasets/{ds}/reservations",
        json={"reservation_id": "has space", "epsilon": 0.1, "delta": 1e-8,
              "ttl_seconds": 60},
    )
    assert bad_id.status_code == 422


# ------------------------------------------------------------- idempotency --


@pytest.mark.asyncio
async def test_reservation_idempotent_replay(client):
    ds = _new_dataset_id("idem")
    await _register(client, ds, eps=1.0, delta=1e-6)
    payload = {"reservation_id": "rsv-idem", "epsilon": 0.2, "delta": 1e-8,
               "ttl_seconds": 300.0}

    first = await client.post(f"/datasets/{ds}/reservations", json=payload)
    assert first.status_code == 201
    assert first.json()["idempotent_replay"] is False

    for _ in range(3):
        again = await client.post(f"/datasets/{ds}/reservations", json=payload)
        assert again.status_code == 200
        body = again.json()
        assert body["idempotent_replay"] is True
        assert body["allowed"] is True
        assert body["created_at"] == first.json()["created_at"]
        assert body["expires_at"] == first.json()["expires_at"]

    budget = await _budget(client, ds)
    assert budget["active_reservations"] == 1
    listing = (await client.get(f"/datasets/{ds}/reservations")).json()
    assert listing["count"] == 1


@pytest.mark.asyncio
async def test_reservation_payload_conflict_409(client):
    ds = _new_dataset_id("conflict")
    await _register(client, ds, eps=1.0, delta=1e-6)
    assert (await _reserve(client, ds, "rsv-k", eps=0.1)).status_code == 201

    for changed in (
        {"reservation_id": "rsv-k", "epsilon": 0.2, "delta": 1e-8,
         "ttl_seconds": 600.0},
        {"reservation_id": "rsv-k", "epsilon": 0.1, "delta": 1e-9,
         "ttl_seconds": 600.0},
        {"reservation_id": "rsv-k", "epsilon": 0.1, "delta": 1e-8,
         "ttl_seconds": 60.0},
    ):
        r = await client.post(f"/datasets/{ds}/reservations", json=changed)
        assert r.status_code == 409
        assert r.json()["error"] == "reservation_id_conflict"


@pytest.mark.asyncio
async def test_rejected_reservation_replays_rejection(client):
    ds = _new_dataset_id("idemrej")
    await _register(client, ds, eps=0.1, delta=1e-6)
    payload = {"reservation_id": "rsv-big", "epsilon": 5.0, "delta": 1e-8,
               "ttl_seconds": 60.0}

    first = await client.post(f"/datasets/{ds}/reservations", json=payload)
    assert first.json()["allowed"] is False
    second = await client.post(f"/datasets/{ds}/reservations", json=payload)
    assert second.status_code == 200
    assert second.json()["idempotent_replay"] is True
    assert second.json()["allowed"] is False
    assert second.json()["status"] == "rejected"


# --------------------------------------------------- holds vs. admissions --


@pytest.mark.asyncio
async def test_reservation_blocks_requests_and_other_reservations(client):
    ds = _new_dataset_id("holds")
    await _register(client, ds, eps=1.0, delta=1e-6)

    r = await _reserve(client, ds, "rsv-hold", eps=0.9, delta=1e-8)
    assert r.json()["allowed"] is True

    # A direct request that would fit without the hold is now rejected.
    direct = await client.post(
        f"/datasets/{ds}/requests",
        json={"request_id": "req-blocked", "query_type": "gaussian_mean",
              "epsilon": 0.9, "delta": 1e-8},
    )
    assert direct.status_code == 201
    assert direct.json()["allowed"] is False
    assert "budget exceeded" in direct.json()["reason"]

    # So is a second reservation of the same size.
    r2 = await _reserve(client, ds, "rsv-blocked", eps=0.9, delta=1e-8)
    assert r2.json()["allowed"] is False

    budget = await _budget(client, ds)
    assert budget["active_reservations"] == 1
    assert budget["granted_requests"] == 0
    assert budget["epsilon_committed"] <= 1.0 + 1e-12


# ------------------------------------------------------------------ confirm --


@pytest.mark.asyncio
async def test_confirm_converts_reservation_into_request(client):
    ds = _new_dataset_id("confirm")
    await _register(client, ds, eps=1.0, delta=1e-6)
    assert (await _reserve(client, ds, "rsv-c", eps=0.2, delta=1e-8)).json()[
        "allowed"
    ] is True

    before = await _budget(client, ds)
    conf = await client.post(
        f"/datasets/{ds}/reservations/rsv-c/confirm",
        json={"request_id": "req-from-rsv", "query_type": "gaussian_mean"},
    )
    assert conf.status_code == 201
    body = conf.json()
    assert body["status"] == "granted"
    assert body["request_id"] == "req-from-rsv"
    assert body["query_type"] == "gaussian_mean"
    assert body["epsilon"] == 0.2
    assert body["delta"] == 1e-8
    assert body["idempotent_replay"] is False
    assert body["confirmed_at"] == body["created_at"]

    after = await _budget(client, ds)
    # The hold became consumption: committed is unchanged, nothing reserved.
    assert after["granted_requests"] == 1
    assert after["active_reservations"] == 0
    assert after["epsilon_reserved"] == 0.0
    assert after["epsilon_consumed"] == pytest.approx(
        before["epsilon_committed"], abs=1e-9
    )
    assert after["epsilon_committed"] == pytest.approx(
        before["epsilon_committed"], abs=1e-9
    )

    # The new request shows up in the ledger like any granted request.
    reqs = (await client.get(f"/datasets/{ds}/requests")).json()
    assert reqs["count"] == 1
    assert reqs["requests"][0]["request_id"] == "req-from-rsv"
    assert reqs["requests"][0]["status"] == "granted"

    # The reservation record reflects the transition.
    rec = (await client.get(f"/datasets/{ds}/reservations/rsv-c")).json()
    assert rec["status"] == "confirmed"
    assert rec["request_id"] == "req-from-rsv"
    assert rec["confirmed_at"] is not None


@pytest.mark.asyncio
async def test_confirm_defaults_and_idempotent_replay(client):
    ds = _new_dataset_id("confidem")
    await _register(client, ds, eps=1.0, delta=1e-6)
    await _reserve(client, ds, "rsv-def", eps=0.1, delta=1e-8)

    first = await client.post(f"/datasets/{ds}/reservations/rsv-def/confirm")
    assert first.status_code == 201
    assert first.json()["request_id"] == "rsv-def"
    assert first.json()["query_type"] == "reserved_analysis"

    for _ in range(3):
        again = await client.post(f"/datasets/{ds}/reservations/rsv-def/confirm")
        assert again.status_code == 200
        assert again.json()["idempotent_replay"] is True
        assert again.json()["request_id"] == "rsv-def"

    budget = await _budget(client, ds)
    assert budget["granted_requests"] == 1
    reqs = (await client.get(f"/datasets/{ds}/requests")).json()
    assert reqs["count"] == 1


@pytest.mark.asyncio
async def test_confirm_replay_with_different_payload_conflicts(client):
    ds = _new_dataset_id("confconf")
    await _register(client, ds, eps=1.0, delta=1e-6)
    await _reserve(client, ds, "rsv-x", eps=0.1, delta=1e-8)

    ok = await client.post(
        f"/datasets/{ds}/reservations/rsv-x/confirm",
        json={"request_id": "req-a"},
    )
    assert ok.status_code == 201

    clash = await client.post(
        f"/datasets/{ds}/reservations/rsv-x/confirm",
        json={"request_id": "req-b"},
    )
    assert clash.status_code == 409
    assert clash.json()["error"] == "reservation_id_conflict"


@pytest.mark.asyncio
async def test_confirm_request_id_collision_with_existing_request(client):
    ds = _new_dataset_id("confclash")
    await _register(client, ds, eps=1.0, delta=1e-6)
    direct = await client.post(
        f"/datasets/{ds}/requests",
        json={"request_id": "req-taken", "query_type": "q",
              "epsilon": 0.1, "delta": 1e-8},
    )
    assert direct.json()["allowed"] is True
    await _reserve(client, ds, "rsv-y", eps=0.1, delta=1e-8)

    r = await client.post(
        f"/datasets/{ds}/reservations/rsv-y/confirm",
        json={"request_id": "req-taken"},
    )
    assert r.status_code == 409
    assert r.json()["error"] == "request_id_conflict"

    # The reservation is untouched and can still be confirmed differently.
    ok = await client.post(f"/datasets/{ds}/reservations/rsv-y/confirm")
    assert ok.status_code == 201


@pytest.mark.asyncio
async def test_confirm_missing_reservation_404(client):
    ds = _new_dataset_id("conf404")
    await _register(client, ds)
    r = await client.post(f"/datasets/{ds}/reservations/nope/confirm")
    assert r.status_code == 404
    assert r.json()["error"] == "reservation_not_found"


# ------------------------------------------------------------------ release --


@pytest.mark.asyncio
async def test_release_frees_hold_and_is_idempotent(client):
    ds = _new_dataset_id("release")
    await _register(client, ds, eps=1.0, delta=1e-6)
    await _reserve(client, ds, "rsv-rel", eps=0.9, delta=1e-8)
    assert (await _budget(client, ds))["epsilon_reserved"] > 0.0

    rel = await client.post(f"/datasets/{ds}/reservations/rsv-rel/release")
    assert rel.status_code == 200
    body = rel.json()
    assert body["status"] == "released"
    assert body["idempotent_replay"] is False
    assert body["released_at"] is not None

    budget = await _budget(client, ds)
    assert budget["active_reservations"] == 0
    assert budget["epsilon_reserved"] == 0.0
    assert budget["epsilon_available"] == pytest.approx(1.0)

    again = await client.post(f"/datasets/{ds}/reservations/rsv-rel/release")
    assert again.status_code == 200
    assert again.json()["idempotent_replay"] is True
    assert again.json()["released_at"] == body["released_at"]

    # A released reservation can no longer be confirmed.
    conf = await client.post(f"/datasets/{ds}/reservations/rsv-rel/confirm")
    assert conf.status_code == 409
    assert conf.json()["error"] == "reservation_already_released"

    rec = (await client.get(f"/datasets/{ds}/reservations/rsv-rel")).json()
    assert rec["status"] == "released"
    assert rec["released_at"] is not None


@pytest.mark.asyncio
async def test_release_confirmed_reservation_409(client):
    ds = _new_dataset_id("relconf")
    await _register(client, ds, eps=1.0, delta=1e-6)
    await _reserve(client, ds, "rsv-rc", eps=0.1, delta=1e-8)
    assert (await client.post(
        f"/datasets/{ds}/reservations/rsv-rc/confirm"
    )).status_code == 201

    rel = await client.post(f"/datasets/{ds}/reservations/rsv-rc/release")
    assert rel.status_code == 409
    assert rel.json()["error"] == "reservation_already_confirmed"


@pytest.mark.asyncio
async def test_release_missing_reservation_404(client):
    ds = _new_dataset_id("rel404")
    await _register(client, ds)
    r = await client.post(f"/datasets/{ds}/reservations/nope/release")
    assert r.status_code == 404
    assert r.json()["error"] == "reservation_not_found"


@pytest.mark.asyncio
async def test_rejected_reservation_is_terminal(client):
    ds = _new_dataset_id("terminal")
    await _register(client, ds, eps=0.1, delta=1e-6)
    r = await _reserve(client, ds, "rsv-term", eps=5.0, delta=1e-8)
    assert r.json()["status"] == "rejected"

    conf = await client.post(f"/datasets/{ds}/reservations/rsv-term/confirm")
    assert conf.status_code == 409
    assert conf.json()["error"] == "reservation_not_active"

    rel = await client.post(f"/datasets/{ds}/reservations/rsv-term/release")
    assert rel.status_code == 409
    assert rel.json()["error"] == "reservation_not_active"


# ------------------------------------------------------------------- expiry --


@pytest.mark.asyncio
async def test_expired_reservation_frees_budget(client):
    ds = _new_dataset_id("expire")
    await _register(client, ds, eps=0.5, delta=1e-6)
    r = await _reserve(client, ds, "rsv-ttl", eps=0.4, delta=1e-8, ttl=0.15)
    assert r.json()["allowed"] is True
    assert (await _budget(client, ds))["active_reservations"] == 1

    await asyncio.sleep(0.3)

    # Queries no longer count the expired hold.
    budget = await _budget(client, ds)
    assert budget["active_reservations"] == 0
    assert budget["epsilon_reserved"] == 0.0
    assert budget["epsilon_available"] == pytest.approx(0.5)

    # The record reports the expired state (lazily on reads).
    rec = (await client.get(f"/datasets/{ds}/reservations/rsv-ttl")).json()
    assert rec["status"] == "expired"

    # A new reservation of the same size fits again.
    r2 = await _reserve(client, ds, "rsv-ttl-2", eps=0.4, delta=1e-8)
    assert r2.json()["allowed"] is True

    # The sweep persisted the expired transition.
    rec2 = (await client.get(f"/datasets/{ds}/reservations/rsv-ttl")).json()
    assert rec2["status"] == "expired"


@pytest.mark.asyncio
async def test_expired_reservation_cannot_confirm_or_release(client):
    ds = _new_dataset_id("expterm")
    await _register(client, ds, eps=1.0, delta=1e-6)
    await _reserve(client, ds, "rsv-old", eps=0.1, delta=1e-8, ttl=0.1)
    await asyncio.sleep(0.25)

    conf = await client.post(f"/datasets/{ds}/reservations/rsv-old/confirm")
    assert conf.status_code == 409
    assert conf.json()["error"] == "reservation_expired"

    rel = await client.post(f"/datasets/{ds}/reservations/rsv-old/release")
    assert rel.status_code == 409
    assert rel.json()["error"] == "reservation_expired"


@pytest.mark.asyncio
async def test_list_reservations_shows_all_states(client):
    ds = _new_dataset_id("listall")
    await _register(client, ds, eps=1.0, delta=1e-6)
    await _reserve(client, ds, "rsv-a", eps=0.1, delta=1e-8)
    await _reserve(client, ds, "rsv-b", eps=0.1, delta=1e-8)
    await _reserve(client, ds, "rsv-c", eps=0.1, delta=1e-8, ttl=0.1)
    await client.post(f"/datasets/{ds}/reservations/rsv-a/confirm")
    await client.post(f"/datasets/{ds}/reservations/rsv-b/release")
    await asyncio.sleep(0.25)

    listing = (await client.get(f"/datasets/{ds}/reservations")).json()
    assert listing["count"] == 3
    by_id = {r["reservation_id"]: r for r in listing["reservations"]}
    assert by_id["rsv-a"]["status"] == "confirmed"
    assert by_id["rsv-a"]["confirmed_at"] is not None
    assert by_id["rsv-b"]["status"] == "released"
    assert by_id["rsv-b"]["released_at"] is not None
    assert by_id["rsv-c"]["status"] == "expired"
    for rec in by_id.values():
        assert rec["created_at"] <= rec["expires_at"]


# ------------------------------------------------------------- concurrency --


@pytest.mark.asyncio
async def test_concurrent_reservations_never_overshoot(client):
    ds = _new_dataset_id("conc")
    cap_eps, cap_delta = 1.0, 1e-6
    await _register(client, ds, eps=cap_eps, delta=cap_delta)

    n = 40
    results = await asyncio.gather(*[
        _reserve(client, ds, f"rsv-c-{i:03d}", eps=0.05, delta=1e-8)
        for i in range(n)
    ])
    assert all(r.status_code in (200, 201) for r in results)
    verdicts = [r.json() for r in results]
    actives = [v for v in verdicts if v["allowed"]]
    rejected = [v for v in verdicts if not v["allowed"]]
    assert len(actives) + len(rejected) == n
    assert len(actives) < n  # 40 x 0.05 cannot all fit under cap 1.0

    # Independently recompute what the committed composition must be.
    curves = [rdp.single_rdp_curve(0.05, 1e-8) for _ in actives]
    committed, _ = rdp.composed_epsilon(curves, cap_delta)
    assert committed <= cap_eps + 1e-12

    budget = await _budget(client, ds)
    assert budget["active_reservations"] == len(actives)
    assert budget["epsilon_committed"] == pytest.approx(committed, abs=1e-9)
    assert budget["epsilon_committed"] <= cap_eps + 1e-12
    assert budget["epsilon_available"] >= -1e-9


@pytest.mark.asyncio
async def test_concurrent_duplicate_reservation_ids_are_serialised(client):
    ds = _new_dataset_id("concidem")
    await _register(client, ds, eps=1.0, delta=1e-6)
    payload = {"reservation_id": "same", "epsilon": 0.1, "delta": 1e-8,
               "ttl_seconds": 300.0}

    results = await asyncio.gather(*[
        client.post(f"/datasets/{ds}/reservations", json=payload)
        for _ in range(20)
    ])
    created = [r for r in results if r.status_code == 201]
    replays = [r for r in results if r.status_code == 200]
    assert len(created) == 1
    assert len(replays) == 19
    budget = await _budget(client, ds)
    assert budget["active_reservations"] == 1


@pytest.mark.asyncio
async def test_concurrent_confirms_create_single_request(client):
    ds = _new_dataset_id("concconf")
    await _register(client, ds, eps=1.0, delta=1e-6)
    await _reserve(client, ds, "rsv-race", eps=0.2, delta=1e-8)

    results = await asyncio.gather(*[
        client.post(f"/datasets/{ds}/reservations/rsv-race/confirm")
        for _ in range(10)
    ])
    created = [r for r in results if r.status_code == 201]
    replays = [r for r in results if r.status_code == 200]
    assert len(created) == 1
    assert len(replays) == 9

    budget = await _budget(client, ds)
    assert budget["granted_requests"] == 1
    assert budget["active_reservations"] == 0
    reqs = (await client.get(f"/datasets/{ds}/requests")).json()
    assert reqs["count"] == 1


@pytest.mark.asyncio
async def test_concurrent_confirm_and_release_race_is_safe(client):
    ds = _new_dataset_id("concrace")
    await _register(client, ds, eps=1.0, delta=1e-6)
    await _reserve(client, ds, "rsv-cr", eps=0.2, delta=1e-8)

    confirm, release = await asyncio.gather(
        client.post(f"/datasets/{ds}/reservations/rsv-cr/confirm"),
        client.post(f"/datasets/{ds}/reservations/rsv-cr/release"),
    )
    outcomes = sorted([confirm.status_code, release.status_code])
    # Exactly one transition wins; the loser sees a state error.
    assert outcomes == [201, 409] or outcomes == [200, 409]

    budget = await _budget(client, ds)
    # Either way the hold is gone and nothing is double-counted.
    assert budget["active_reservations"] == 0
    if confirm.status_code == 201:
        assert budget["granted_requests"] == 1
        assert budget["epsilon_committed"] == pytest.approx(
            budget["epsilon_consumed"], abs=1e-12
        )
    else:
        assert release.status_code == 200
        assert budget["granted_requests"] == 0
        assert budget["epsilon_committed"] == 0.0


@pytest.mark.asyncio
async def test_mixed_concurrent_load_stays_within_budget(client):
    ds = _new_dataset_id("concmix")
    cap_eps, cap_delta = 1.0, 1e-6
    await _register(client, ds, eps=cap_eps, delta=cap_delta)

    tasks = []
    for i in range(15):
        tasks.append(client.post(
            f"/datasets/{ds}/requests",
            json={"request_id": f"req-{i:03d}", "query_type": "gaussian_mean",
                  "epsilon": 0.05, "delta": 1e-8},
        ))
        tasks.append(_reserve(client, ds, f"rsv-{i:03d}", eps=0.05, delta=1e-8))
    results = await asyncio.gather(*tasks)
    assert all(r.status_code in (200, 201) for r in results)

    budget = await _budget(client, ds)
    assert budget["epsilon_committed"] <= cap_eps + 1e-12
    assert budget["epsilon_consumed"] <= cap_eps + 1e-12

    # Confirm every live reservation; committed must still never exceed cap.
    listing = (await client.get(f"/datasets/{ds}/reservations")).json()
    confirm_results = await asyncio.gather(*[
        client.post(f"/datasets/{ds}/reservations/{r['reservation_id']}/confirm")
        for r in listing["reservations"]
        if r["status"] == "active"
    ])
    assert all(r.status_code in (200, 201) for r in confirm_results)
    final = await _budget(client, ds)
    assert final["active_reservations"] == 0
    assert final["epsilon_consumed"] <= cap_eps + 1e-12
    assert final["epsilon_committed"] == pytest.approx(
        final["epsilon_consumed"], abs=1e-9
    )


# --------------------------------------------------------------- migrations --


@pytest.mark.asyncio
async def test_schema_migrations_are_versioned():
    async with db.connect() as conn:
        async with conn.execute("PRAGMA user_version") as cur:
            row = await cur.fetchone()
    assert row[0] == len(db.MIGRATIONS) == 2


@pytest.mark.asyncio
async def test_migration_from_v1_upgrades_existing_database():
    # Simulate a database written by the pre-reservations release.
    async with db.connect() as conn:
        await conn.execute("DROP TABLE IF EXISTS reservations")
        await conn.execute("PRAGMA user_version = 1")
        await conn.commit()

    await db.init_db()

    async with db.connect() as conn:
        async with conn.execute(
            "SELECT name FROM sqlite_master "
            "WHERE type = 'table' AND name = 'reservations'"
        ) as cur:
            row = await cur.fetchone()
        assert row is not None and row[0] == "reservations"
        async with conn.execute("PRAGMA user_version") as cur:
            version = await cur.fetchone()
    assert version[0] == 2

    # Re-running init is a no-op and keeps the version.
    await db.init_db()
    async with db.connect() as conn:
        async with conn.execute("PRAGMA user_version") as cur:
            assert (await cur.fetchone())[0] == 2
