from __future__ import annotations

import asyncio
import json

import httpx
import pytest
from httpx import ASGITransport

from app.broadcaster import Broadcaster
from app.sse import parse_last_id, stream_events


# ----- parse_last_id unit tests -----


def test_parse_last_id_header_wins_over_cursor():
    assert parse_last_id("7", 3) == 7


def test_parse_last_id_falls_back_to_cursor():
    assert parse_last_id(None, 4) == 4


def test_parse_last_id_ignores_malformed_header():
    assert parse_last_id("not-a-number", 2) == 2


def test_parse_last_id_clamps_negative_cursor():
    assert parse_last_id(None, -5) == 0


# ----- stream_events: direct async-iteration unit tests -----


async def _collect(gen, limit: int, timeout: float = 1.5) -> list[dict]:
    out: list[dict] = []

    async def consume():
        async for evt in gen:
            out.append(evt)
            if len(out) >= limit:
                return

    await asyncio.wait_for(consume(), timeout=timeout)
    return out


async def test_stream_events_replays_ring_on_connect():
    bc = Broadcaster(bucket_names=["low"], ring_size=16)
    bc.publish("low", "upsert", {"opportunity_id": 1, "profit_margin_bps": 400})
    bc.publish("low", "upsert", {"opportunity_id": 2, "profit_margin_bps": 450})

    gen = stream_events(
        broadcaster=bc, bucket="low", last_id=0, heartbeat_seconds=0.1,
    )
    events = await _collect(gen, limit=2)
    assert [int(e["id"]) for e in events] == [1, 2]
    assert events[0]["event"] == "upsert"
    assert json.loads(events[0]["data"])["opportunity_id"] == 1
    await gen.aclose()


async def test_stream_events_skips_already_seen_via_last_id():
    bc = Broadcaster(bucket_names=["low"], ring_size=16)
    for i in range(1, 4):
        bc.publish("low", "upsert", {"opportunity_id": i})

    gen = stream_events(
        broadcaster=bc, bucket="low", last_id=2, heartbeat_seconds=0.1,
    )
    events = await _collect(gen, limit=1)
    assert [int(e["id"]) for e in events] == [3]
    await gen.aclose()


async def test_stream_events_emits_snapshot_stale_on_gap():
    bc = Broadcaster(bucket_names=["low"], ring_size=3)
    for i in range(6):
        bc.publish("low", "upsert", {"i": i})
    # oldest ring id is 4; client's last_id=1 → gap
    gen = stream_events(
        broadcaster=bc, bucket="low", last_id=1, heartbeat_seconds=0.1,
    )
    events = await _collect(gen, limit=1)
    assert len(events) == 1
    assert events[0]["event"] == "snapshot_stale"
    data = json.loads(events[0]["data"])
    assert data == {"reason": "cursor_gap", "recommended": "/snapshot/low"}
    # gap generator returns immediately, so the next pull should end.
    async for _ in gen:
        pytest.fail("generator should have returned after snapshot_stale")


async def test_stream_events_receives_live_publishes_after_subscribe():
    bc = Broadcaster(bucket_names=["low"], ring_size=16)
    gen = stream_events(
        broadcaster=bc, bucket="low", last_id=0, heartbeat_seconds=0.1,
    )

    async def driver():
        await asyncio.sleep(0.05)
        bc.publish("low", "upsert", {"opportunity_id": 1})
        await asyncio.sleep(0.05)
        bc.publish("low", "delete", {"opportunity_id": 1})

    events: list[dict] = []

    async def consume():
        async for evt in gen:
            events.append(evt)
            if len(events) >= 2:
                return

    await asyncio.wait_for(
        asyncio.gather(driver(), consume()), timeout=2.0,
    )
    assert [e["event"] for e in events] == ["upsert", "delete"]
    assert [int(e["id"]) for e in events] == [1, 2]
    await gen.aclose()


async def test_stream_events_disconnect_terminates_loop():
    """When is_disconnected returns True, the generator must exit
    within one heartbeat tick even if no events arrive."""
    bc = Broadcaster(bucket_names=["low"], ring_size=16)
    state = {"closed": False}

    async def is_disc() -> bool:
        return state["closed"]

    gen = stream_events(
        broadcaster=bc, bucket="low", last_id=0,
        heartbeat_seconds=0.05, is_disconnected=is_disc,
    )

    async def flip_after_delay():
        await asyncio.sleep(0.1)
        state["closed"] = True

    async def consume():
        async for _ in gen:
            pass

    await asyncio.wait_for(
        asyncio.gather(flip_after_delay(), consume()), timeout=1.0,
    )
    assert bc.subscriber_count("low") == 0


# ----- HTTP layer smoke: just confirm routing + 404 ------


async def test_unknown_bucket_returns_404(app_with_state):
    transport = ASGITransport(app=app_with_state)
    async with httpx.AsyncClient(
        transport=transport, base_url="http://testserver",
    ) as c:
        r = await c.get("/stream/nope")
        assert r.status_code == 404
