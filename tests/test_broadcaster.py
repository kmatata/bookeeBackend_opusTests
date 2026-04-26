from __future__ import annotations

import asyncio

import pytest

from app.broadcaster import Broadcaster


def _bc(ring_size: int = 8) -> Broadcaster:
    return Broadcaster(bucket_names=["low", "mid"], ring_size=ring_size)


def test_publish_assigns_monotonic_ids():
    bc = _bc()
    e1 = bc.publish("low", "upsert", {"id": 1})
    e2 = bc.publish("low", "upsert", {"id": 2})
    assert (e1.id, e2.id) == (1, 2)
    assert bc.current_cursor("low") == 2


def test_publish_rejects_unknown_bucket():
    bc = _bc()
    with pytest.raises(KeyError):
        bc.publish("bogus", "upsert", {})


def test_ring_buffer_evicts_oldest():
    bc = _bc(ring_size=3)
    for i in range(5):
        bc.publish("low", "upsert", {"i": i})
    assert bc.oldest_event_id("low") == 3
    assert bc.current_cursor("low") == 5


def test_replay_since_filters_by_id():
    bc = _bc()
    for i in range(5):
        bc.publish("low", "upsert", {"i": i})
    out = bc.replay_since("low", 2)
    assert [e.id for e in out] == [3, 4, 5]


def test_has_gap_detects_ring_overflow():
    bc = _bc(ring_size=3)
    for i in range(5):
        bc.publish("low", "upsert", {"i": i})
    # oldest=3; client with last_id=1 can't be replayed cleanly
    assert bc.has_gap("low", last_id=1)
    # last_id=2 would need event id 3 — which is the oldest, so no gap
    assert not bc.has_gap("low", last_id=2)


def test_has_gap_false_when_empty_ring():
    bc = _bc()
    assert not bc.has_gap("low", last_id=0)


async def test_subscribe_receives_new_events():
    bc = _bc()
    q = bc.subscribe("low")
    bc.publish("low", "upsert", {"x": 1})
    bc.publish("low", "delete", {"opportunity_id": 99})
    e1 = await asyncio.wait_for(q.get(), timeout=1.0)
    e2 = await asyncio.wait_for(q.get(), timeout=1.0)
    assert e1.event == "upsert" and e1.data == {"x": 1}
    assert e2.event == "delete" and e2.data["opportunity_id"] == 99


async def test_slow_subscriber_is_dropped():
    bc = Broadcaster(bucket_names=["low"], ring_size=32, subscriber_queue_size=2)
    q = bc.subscribe("low")
    # fill + overflow
    for i in range(5):
        bc.publish("low", "upsert", {"i": i})
    # queue capped at 2, remaining 3 drop this subscriber
    assert bc.subscriber_count("low") == 0
    # the 2 that made it in are intact
    e1 = await asyncio.wait_for(q.get(), timeout=1.0)
    e2 = await asyncio.wait_for(q.get(), timeout=1.0)
    assert (e1.id, e2.id) == (1, 2)


def test_unsubscribe_is_idempotent():
    bc = _bc()
    q = bc.subscribe("low")
    bc.unsubscribe("low", q)
    bc.unsubscribe("low", q)  # no raise
    assert bc.subscriber_count("low") == 0


def test_cursor_and_oldest_on_fresh_bucket():
    bc = _bc()
    assert bc.current_cursor("low") == 0
    assert bc.oldest_event_id("low") is None
