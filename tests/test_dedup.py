from __future__ import annotations

from app.dedup import dedup, dedup_key


def _row(opp_id, group_id, bps, bookmakers, last_seen_at="2026-04-21T09:00:00+03:00"):
    return {
        "opportunity_id": opp_id,
        "group_id": group_id,
        "profit_margin_bps": bps,
        "last_seen_at": last_seen_at,
        "legs": [{"bookmaker": b, "leg_index": i} for i, b in enumerate(bookmakers)],
    }


def test_dedup_key_sorts_bookmakers():
    r = _row(1, 42, 400, ["sport_pesa", "betika", "odi"])
    assert dedup_key(r) == (42, ("betika", "odi", "sport_pesa"))


def test_dedup_group_4262_real_case():
    """Real fixture from repo/arbitrage.db group_id=4262:
    opp 1 at 428 bps and opp 2 at 443 bps, both over {betika, odi, sport_pesa}.
    Winner must be opp 2 (higher bps)."""
    rows = [
        _row(1, 4262, 428, ["betika", "odi", "sport_pesa"]),
        _row(2, 4262, 443, ["betika", "odi", "sport_pesa"]),
    ]
    out = dedup(rows)
    assert len(out) == 1
    assert out[0]["opportunity_id"] == 2
    assert out[0]["profit_margin_bps"] == 443


def test_dedup_keeps_both_when_bookmaker_sets_differ():
    rows = [
        _row(1, 4262, 428, ["betika", "odi", "sport_pesa"]),
        _row(2, 4262, 400, ["betika", "odi", "betpawa"]),  # different set
    ]
    out = dedup(rows)
    assert len(out) == 2


def test_dedup_keeps_both_when_group_differs():
    rows = [
        _row(1, 4262, 428, ["betika", "odi", "sport_pesa"]),
        _row(2, 4263, 400, ["betika", "odi", "sport_pesa"]),
    ]
    out = dedup(rows)
    assert len(out) == 2


def test_dedup_tiebreak_newest_wins_on_equal_bps():
    rows = [
        _row(5, 1, 500, ["a", "b"], last_seen_at="2026-04-21T09:00:00+03:00"),
        _row(6, 1, 500, ["a", "b"], last_seen_at="2026-04-21T10:00:00+03:00"),
    ]
    out = dedup(rows)
    assert out[0]["opportunity_id"] == 6


def test_dedup_tiebreak_lowest_id_on_equal_bps_and_timestamp():
    ts = "2026-04-21T09:00:00+03:00"
    rows = [
        _row(9, 1, 500, ["a", "b"], last_seen_at=ts),
        _row(3, 1, 500, ["a", "b"], last_seen_at=ts),
    ]
    out = dedup(rows)
    assert out[0]["opportunity_id"] == 3


def test_dedup_stable_across_input_order():
    a = _row(1, 4262, 428, ["betika", "odi", "sport_pesa"])
    b = _row(2, 4262, 443, ["betika", "odi", "sport_pesa"])
    out1 = dedup([a, b])
    out2 = dedup([b, a])
    assert out1[0]["opportunity_id"] == out2[0]["opportunity_id"] == 2


def test_dedup_empty_input():
    assert dedup([]) == []
