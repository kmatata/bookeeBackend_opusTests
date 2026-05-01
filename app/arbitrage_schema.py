"""Copy of the ETL's arbitrage DDL (bookeeETL_opusTests/arbitrage/schema.py).

Used as a fallback when the snapshot builder must emit an empty-but-valid
SQLite file before the ETL has ever pushed one; and as the fixture source
in tests. When the ETL has pushed at least once the snapshot builder
prefers replicating the source DB's own `sqlite_master` so schema drift
is picked up automatically.
"""

from __future__ import annotations

ARBITRAGE_SCHEMA_SQL = r"""
CREATE TABLE arb_runs (
    id                    INTEGER PRIMARY KEY AUTOINCREMENT,
    started_at            TEXT NOT NULL,
    ended_at              TEXT,
    source_type           TEXT NOT NULL,
    market_type           TEXT NOT NULL,
    groups_scanned        INTEGER NOT NULL DEFAULT 0,
    opportunities_found   INTEGER NOT NULL DEFAULT 0,
    threshold             REAL NOT NULL,
    min_confidence        TEXT NOT NULL,
    stake                 REAL NOT NULL
);

CREATE TABLE arb_opportunities (
    id                       INTEGER PRIMARY KEY AUTOINCREMENT,
    group_id                 INTEGER NOT NULL,
    source_type              TEXT NOT NULL,
    market_type              TEXT NOT NULL,
    target_date              TEXT NOT NULL,
    start_time               TEXT NOT NULL,
    canonical_home           TEXT NOT NULL,
    canonical_away           TEXT NOT NULL,
    competition              TEXT,
    country                  TEXT,
    confidence               TEXT,
    n_legs                   INTEGER NOT NULL,
    leg_signature            TEXT NOT NULL,
    inverse_odds_sum         REAL NOT NULL,
    profit_margin_bps        INTEGER NOT NULL,
    total_stake              REAL NOT NULL,
    guaranteed_return        REAL NOT NULL,
    guaranteed_profit        REAL NOT NULL,
    oldest_odd_updated_at    TEXT NOT NULL,
    latest_odd_updated_at    TEXT NOT NULL,
    first_seen_at            TEXT NOT NULL,
    last_seen_at             TEXT NOT NULL,
    last_run_id              INTEGER REFERENCES arb_runs(id),
    UNIQUE (group_id, leg_signature)
);

CREATE TABLE arb_legs (
    opportunity_id     INTEGER NOT NULL REFERENCES arb_opportunities(id) ON DELETE CASCADE,
    leg_index          INTEGER NOT NULL,
    bookmaker          TEXT NOT NULL,
    outcome            TEXT NOT NULL,
    odd                REAL NOT NULL,
    stake              REAL NOT NULL,
    expected_return    REAL NOT NULL,
    event_id           INTEGER NOT NULL,
    bookmaker_event_id TEXT,
    fetch_url          TEXT,
    event_active       TEXT,
    odd_updated_at     TEXT NOT NULL,
    event_updated_at   TEXT,
    PRIMARY KEY (opportunity_id, leg_index),
    UNIQUE (opportunity_id, bookmaker)
);

CREATE VIEW arb_view AS
SELECT
    o.id                       AS opportunity_id,
    o.group_id,
    o.source_type,
    o.market_type,
    o.target_date,
    o.start_time,
    o.canonical_home,
    o.canonical_away,
    o.competition,
    o.country,
    o.confidence,
    o.n_legs,
    o.inverse_odds_sum,
    o.profit_margin_bps,
    o.total_stake,
    o.guaranteed_return,
    o.guaranteed_profit,
    o.oldest_odd_updated_at,
    o.latest_odd_updated_at,
    o.first_seen_at,
    o.last_seen_at,
    CASE
        WHEN source_type = 'live'
             AND datetime(start_time, '-3 hours') < datetime('now', '-150 minutes')
            THEN 'EXPIRED'
        WHEN source_type = 'live'
             AND julianday(oldest_odd_updated_at) < julianday('now') - 0.001388888888888889
            THEN 'STALE'
        WHEN source_type != 'live'
             AND julianday(last_seen_at) < julianday('now') - 0.006944444444444444
            THEN 'STALE'
        ELSE 'ACTIVE'
    END                        AS status,
    json_group_array(
        json_object(
            'leg_index',           l.leg_index,
            'bookmaker',           l.bookmaker,
            'outcome',             l.outcome,
            'odd',                 l.odd,
            'stake',               l.stake,
            'expected_return',     l.expected_return,
            'event_id',            l.event_id,
            'bookmaker_event_id',  l.bookmaker_event_id,
            'fetch_url',           l.fetch_url,
            'event_active',        l.event_active,
            'odd_updated_at',      l.odd_updated_at,
            'event_updated_at',    l.event_updated_at
        )
    )                          AS legs_json
FROM arb_opportunities o
LEFT JOIN arb_legs l ON l.opportunity_id = o.id
GROUP BY o.id;
"""
