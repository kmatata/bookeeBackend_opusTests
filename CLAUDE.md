# bookeeBackend — developer notes

## What this is

FastAPI backend that receives data from the ETL (`bookeeETL_opusTests`).

**Default mode (client-side arbitrage):** the ETL pushes `matches.db`
(containing materialised matched odds) to `PUT /ingest/matches`.  The
backend stores the file and fans out ``refresh`` events on
`GET /stream/matches` so clients know to re-fetch
`GET /snapshot/matches`.  Arbitrage calculation runs in the browser.

**Legacy mode (`--legacy-arb` on ETL):** the ETL pushes `arbitrage.db`
(containing pre-computed opportunities) to `PUT /ingest/arbitrage`.  The
backend deduplicates, bucket-partitions, and fans out per-bucket SSE
deltas.  Clients boot via `GET /snapshot/{bucket}` then follow
`GET /stream/{bucket}`.

## Why it only runs inside Docker

`STATE_DIR` (default `/state`) is an **absolute path mounted as a named
Docker volume**. The app reads and writes:

| Path | Purpose |
|------|---------|
| `/state/matches.db` | Latest matched-odds SQLite pushed by ETL (default mode) |
| `/state/matches.db.incoming` | Temp target during matches upload |
| `/state/arbitrage.db` | Latest arbitrage SQLite pushed by ETL (legacy mode) |
| `/state/arbitrage.db.incoming` | Temp target during arbitrage upload |
| `/state/ops.db` | Receive timings + EMA state (persists across restarts) |
| `/state/snapshots/` | Cached per-bucket snapshot files |
| `/state/logs.db` | Structured JSON log records (ts, level, event, data) |

Running `python main.py` outside Docker writes to `/state` on the host
filesystem, which requires root or a pre-created writable `/state` dir.
Tests bypass this by monkey-patching `STATE_DIR` to `tmp_path` via the
`_isolated_state` fixture in `conftest.py`.

## Running tests (no Docker required)

```bash
cd ~/Documents/bookeeBackend_opusTests
python -m pytest tests/ -v
```

All tests are self-contained; fixtures build fresh SQLite DBs in `tmp_path`
and clear the settings cache between runs.

## Running the full stack

`docker-compose.yml` lives in this directory and references the ETL repo at
`../bookeeETL_opusTests`. Both containers join `chrome-net` (the pre-existing
Docker bridge that the `chrome_client` container lives on) so the ETL can
reach the Chrome CDP endpoint and the backend by name simultaneously.

### Mode A — Docker Compose (recommended for integration testing)

```bash
cd ~/Documents/bookeeBackend_opusTests

# Build both images and start
docker compose up --build

# Follow logs for one service
docker compose logs -f bookee-backend
docker compose logs -f bookee-etl

# Probe the backend while compose is running
curl -s http://localhost:8000/health  | jq
curl -s http://localhost:8000/metrics | jq .hypothesis_5s
curl -s http://localhost:8000/buckets | jq

# Tear down (volumes are preserved)
docker compose down
```

The ETL service waits for the backend's `/health` to return 200 before
starting (`depends_on: condition: service_healthy`), so the shipper never
fires before the backend is ready to accept pushes.

### Mode B — ETL local + backend in Docker (recommended during ETL development)

Start the backend container standalone on `chrome-net`:

```bash
docker build -t bookee-backend ~/Documents/bookeeBackend_opusTests
docker run -d \
  --name bookee-backend \
  --network chrome-net \
  -p 8000:8000 \
  -v bookee_state:/state \
  bookee-backend
```

Then run the ETL locally pointing at it:

```bash
cd ~/Documents/bookeeETL_opusTests
export BACKEND_URL=http://localhost:8000
export INGEST_TOKEN=dev-secret
python orchestrator.py
```

The ETL reaches the container via the published port on the host. Changes to
ETL Python code take effect immediately on the next `python orchestrator.py`
restart — no rebuild needed.

To stop the backend container when done:
```bash
docker rm -f bookee-backend
```

### Mode C — fully local (no Docker)

Override `STATE_DIR` so the app writes to a local directory instead of `/state`:

```bash
# terminal 1 — backend
cd ~/Documents/bookeeBackend_opusTests
mkdir -p ./state/snapshots
export STATE_DIR=./state
python main.py

# terminal 2 — ETL
cd ~/Documents/bookeeETL_opusTests
export BACKEND_URL=http://localhost:8000
export INGEST_TOKEN=dev-secret
python orchestrator.py
```

### Verifying end-to-end

After the first ETL scanner pass (~10 s for live, ~10 min for upcoming):

```bash
# samples increments with each push
curl -s http://localhost:8000/metrics | jq .samples

# inter-arrival EMA should converge toward 5000 ms after a few minutes
curl -s http://localhost:8000/metrics | jq .hypothesis_5s

# non-zero counts confirm reconcile is running
curl -s http://localhost:8000/buckets | jq
```

## Running the app (Docker, standalone)

```bash
# build
docker build -t bookee-backend .

# run on chrome-net so ETL containers can reach it by name
docker run -d \
  --name bookee-backend \
  --network chrome-net \
  -p 8000:8000 \
  -v bookee_state:/state \
  bookee-backend
```

The `.env` file is baked into the image at build time (see `Dockerfile`).
Override individual variables with `-e KEY=value` at runtime.

## Simulating an ETL push (curl)

```bash
# push matches.db (default / client-side arb mode)
curl -X PUT http://localhost:8000/ingest/matches \
  -H "Content-Type: application/vnd.sqlite3" \
  -H "X-Ingest-Token: dev-secret" \
  --data-binary @~/Documents/bookeeETL_opusTests/repo/matches.db \
  -D -

# push arbitrage.db (legacy mode)
curl -X PUT http://localhost:8000/ingest/arbitrage \
  -H "Content-Type: application/vnd.sqlite3" \
  -H "X-Ingest-Token: dev-secret" \
  -H "X-Populate-Duration-Ms: 87.4" \
  -H "X-Scanner-Run-Id: 1" \
  --data-binary @~/Documents/bookeeETL_opusTests/repo/arbitrage.db \
  -D -
# inspect X-Upload-Ms, X-Reconcile-Ms, X-E2E-Ms in response headers
```

## Probing the API

```bash
curl -s http://localhost:8000/health   | jq
curl -s http://localhost:8000/meta     | jq
curl -s http://localhost:8000/buckets  | jq
curl -s http://localhost:8000/metrics  | jq

# JSON contents of a bucket (debug, legacy only)
curl -s http://localhost:8000/arbs/low | jq '.[0]'

# SQLite snapshot — matches (client-side arb)
curl -so matches.db --compressed http://localhost:8000/snapshot/matches
sqlite3 matches.db 'SELECT COUNT(*) FROM matched_three_way_odds;'

# SSE stream — matches refresh events (client-side arb)
curl -N http://localhost:8000/stream/matches

# SQLite snapshot — per-bucket (legacy)
curl -so low.db --compressed http://localhost:8000/snapshot/low
sqlite3 low.db 'SELECT COUNT(*) FROM arb_opportunities;'

# SSE stream — per-bucket (legacy)
curl -N http://localhost:8000/stream/low
```

## Retrieving structured logs

Every log line is written to `/state/logs.db` inside the container.
Copy it to the host at any time (container can keep running):

```bash
docker cp bookee-backend:/state/logs.db ./logs.db

# Query recent errors
sqlite3 logs.db "SELECT ts, event, data FROM logs WHERE level IN ('ERROR','WARNING') ORDER BY ts DESC LIMIT 50;"

# All push events
sqlite3 logs.db "SELECT ts, data FROM logs WHERE event='push_received' ORDER BY ts DESC LIMIT 20;"

# SSE connection history
sqlite3 logs.db "SELECT ts, event, data FROM logs WHERE event IN ('sse_connect','sse_disconnect') ORDER BY ts DESC LIMIT 40;"
```

The `data` column holds a JSON object with all event-specific fields
(run_id, bytes, bucket_counts, etc.).

## Key design decisions

### Atomic ingest swap
Body is streamed to `*.db.incoming`, fsync'd, then `os.replace()`'d to
`*.db`. A single `asyncio.Lock` serialises concurrent pushes so the
reconciler always sees a complete file.

### Matches mode (default) — snapshot refresh
The backend does NOT compute row-level deltas for `matches.db`.  It
stores the file and broadcasts a ``refresh`` event on the matches SSE
stream.  Clients react by re-fetching `/snapshot/matches`.  This keeps
the backend stateless and fast — the arbitrage scanner runs in the
browser/extension.

### Legacy mode — dedup + bucket partition
`(group_id, sorted(bookmakers_across_all_legs))` — when the ETL produces
two rows for the same fixture on the same bookmaker set, only the highest
`profit_margin_bps` row is forwarded to clients.

### 409 on duplicate scanner run-id
`X-Scanner-Run-Id` is a monotone push counter (legacy only). If a retry
arrives with `run_id ≤ last_applied_run_id`, the backend returns 409
immediately (inside the asyncio.Lock) without touching the filesystem.

### SSE ring buffer
Default 1024 events per bucket. Reconnecting clients with
`Last-Event-ID ≤ oldest_ring_id - 1` receive `snapshot_stale` and
must re-fetch `/snapshot/{bucket}` before reconnecting.

### Snapshot cache
`GET /snapshot/{bucket}` results are cached per-bucket for
`SNAPSHOT_CACHE_SECONDS` (default 2) so a boot-time client fan-out
triggers one `VACUUM INTO` per bucket rather than N.

## Configuration reference

All settings are loaded from `.env` via `pydantic-settings`. See
`.env.example` for defaults. Key variables:

| Variable | Default | Notes |
|----------|---------|-------|
| `STATE_DIR` | `/state` | Must be writable; mount as Docker volume |
| `INGEST_TOKEN` | `change-me` | Shared secret with ETL shipper |
| `BUCKETS` | `low:300:500,...` | `name:lo_bps:hi_bps` comma-separated |
| `SSE_RING_BUFFER_SIZE` | `1024` | Events per bucket kept for replay |
| `SSE_HEARTBEAT_SECONDS` | `15` | Keepalive + disconnect-detection interval |
| `SNAPSHOT_CACHE_SECONDS` | `2` | Per-bucket snapshot TTL |
| `EMA_ALPHA` | `0.2` | Exponential smoothing factor |
| `EMA_WARMUP_SAMPLES` | `5` | Samples before switching from mean to EMA |

## File layout

```
app/
  __init__.py          FastAPI app instance + router registration
  config.py            Settings (pydantic-settings; NoDecode for BUCKETS)
  lifespan.py          Startup: ops.db init, BucketState, Broadcaster, SnapshotCache
  arbitrage_schema.py  Bundled DDL mirror of ETL schema (views + tables)
  state_paths.py       Path helpers relative to STATE_DIR
  auth.py              HMAC constant-time token check
  ingest.py            Stream body → temp file → atomic swap
  reconciler.py        Read arb_view, dedup, diff bucket state, publish
  dedup.py             group_id + bookmaker-set key; highest bps wins
  bucket_state.py      In-memory current state; reconcile → BucketDelta list
  broadcaster.py       Per-bucket asyncio.Queue fan-out + ring buffer
  sse.py               stream_events() generator; parse_last_id()
  snapshot.py          SnapshotCache + VACUUM INTO builder
  ops_db.py            ops.db: receive_timings + receive_ema; EMA math
  timing.py            compute_ema() helper
  api/
    health.py          /health
    ingest.py          PUT /ingest/arbitrage
    matches_ingest.py  PUT /ingest/matches
    snapshot.py        GET /snapshot/{bucket}
    matches_snapshot.py GET /snapshot/matches
    stream.py          GET /stream/{bucket}
    matches_stream.py  GET /stream/matches
    arbs.py            GET /arbs/{bucket}, /meta, /buckets, /metrics
tests/
  conftest.py          _isolated_state, build_arbitrage_db, app_with_state
  _schema.py           Re-exports ARBITRAGE_SCHEMA_SQL from app.arbitrage_schema
  test_timing.py       EMA convergence (6 tests)
  test_dedup.py        Dedup key + tiebreak (8 tests)
  test_bucket_state.py Reconcile diffs + bucket migration (9 tests)
  test_broadcaster.py  Ring eviction, gap detection, slow-subscriber drop (10 tests)
  test_reconciler.py   DB-to-broadcast integration (5 tests)
  test_stream.py       SSE generator unit tests (9 tests)
  test_ingest.py       Auth, atomic swap, gzip, size limit (7 tests)
  test_snapshot.py     VACUUM INTO builder + HTTP endpoint (9 tests)
  test_api_misc.py     /meta /buckets /arbs /metrics /health (10 tests)
  test_end_to_end.py   Full push→reconcile→broadcast→snapshot flow (6 tests)
```
