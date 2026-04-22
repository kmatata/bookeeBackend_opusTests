# bookeeBackend — developer notes

## What this is

FastAPI backend that receives `arbitrage.db` SQLite pushes from the ETL
container (`bookeeETL_opusTests`), deduplicates and bucket-partitions the
opportunities, then fans out per-bucket SSE deltas to browser clients.
Clients boot via `GET /snapshot/{bucket}` then follow `GET /stream/{bucket}`.

## Why it only runs inside Docker

`STATE_DIR` (default `/state`) is an **absolute path mounted as a named
Docker volume**. The app reads and writes:

| Path | Purpose |
|------|---------|
| `/state/arbitrage.db` | Latest SQLite pushed by ETL |
| `/state/arbitrage.db.incoming` | Temp target during upload (renamed atomically) |
| `/state/ops.db` | Receive timings + EMA state (persists across restarts) |
| `/state/snapshots/` | Cached per-bucket snapshot files |

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

## Running the app (Docker)

```bash
# build
docker build -t bookee-backend .

# run (state volume persists ops.db + last received arbitrage.db)
docker run --rm -p 8000:8000 \
  -v bookee_state:/state \
  -e INGEST_TOKEN=dev-secret \
  bookee-backend

# or with full env override
docker run --rm -p 8000:8000 \
  -v bookee_state:/state \
  --env-file .env \
  bookee-backend
```

The `.env` file is also baked into the image at build time (see `Dockerfile`),
so variables in `.env` are available without `--env-file` when the file is
present at build time. Override with `-e` flags at runtime.

## Simulating an ETL push (curl)

```bash
# push a real arbitrage.db from the ETL repo
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

# JSON contents of a bucket (debug)
curl -s http://localhost:8000/arbs/low | jq '.[0]'

# SQLite snapshot (decompress and open)
curl -so low.db --compressed http://localhost:8000/snapshot/low
sqlite3 low.db 'SELECT COUNT(*) FROM arb_opportunities;'

# SSE stream (Ctrl-C to stop)
curl -N http://localhost:8000/stream/low
```

## Key design decisions

### Atomic ingest swap
Body is streamed to `arbitrage.db.incoming`, fsync'd, then
`os.replace()`'d to `arbitrage.db`. A single `asyncio.Lock` serialises
concurrent pushes so the reconciler always sees a complete file.

### Dedup key
`(group_id, sorted(bookmakers_across_all_legs))` — when the ETL produces
two rows for the same fixture on the same bookmaker set, only the highest
`profit_margin_bps` row is forwarded to clients.

### 409 on duplicate scanner run-id
`X-Scanner-Run-Id` is a monotone push counter. If a retry arrives with
`run_id ≤ last_applied_run_id`, the backend returns 409 immediately
(inside the asyncio.Lock) without touching the filesystem.

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
    snapshot.py        GET /snapshot/{bucket}
    stream.py          GET /stream/{bucket}
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
