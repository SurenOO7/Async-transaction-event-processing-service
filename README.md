# Async transaction-event processing service

Ingests transaction events over HTTP, processes them asynchronously (currency
conversion + dedup + durable storage), and serves per-user reads. Built around
**Redis Streams** for at-least-once delivery, a **separate retry worker + dead-letter
queue** for failure handling, and **indexed Postgres** for reads.

Five processes (see `docker-compose.yml`): **postgres**, **redis**, a one-shot
**migrate**, the **api**, the **consumer**, and the **retry-worker**.

## Run it

```bash
docker compose up --build        # postgres, redis, migrate, api, consumer, retry-worker
```

Migrations apply automatically (the `migrate` service runs `alembic upgrade head`
before api/consumer start). The API is on `http://localhost:8000`.

```bash
# enqueue an event (currency normalized, amount validated) -> 202
curl -X POST localhost:8000/transactions -H 'Content-Type: application/json' \
  -d '{"id":"evt-1","user_id":"alice","amount":"100.00","currency":"eur","timestamp":"2026-06-20T12:00:00Z"}'

curl localhost:8000/users/alice/summary
curl "localhost:8000/users/alice/transactions?from=2026-06-01T00:00:00Z&limit=50"
curl localhost:9100/metrics            # consumer metrics (prometheus)
```

FX rates are fetched once per currency and cached in Redis (`rate:{CUR}:USD`,
300s TTL). For a fully offline demo, seed a rate:
`docker compose exec redis redis-cli set rate:EUR:USD 1.10`.

## Tests

```bash
python -m venv .venv && . .venv/bin/activate
pip install -r requirements.txt
python -m pytest        # 59 tests; unit-level (fakes/sqlite), no infra needed
```

## Design decisions

### Why Redis Streams (the queue)
Streams give **at-least-once delivery** with consumer groups: a delivered message
sits in the consumer's Pending Entries List (PEL) until `XACK`. If a worker
crashes mid-process, the message isn't lost — it stays in the PEL and is
redelivered (recoverable with `XAUTOCLAIM`). Plain pub/sub would drop messages
with no consumer; a list (`LPUSH`/`BRPOP`) has no per-consumer ack/redelivery.
Streams are the lightest thing that gives durable, replayable, ack-based
delivery without standing up Kafka.

### Delivery guarantee: at-least-once + dedup ⇒ effectively exactly-once storage
- The stream delivers **at least once** (crashes cause redelivery).
- Storage is **idempotent on `id`**: a Postgres primary key plus an
  `IntegrityError` catch. A cheap read-check skips the insert in the common case;
  the PK constraint is the real guarantee — under concurrent workers both
  read-checks can miss, and the second insert simply hits the constraint and is
  treated as a duplicate.
- Net effect: a transaction is **stored exactly once** even though it may be
  *delivered* and *processed* more than once.

**Where could an event be lost?** Only at the `XACK`. We ack **only after** a
successful store, so a crash before the ack means redelivery (safe), never loss.
On a processing failure we enqueue to the retry queue **before** acking, so the
event is always in *either* the stream PEL *or* the retry ZSET — never gone from
both.

### Retries off the hot path + dead-letter queue
Failures (DB down, FX lookup down) don't block the consumer. The consumer tries
**once**, then hands the event to a Redis **ZSET scored by due-time**. A separate
**retry worker** pops only due events, reprocesses with **capped exponential
backoff** (1, 2, 4, 8 … capped), and after `MAX_ATTEMPTS` moves the event to a
**dead-letter stream** — so a poison event stops looping instead of churning
forever. The retry worker claims each event with `ZREM` before processing, so it
is safe to run multiple replicas.

### Reads: indexed Postgres, no read cache
`/summary` and `/transactions` query Postgres on every call, backed by a
composite **`(user_id, timestamp)`** index. At this scale that index — not a
cache — is what keeps reads fast, and it avoids cache staleness/invalidation.
Read logic lives behind service functions, so a cache can be slotted in later.

### Money
`Decimal` end-to-end, stored as `Numeric(18,8)` — no binary-float rounding error
across millions of rows. Rates are parsed via `Decimal(str(x))` to avoid float
contamination.

## Trade-off made
Compute-on-read for `/summary` (a `SUM`/`COUNT` per request) instead of
maintaining a running total. Simpler and always correct; costs a indexed
aggregate per call. Fine at ~100 events/sec; see below for when it changes.

## What I'd change at 10x load
- **Write-through summary**: maintain a per-user running total in Redis, updated
  by the consumer — O(1) reads, always fresh (replaces compute-on-read).
- **Batch inserts** in the consumer (`XREADGROUP` already returns batches).
- **Partition** the transactions table by time; archive cold partitions.
- Scale consumers/retry-workers horizontally (consumer groups + `ZREM` claim
  already support this); shard Redis if the stream is the bottleneck.

## Not built (deliberately)
API-key auth, CORS lockdown, and a token-bucket rate limiter are scoped but left
out to keep the core literal to the task. They're easy to add behind a FastAPI
dependency.
