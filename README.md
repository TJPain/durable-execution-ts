# Durable Execution in TypeScript

Building a durable execution engine from scratch using TypeScript, Node.js and Postgres.

Durable execution is a mechanism to incrementally checkpoint the state of a function as it makes progress, so that in the case of unexpected failure, the function can recover from where it left off. It's particularly relevant in newer stacks and projects implementing AI agents, which are long-running and stateful. A system which implements durable execution is often called a "workflow engine."

Inspired by the Go project [Durable Execution, the Hard Way](https://github.com/hatchet-dev/durable-execution-the-hard-way) but extended with LISTEN/NOTIFY.

## Architecture

```mermaid
graph LR
  Producer -->|enqueue| Queue[(Postgres)]
  Queue -->|dequeue| Worker
  Worker -->|ack/nack| Queue
  Worker -->|heartbeat| Queue
  Sweeper -->|reclaim stuck tasks| Queue
  Sweeper -->|fail expired tasks| Queue
```

### Task lifecycle

```mermaid
stateDiagram-v2
  [*] --> pending
  pending --> running: dequeued by worker
  running --> completed: handler succeeds
  running --> pending: handler fails (retryable, attempts < max)
  running --> failed: handler fails (max attempts or non-retryable)
  running --> pending: worker crashes (reclaimed by sweeper)
  pending --> failed: scheduling timeout exceeded
```

### Components

| Component | File | Responsibility |
|-----------|------|----------------|
| **Queue** | `src/queue.ts` | Enqueue/dequeue tasks, ack/nack, worker registration, sweeper queries |
| **Worker** | `src/worker.ts` | Poll loop, concurrent task dispatch, heartbeat, timeout enforcement |
| **DB** | `src/db.ts` | Postgres connection pool via [Postgres.js](https://github.com/porsager/postgres) |

## Features

### Task queue with concurrent processing

Tasks are stored in Postgres and dequeued atomically using `FOR UPDATE SKIP LOCKED` — multiple workers can run in parallel without ever processing the same task twice. Each worker processes up to N tasks concurrently (configurable).

### Retries with exponential backoff

When a task fails, it's automatically retried up to `max_attempts` times. Each retry waits exponentially longer (1s, 2s, 4s, 8s...) via a `run_after` column that the dequeue query respects. Handlers can throw `NonRetryableError` to fail immediately regardless of remaining attempts.

### Execution timeouts

Each task has a `timeout_seconds` value. The worker enforces this via `Promise.race` — even if the handler ignores the `AbortSignal`, it will be timed out and nacked. This prevents hung tasks from consuming concurrency slots indefinitely.

### Worker heartbeats and stuck task recovery

Workers register themselves in a `workers` table and update `last_heartbeat_at` every few seconds. A sweeper periodically checks for workers that have gone silent (crashed, OOM-killed, network-partitioned) and requeues their running tasks so another worker can pick them up.

### Scheduling timeouts

Tasks can specify a scheduling timeout — if they sit in `pending` for too long without being picked up by any worker, they're failed permanently. Useful for time-sensitive work where a stale result is worse than no result.

### Priority ordering

Tasks have a `priority` field (default 0). Higher priority tasks skip ahead in the queue. Payment processing at priority 10 will always be dequeued before a background report at priority 0, regardless of creation time.

## Prerequisites

- Node.js 22+
- Docker

## Getting started

```bash
# Install dependencies
npm install

# Start Postgres
npm run db:up

# Set the connection string
export DATABASE_URL="postgresql://durable:durable@localhost:5432/durable"

# Apply the schema
npm run db:migrate

# Run the demo
npm start
```

Press Ctrl+C to shut down the worker gracefully.

## Tests

```bash
npm test
```

## Other commands

```bash
npm run db:reset   # Drop and recreate all tables
```
