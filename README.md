# Durable Execution in TypeScript

Building a durable execution engine from scratch using TypeScript, Node.js and Postgres.

Durable execution is a mechanism to incrementally checkpoint the state of a function as it makes progress, so that in the case of unexpected failure, the function can recover from where it left off. It's particularly relevant in newer stacks and projects implementing AI agents, which are long-running and stateful. A system which implements durable execution is often called a "workflow engine."

Inspired by the Go project [Durable Execution, the Hard Way](https://github.com/hatchet-dev/durable-execution-the-hard-way) but extended with LISTEN/NOTIFY.

## Architecture

- **Queue** (`src/queue.ts`) — Enqueue/dequeue tasks backed by Postgres `FOR UPDATE SKIP LOCKED` for safe concurrent access. Retries with exponential backoff, `NonRetryableError` for permanent failures. Priority ordering.
- **Worker** (`src/worker.ts`) — Polls the queue, dispatches tasks to registered handlers with configurable concurrency. Per-task execution timeouts via `AbortSignal`. Heartbeat-based health tracking with automatic reclaim of stuck tasks from crashed workers. Scheduling timeout sweeper for tasks that sit in the queue too long.
- **DB** (`src/db.ts`) — Postgres connection pool via [Postgres.js](https://github.com/porsager/postgres)

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
npm run db:reset   # Drop and recreate the tasks table
```
