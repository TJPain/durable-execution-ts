# Durable Execution in TypeScript

Building a durable execution engine from scratch using TypeScript, Node.js and Postgres.

Durable execution is a mechanism to incrementally checkpoint the state of a function as it makes progress, so that in the case of unexpected failure, the function can recover from where it left off. It's particularly relevant in newer stacks and projects implementing AI agents, which are long-running and stateful. A system which implements durable execution is often called a "workflow engine."

Inspired by the Go project [Durable Execution, the Hard Way](https://github.com/hatchet-dev/durable-execution-the-hard-way) but extended with LISTEN/NOTIFY.

## Prerequisites

- Node.js 22+
- Docker

## Getting started

```bash
# Install dependencies
npm install

# Start Postgres
docker compose up -d

# Set the connection string
export DATABASE_URL="postgresql://durable:durable@localhost:5432/durable"

# Apply the schema
psql $DATABASE_URL -f sql/schema.sql

# Run the demo
npx tsx src/main.ts
```

Press Ctrl+C to shut down the worker gracefully.
