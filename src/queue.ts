import sql from "./db";
import type { JSONValue } from "postgres";

interface Task {
  id: string;
  name: string;
  payload: Record<string, unknown>;
  attempts: number;
  max_attempts: number;
  timeout_seconds: number;
}

interface EnqueueOptions {
  maxAttempts?: number;
  timeoutSeconds?: number;
}

/** Throw this from a handler to fail the task permanently, bypassing retry logic. */
export class NonRetryableError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "NonRetryableError";
  }
}

export async function enqueue(
  name: string,
  payload: JSONValue = {},
  options: EnqueueOptions = {}
): Promise<string> {
  const { maxAttempts = 3, timeoutSeconds = 60 } = options;

  const [task] = await sql`
    INSERT INTO tasks (name, payload, max_attempts, timeout_seconds)
    VALUES (${name}, ${sql.json(payload)}, ${maxAttempts}, ${timeoutSeconds})
    RETURNING id
  `;
  return task.id;
}

/**
 * Atomically claims the next available task. Uses FOR UPDATE SKIP LOCKED
 * so concurrent workers never receive the same task. Respects run_after
 * for backoff timing.
 */
export async function dequeue(): Promise<Task | null> {
  const [task] = await sql<Task[]>`
    UPDATE tasks
    SET status = 'running', started_at = now(), attempts = attempts + 1
    WHERE id = (
      SELECT id FROM tasks
      WHERE status = 'pending'
        AND run_after <= now()
      ORDER BY created_at
      FOR UPDATE SKIP LOCKED
      LIMIT 1
    )
    RETURNING id, name, payload, attempts, max_attempts, timeout_seconds
  `;
  return task ?? null;
}

export async function ack(id: string): Promise<void> {
  await sql`
    UPDATE tasks
    SET status = 'completed', completed_at = now()
    WHERE id = ${id}
  `;
}

/**
 * Marks a task as failed. If retryable and under max_attempts, requeues with
 * exponential backoff (2^n seconds). Otherwise fails permanently.
 */
export async function nack(id: string, error: Error): Promise<void> {
  const isRetryable = !(error instanceof NonRetryableError);

  const [task] = await sql`SELECT attempts, max_attempts FROM tasks WHERE id = ${id}`;

  if (isRetryable && task.attempts < task.max_attempts) {
    const backoffSeconds = Math.pow(2, task.attempts - 1);

    await sql`
      UPDATE tasks
      SET status = 'pending',
          error = ${error.message},
          run_after = now() + ${backoffSeconds + " seconds"}::interval
      WHERE id = ${id}
    `;
  } else {
    await sql`
      UPDATE tasks
      SET status = 'failed', completed_at = now(), error = ${error.message}
      WHERE id = ${id}
    `;
  }
}
