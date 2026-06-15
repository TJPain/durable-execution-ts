import sql from "./db";
import type { JSONValue } from "postgres";

export interface Task {
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
  priority?: number;
  scheduleTimeoutSeconds?: number;
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
  const { maxAttempts = 3, timeoutSeconds = 60, priority = 0, scheduleTimeoutSeconds } = options;

  const scheduleTimeoutAt = scheduleTimeoutSeconds != null
    ? sql`now() + ${scheduleTimeoutSeconds + " seconds"}::interval`
    : sql`NULL`;

  const [task] = await sql`
    INSERT INTO tasks (name, payload, max_attempts, timeout_seconds, priority, schedule_timeout_at)
    VALUES (${name}, ${sql.json(payload)}, ${maxAttempts}, ${timeoutSeconds}, ${priority}, ${scheduleTimeoutAt})
    RETURNING id
  `;
  return task.id;
}

/**
 * Atomically claims the next available task. Uses FOR UPDATE SKIP LOCKED
 * so concurrent workers never receive the same task. Orders by priority
 * (highest first), then created_at. Respects run_after for backoff timing.
 */
export async function dequeue(workerId: string): Promise<Task | null> {
  const [task] = await sql<Task[]>`
    UPDATE tasks
    SET status = 'running', started_at = now(), attempts = attempts + 1, worker_id = ${workerId}
    WHERE id = (
      SELECT id FROM tasks
      WHERE status = 'pending'
        AND run_after <= now()
      ORDER BY priority DESC, created_at
      FOR UPDATE SKIP LOCKED
      LIMIT 1
    )
    RETURNING id, name, payload, attempts, max_attempts, timeout_seconds
  `;
  return task ?? null;
}

/** Only acks if this worker still owns the task. */
export async function ack(id: string, workerId: string): Promise<void> {
  await sql`
    UPDATE tasks
    SET status = 'completed', completed_at = now(), worker_id = NULL
    WHERE id = ${id} AND status = 'running' AND worker_id = ${workerId}
  `;
}

/**
 * Marks a task as failed. If retryable and under max_attempts, requeues with
 * exponential backoff (2^n seconds). Otherwise fails permanently.
 * Only updates if this worker still owns the task.
 */
export async function nack(id: string, workerId: string, task: Pick<Task, "attempts" | "max_attempts">, error: Error): Promise<void> {
  const isRetryable = !(error instanceof NonRetryableError);

  if (isRetryable && task.attempts < task.max_attempts) {
    const backoffSeconds = Math.pow(2, task.attempts - 1);

    await sql`
      UPDATE tasks
      SET status = 'pending',
          error = ${error.message},
          worker_id = NULL,
          run_after = now() + ${backoffSeconds + " seconds"}::interval
      WHERE id = ${id} AND status = 'running' AND worker_id = ${workerId}
    `;
  } else {
    await sql`
      UPDATE tasks
      SET status = 'failed', completed_at = now(), worker_id = NULL, error = ${error.message}
      WHERE id = ${id} AND status = 'running' AND worker_id = ${workerId}
    `;
  }
}

/** Registers a worker and returns its ID. */
export async function registerWorker(name: string): Promise<string> {
  const [worker] = await sql`
    INSERT INTO workers (name)
    VALUES (${name})
    RETURNING id
  `;
  return worker.id;
}

/** Updates the worker's heartbeat timestamp. */
export async function heartbeat(workerId: string): Promise<void> {
  await sql`
    UPDATE workers SET last_heartbeat_at = now()
    WHERE id = ${workerId}
  `;
}

/** Removes a worker record on graceful shutdown. */
export async function deregisterWorker(workerId: string): Promise<void> {
  await sql`DELETE FROM workers WHERE id = ${workerId}`;
}

/**
 * Finds tasks stuck on stale workers (no heartbeat for inactivitySeconds)
 * and requeues them. Decrements attempts so a crash doesn't consume a retry
 * slot. Clears schedule_timeout_at to prevent false timeout after reclaim.
 */
export async function reclaimStaleTasks(inactivitySeconds: number): Promise<number> {
  const result = await sql`
    UPDATE tasks
    SET status = 'pending',
        worker_id = NULL,
        started_at = NULL,
        attempts = GREATEST(attempts - 1, 0),
        schedule_timeout_at = NULL
    WHERE id IN (
      SELECT t.id
      FROM tasks t
      JOIN workers w ON t.worker_id = w.id
      WHERE t.status = 'running'
        AND w.last_heartbeat_at < now() - ${inactivitySeconds + " seconds"}::interval
      FOR UPDATE SKIP LOCKED
    )
  `;
  return result.count;
}

/**
 * Fails pending tasks that have exceeded their schedule_timeout_at.
 * Returns the number of timed-out tasks.
 */
export async function failScheduleTimeouts(): Promise<number> {
  const result = await sql`
    UPDATE tasks
    SET status = 'failed',
        completed_at = now(),
        error = 'task was not picked up within the scheduling timeout'
    WHERE id IN (
      SELECT id FROM tasks
      WHERE status = 'pending'
        AND schedule_timeout_at IS NOT NULL
        AND schedule_timeout_at < now()
      FOR UPDATE SKIP LOCKED
    )
  `;
  return result.count;
}
