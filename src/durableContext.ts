import sql from "./db";

export class NonDeterminismError extends Error {
  constructor(taskId: string, eventId: number, expectedLabel: string, actualLabel: string) {
    super(
      `Non-determinism detected in task ${taskId}: step ${eventId} was "${expectedLabel}" in original execution but is now "${actualLabel}"`
    );
    this.name = "NonDeterminismError";
  }
}

/** Thrown when a worker can't acquire the task lock — it has been evicted and another worker owns the task. */
export class DurableTaskEvictedError extends Error {
  constructor(taskId: string) {
    super(`Worker no longer owns task ${taskId}; event write aborted`);
    this.name = "DurableTaskEvictedError";
  }
}

interface EventRow {
  label: string;
  output: unknown;
}

/**
 * Passed to durable task handlers in place of raw payload. Each ctx.run() call
 * is checkpointed — on retry, completed steps return their stored output instead
 * of re-executing. Steps are identified by position; label mismatches between
 * retries throw NonDeterminismError.
 *
 * All prior events are loaded once at construction time. Each new event write
 * acquires a short-lived row-level lock on the task row to guard against a
 * stale/evicted worker writing events after another worker has taken ownership.
 *
 * At-least-once semantics: fn() executes before the checkpoint is committed to
 * the DB. A crash between fn() completing and the INSERT committing will cause
 * fn() to run again on retry. Steps must therefore be idempotent.
 */
export class DurableContext {
  private nextEventId = 0;
  private events: Map<number, EventRow>;

  private constructor(
    private readonly taskId: string,
    private readonly workerId: string,
    events: Map<number, EventRow>,
  ) {
    this.events = events;
  }

  static async create(taskId: string, workerId: string): Promise<DurableContext> {
    const rows = await sql<{ event_id: number; label: string; output: unknown }[]>`
      SELECT event_id, label, output FROM durable_events
      WHERE task_id = ${taskId}
      ORDER BY event_id
    `;
    const events = new Map(rows.map(r => [r.event_id, { label: r.label, output: r.output }]));
    return new DurableContext(taskId, workerId, events);
  }

  async run<T>(label: string, fn: () => Promise<T>): Promise<T> {
    const eventId = this.nextEventId++;
    const existing = this.events.get(eventId);

    if (existing) {
      if (existing.label !== label) {
        throw new NonDeterminismError(this.taskId, eventId, existing.label, label);
      }
      console.log(`  [durable] step ${eventId} (${label}): replaying from log`);
      return existing.output as T;
    }

    const output = await fn();

    await sql.begin(async (tx) => {
      // Acquire a row-level lock on the task. SKIP LOCKED means if another
      // transaction holds the lock (a new worker), we immediately get no rows
      // rather than waiting — we're evicted and must not write.
      const [locked] = await tx`
        SELECT id FROM tasks
        WHERE id = ${this.taskId}
          AND worker_id = ${this.workerId}
          AND status = 'running'
        FOR UPDATE SKIP LOCKED
      `;

      if (!locked) {
        throw new DurableTaskEvictedError(this.taskId);
      }

      await tx`
        INSERT INTO durable_events (task_id, event_id, label, output)
        VALUES (${this.taskId}, ${eventId}, ${label}, ${sql.json(output as any)})
      `;
    });

    console.log(`  [durable] step ${eventId} (${label}): executed and checkpointed`);
    return output;
  }
}
