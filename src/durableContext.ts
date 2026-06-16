import sql from "./db";

export class NonDeterminismError extends Error {
  constructor(taskId: string, eventId: number, expectedLabel: string, actualLabel: string) {
    super(
      `Non-determinism detected in task ${taskId}: step ${eventId} was "${expectedLabel}" in original execution but is now "${actualLabel}"`
    );
    this.name = "NonDeterminismError";
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
 * All prior events are loaded once at construction time to avoid N serial
 * round-trips on replay.
 */
export class DurableContext {
  private nextEventId = 0;
  private events: Map<number, EventRow>;

  private constructor(
    private readonly taskId: string,
    events: Map<number, EventRow>,
  ) {
    this.events = events;
  }

  static async create(taskId: string): Promise<DurableContext> {
    const rows = await sql<{ event_id: number; label: string; output: unknown }[]>`
      SELECT event_id, label, output FROM durable_events
      WHERE task_id = ${taskId}
      ORDER BY event_id
    `;
    const events = new Map(rows.map(r => [r.event_id, { label: r.label, output: r.output }]));
    return new DurableContext(taskId, events);
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

    await sql`
      INSERT INTO durable_events (task_id, event_id, label, output)
      VALUES (${this.taskId}, ${eventId}, ${label}, ${sql.json(output as any)})
    `;

    console.log(`  [durable] step ${eventId} (${label}): executed and checkpointed`);
    return output;
  }
}
