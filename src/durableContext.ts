import sql from "./db";

export class NonDeterminismError extends Error {
  constructor(taskId: string, eventId: number) {
    super(`Non-determinism detected in task ${taskId}: step ${eventId} was not present in original execution`);
    this.name = "NonDeterminismError";
  }
}

/**
 * Passed to durable task handlers in place of raw payload. Each ctx.run() call
 * is checkpointed — on retry, completed steps return their stored output instead
 * of re-executing. Steps are identified by position, not name.
 */
export class DurableContext {
  private nextEventId = 0;

  constructor(private readonly taskId: string) {}

  /**
   * Executes fn and persists its return value. On replay, returns the stored
   * output without calling fn. Resumes from the first un-checkpointed step,
   * so partial completions pick up exactly where they left off.
   */
  async run<T>(label: string, fn: () => Promise<T>): Promise<T> {
    const eventId = this.nextEventId++;

    const [existing] = await sql`
      SELECT output FROM durable_events
      WHERE task_id = ${this.taskId} AND event_id = ${eventId}
    `;

    if (existing) {
      console.log(`  [durable] step ${eventId} (${label}): replaying from log`);
      return existing.output as T;
    }

    const output = await fn();

    await sql`
      INSERT INTO durable_events (task_id, event_id, output)
      VALUES (${this.taskId}, ${eventId}, ${sql.json(output as any)})
    `;

    console.log(`  [durable] step ${eventId} (${label}): executed and checkpointed`);
    return output;
  }
}

