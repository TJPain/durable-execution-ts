import { describe, it, expect, beforeEach, afterAll } from "vitest";
import sql from "../src/db";
import { enqueue, registerWorker, type Task } from "../src/queue";
import { register, registerDurable, clearHandlers, start, processTask } from "../src/worker";
import type { DurableContext } from "../src/durableContext";

let workerId: string;

beforeEach(async () => {
  await sql`TRUNCATE tasks, workers CASCADE`;
  clearHandlers();
  workerId = await registerWorker("test-worker");
});

afterAll(async () => {
  await sql.end();
});

async function dequeueRaw(id: string): Promise<Task> {
  const [task] = await sql<Task[]>`
    UPDATE tasks SET status = 'running', attempts = attempts + 1, worker_id = ${workerId}
    WHERE id = ${id}
    RETURNING id, name, payload, attempts, max_attempts, timeout_seconds, is_durable
  `;
  return task;
}

describe("processTask", () => {
  it("calls the handler with the task payload and acks", async () => {
    let received: object | null = null;
    register("test-task", async (payload) => {
      received = payload;
    });

    const id = await enqueue("test-task", { key: "value" });
    const task = await dequeueRaw(id);

    await processTask(task, workerId);

    expect(received).toEqual({ key: "value" });
    const [row] = await sql`SELECT status FROM tasks WHERE id = ${id}`;
    expect(row.status).toBe("completed");
  });

  it("nacks when the handler throws", async () => {
    register("failing-task", async () => {
      throw new Error("boom");
    });

    const id = await enqueue("failing-task", {}, { maxAttempts: 1 });
    const task = await dequeueRaw(id);

    await processTask(task, workerId);

    const [row] = await sql`SELECT status, error FROM tasks WHERE id = ${id}`;
    expect(row.status).toBe("failed");
    expect(row.error).toBe("boom");
  });

  it("retries when handler throws and attempts remain", async () => {
    register("retryable-task", async () => {
      throw new Error("transient");
    });

    const id = await enqueue("retryable-task", {}, { maxAttempts: 3 });
    const task = await dequeueRaw(id);

    await processTask(task, workerId);

    const [row] = await sql`SELECT status, error FROM tasks WHERE id = ${id}`;
    expect(row.status).toBe("pending");
    expect(row.error).toBe("transient");
  });

  it("nacks when no handler is registered", async () => {
    const id = await enqueue("unknown-task", {}, { maxAttempts: 1 });
    const task = await dequeueRaw(id);

    await processTask(task, workerId);

    const [row] = await sql`SELECT status FROM tasks WHERE id = ${id}`;
    expect(row.status).toBe("failed");
  });

  it("times out if handler exceeds timeout_seconds", async () => {
    register("slow-task", async (_payload, signal) => {
      await new Promise((resolve, reject) => {
        const timer = setTimeout(resolve, 5000);
        signal.addEventListener("abort", () => {
          clearTimeout(timer);
          reject(signal.reason);
        });
      });
    });

    const id = await enqueue("slow-task", {}, { maxAttempts: 1, timeoutSeconds: 1 });
    const task = await dequeueRaw(id);

    await processTask(task, workerId);

    const [row] = await sql`SELECT status FROM tasks WHERE id = ${id}`;
    expect(row.status).toBe("failed");
  }, 3000);

  it("times out even if handler ignores the signal", async () => {
    register("ignores-signal", async () => {
      await new Promise((resolve) => setTimeout(resolve, 5000));
    });

    const id = await enqueue("ignores-signal", {}, { maxAttempts: 1, timeoutSeconds: 1 });
    const task = await dequeueRaw(id);

    await processTask(task, workerId);

    const [row] = await sql`SELECT status FROM tasks WHERE id = ${id}`;
    expect(row.status).toBe("failed");
  }, 3000);
});

describe("processTask (durable)", () => {
  it("runs a durable task and checkpoints each step", async () => {
    const steps: string[] = [];

    registerDurable("durable-task", async (ctx: DurableContext) => {
      await ctx.run("step-1", async () => { steps.push("step-1"); return "a"; });
      await ctx.run("step-2", async () => { steps.push("step-2"); return "b"; });
    });

    const id = await enqueue("durable-task", {}, { isDurable: true });
    const task = await dequeueRaw(id);
    await processTask(task, workerId);

    expect(steps).toEqual(["step-1", "step-2"]);

    const events = await sql`SELECT event_id, output FROM durable_events WHERE task_id = ${id} ORDER BY event_id`;
    expect(events).toHaveLength(2);
    expect(events[0].output).toBe("a");
    expect(events[1].output).toBe("b");

    const [row] = await sql`SELECT status FROM tasks WHERE id = ${id}`;
    expect(row.status).toBe("completed");
  });

  it("skips already-checkpointed steps on retry", async () => {
    const steps: string[] = [];

    registerDurable("durable-task", async (ctx: DurableContext) => {
      await ctx.run("step-1", async () => { steps.push("step-1"); return "a"; });
      await ctx.run("step-2", async () => { steps.push("step-2"); return "b"; });
    });

    const id = await enqueue("durable-task", {}, { isDurable: true });

    // Simulate step-1 having been checkpointed on a prior attempt
    await sql`INSERT INTO durable_events (task_id, event_id, label, output) VALUES (${id}, 0, 'step-1', ${sql.json("a")})`;

    const task = await dequeueRaw(id);
    await processTask(task, workerId);

    // Only step-2 should have actually executed
    expect(steps).toEqual(["step-2"]);
  });

  it("does not nack when evicted — task stays running for the new owner", async () => {
    const otherWorkerId = await registerWorker("other-worker");

    registerDurable("durable-task", async (ctx: DurableContext) => {
      // Simulate eviction mid-handler: reassign the task to another worker
      await sql`UPDATE tasks SET worker_id = ${otherWorkerId} WHERE id = ${ctx["taskId"]}`;
      await ctx.run("step-1", async () => "x");
    });

    const id = await enqueue("durable-task", {}, { isDurable: true });
    const task = await dequeueRaw(id);
    await processTask(task, workerId);

    // Task should still be running (owned by the other worker), not failed/pending
    const [row] = await sql`SELECT status, worker_id FROM tasks WHERE id = ${id}`;
    expect(row.status).toBe("running");
    expect(row.worker_id).toBe(otherWorkerId);
  });
});

describe("start", () => {
  it("processes tasks concurrently up to the limit", async () => {
    let concurrent = 0;
    let maxConcurrent = 0;

    register("concurrent-task", async () => {
      concurrent++;
      maxConcurrent = Math.max(maxConcurrent, concurrent);
      await new Promise((resolve) => setTimeout(resolve, 100));
      concurrent--;
    });

    await enqueue("concurrent-task", {});
    await enqueue("concurrent-task", {});
    await enqueue("concurrent-task", {});
    await enqueue("concurrent-task", {});
    await enqueue("concurrent-task", {});

    const controller = new AbortController();
    setTimeout(() => controller.abort(), 500);

    await start({ concurrency: 3, pollIntervalMs: 50, signal: controller.signal });

    expect(maxConcurrent).toBe(3);
  });

  it("stops when signal is aborted", async () => {
    const controller = new AbortController();
    controller.abort();

    const before = Date.now();
    await start({ signal: controller.signal });
    const elapsed = Date.now() - before;

    expect(elapsed).toBeLessThan(100);
  });

  it("retries a failing task and eventually succeeds", async () => {
    let attempts = 0;

    register("eventually-succeeds", async () => {
      attempts++;
      if (attempts < 3) {
        throw new Error("not yet");
      }
    });

    await enqueue("eventually-succeeds", {}, { maxAttempts: 3 });

    const resetInterval = setInterval(async () => {
      await sql`UPDATE tasks SET run_after = now() WHERE status = 'pending'`;
    }, 50);

    const controller = new AbortController();
    setTimeout(() => controller.abort(), 3000);

    await start({ concurrency: 1, pollIntervalMs: 50, signal: controller.signal });

    clearInterval(resetInterval);

    const [row] = await sql`SELECT status, attempts FROM tasks`;
    expect(row.status).toBe("completed");
    expect(row.attempts).toBe(3);
  }, 5000);
});
