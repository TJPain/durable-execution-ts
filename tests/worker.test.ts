import { describe, it, expect, beforeEach, afterAll } from "vitest";
import sql from "../src/db";
import { enqueue, registerWorker, type Task } from "../src/queue";
import { register, clearHandlers, start, processTask } from "../src/worker";

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
    RETURNING id, name, payload, attempts, max_attempts, timeout_seconds
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
