import { describe, it, expect, beforeEach, afterAll } from "vitest";
import sql from "../src/db";
import { enqueue } from "../src/queue";
import { register, start, processTask } from "../src/worker";

beforeEach(async () => {
  await sql`TRUNCATE tasks`;
});

afterAll(async () => {
  await sql.end();
});

describe("processTask", () => {
  it("calls the handler with the task payload and acks", async () => {
    let received: object | null = null;
    register("test-task", async (payload) => {
      received = payload;
    });

    const id = await enqueue("test-task", { key: "value" });
    const [task] = await sql`
      UPDATE tasks SET status = 'running' WHERE id = ${id}
      RETURNING id, name, payload
    `;

    await processTask(task as { id: string; name: string; payload: object });

    expect(received).toEqual({ key: "value" });
    const [row] = await sql`SELECT status FROM tasks WHERE id = ${id}`;
    expect(row.status).toBe("completed");
  });

  it("nacks when the handler throws", async () => {
    register("failing-task", async () => {
      throw new Error("boom");
    });

    const id = await enqueue("failing-task", {});
    const [task] = await sql`
      UPDATE tasks SET status = 'running' WHERE id = ${id}
      RETURNING id, name, payload
    `;

    await processTask(task as { id: string; name: string; payload: object });

    const [row] = await sql`SELECT status FROM tasks WHERE id = ${id}`;
    expect(row.status).toBe("failed");
  });

  it("nacks when no handler is registered", async () => {
    const id = await enqueue("unknown-task", {});
    const [task] = await sql`
      UPDATE tasks SET status = 'running' WHERE id = ${id}
      RETURNING id, name, payload
    `;

    await processTask(task as { id: string; name: string; payload: object });

    const [row] = await sql`SELECT status FROM tasks WHERE id = ${id}`;
    expect(row.status).toBe("failed");
  });
});

describe("start", () => {
  it("processes tasks concurrently up to the limit", async () => {
    let concurrent = 0;
    let maxConcurrent = 0;

    register("slow-task", async () => {
      concurrent++;
      maxConcurrent = Math.max(maxConcurrent, concurrent);
      await new Promise((resolve) => setTimeout(resolve, 100));
      concurrent--;
    });

    await enqueue("slow-task", {});
    await enqueue("slow-task", {});
    await enqueue("slow-task", {});
    await enqueue("slow-task", {});
    await enqueue("slow-task", {});

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
});
