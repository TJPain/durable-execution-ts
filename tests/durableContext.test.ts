import { describe, it, expect, beforeEach, afterAll } from "vitest";
import sql from "../src/db";
import { enqueue, registerWorker } from "../src/queue";
import { DurableContext, NonDeterminismError } from "../src/durableContext";

let workerId: string;
let taskId: string;

beforeEach(async () => {
  await sql`TRUNCATE tasks, workers CASCADE`;
  workerId = await registerWorker("test-worker");
  taskId = await enqueue("durable-task", {}, { isDurable: true });
  await sql`UPDATE tasks SET status = 'running', worker_id = ${workerId} WHERE id = ${taskId}`;
});

afterAll(async () => {
  await sql.end();
});

describe("DurableContext.run", () => {
  it("executes fn, persists label and output on first run", async () => {
    const ctx = await DurableContext.create(taskId);
    const result = await ctx.run("step-1", async () => 42);

    expect(result).toBe(42);

    const [row] = await sql`SELECT label, output FROM durable_events WHERE task_id = ${taskId} AND event_id = 0`;
    expect(row.label).toBe("step-1");
    expect(row.output).toBe(42);
  });

  it("returns stored output without calling fn on replay", async () => {
    await sql`INSERT INTO durable_events (task_id, event_id, label, output) VALUES (${taskId}, 0, 'step-1', ${sql.json("cached")})`;

    let called = false;
    const ctx = await DurableContext.create(taskId);
    const result = await ctx.run("step-1", async () => {
      called = true;
      return "fresh";
    });

    expect(result).toBe("cached");
    expect(called).toBe(false);
  });

  it("executes subsequent steps after a partially completed run", async () => {
    await sql`INSERT INTO durable_events (task_id, event_id, label, output) VALUES (${taskId}, 0, 'step-0', ${sql.json("step-0-output")})`;

    const ctx = await DurableContext.create(taskId);
    const step0 = await ctx.run("step-0", async () => "should not run");
    const step1 = await ctx.run("step-1", async () => "new work");

    expect(step0).toBe("step-0-output");
    expect(step1).toBe("new work");

    const [row] = await sql`SELECT label, output FROM durable_events WHERE task_id = ${taskId} AND event_id = 1`;
    expect(row.label).toBe("step-1");
    expect(row.output).toBe("new work");
  });

  it("throws NonDeterminismError when label at a position changes between retries", async () => {
    await sql`INSERT INTO durable_events (task_id, event_id, label, output) VALUES (${taskId}, 0, 'original-label', ${sql.json("x")})`;

    const ctx = await DurableContext.create(taskId);
    await expect(ctx.run("different-label", async () => "y")).rejects.toThrow(NonDeterminismError);
  });

  it("persists complex object output", async () => {
    const ctx = await DurableContext.create(taskId);
    const result = await ctx.run("step-1", async () => ({ userId: "abc", score: 99 }));

    expect(result).toEqual({ userId: "abc", score: 99 });

    const [row] = await sql`SELECT output FROM durable_events WHERE task_id = ${taskId} AND event_id = 0`;
    expect(row.output).toEqual({ userId: "abc", score: 99 });
  });
});
