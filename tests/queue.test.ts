import { describe, it, expect, beforeEach, afterAll } from "vitest";
import sql from "../src/db";
import { enqueue, dequeue, ack, nack, NonRetryableError } from "../src/queue";

beforeEach(async () => {
  await sql`TRUNCATE tasks`;
});

afterAll(async () => {
  await sql.end();
});

describe("enqueue", () => {
  it("inserts a task with pending status", async () => {
    const id = await enqueue("send-email", { to: "user@example.com" });

    const [task] = await sql`SELECT * FROM tasks WHERE id = ${id}`;
    expect(task.name).toBe("send-email");
    expect(task.payload).toEqual({ to: "user@example.com" });
    expect(task.status).toBe("pending");
  });

  it("accepts custom max_attempts and timeout", async () => {
    const id = await enqueue("task", {}, { maxAttempts: 5, timeoutSeconds: 120 });

    const [task] = await sql`SELECT max_attempts, timeout_seconds FROM tasks WHERE id = ${id}`;
    expect(task.max_attempts).toBe(5);
    expect(task.timeout_seconds).toBe(120);
  });
});

describe("dequeue", () => {
  it("picks up the oldest pending task and sets it to running", async () => {
    await enqueue("first", {});
    await enqueue("second", {});

    const task = await dequeue();
    expect(task?.name).toBe("first");

    const [row] = await sql`SELECT status, attempts FROM tasks WHERE id = ${task!.id}`;
    expect(row.status).toBe("running");
    expect(row.attempts).toBe(1);
  });

  it("returns null when queue is empty", async () => {
    const task = await dequeue();
    expect(task).toBeNull();
  });

  it("does not return tasks that are already running", async () => {
    await enqueue("task", {});
    await dequeue();

    const second = await dequeue();
    expect(second).toBeNull();
  });

  it("does not return tasks whose run_after is in the future", async () => {
    await sql`
      INSERT INTO tasks (name, payload, run_after)
      VALUES ('future-task', '{}', now() + interval '1 hour')
    `;

    const task = await dequeue();
    expect(task).toBeNull();
  });
});

describe("ack", () => {
  it("marks a task as completed", async () => {
    await enqueue("task", {});
    const task = await dequeue();
    await ack(task!.id);

    const [row] = await sql`SELECT status, completed_at FROM tasks WHERE id = ${task!.id}`;
    expect(row.status).toBe("completed");
    expect(row.completed_at).not.toBeNull();
  });
});

describe("nack", () => {
  it("retries if under max attempts", async () => {
    await enqueue("task", {}, { maxAttempts: 3 });
    const task = await dequeue();
    await nack(task!.id, new Error("transient failure"));

    const [row] = await sql`SELECT status, error, run_after FROM tasks WHERE id = ${task!.id}`;
    expect(row.status).toBe("pending");
    expect(row.error).toBe("transient failure");
    expect(new Date(row.run_after).getTime()).toBeGreaterThan(Date.now());
  });

  it("fails permanently at max attempts", async () => {
    await enqueue("task", {}, { maxAttempts: 1 });
    const task = await dequeue();
    await nack(task!.id, new Error("fatal"));

    const [row] = await sql`SELECT status, error FROM tasks WHERE id = ${task!.id}`;
    expect(row.status).toBe("failed");
    expect(row.error).toBe("fatal");
  });

  it("fails permanently for NonRetryableError regardless of attempts", async () => {
    await enqueue("task", {}, { maxAttempts: 5 });
    const task = await dequeue();
    await nack(task!.id, new NonRetryableError("bad input"));

    const [row] = await sql`SELECT status, error FROM tasks WHERE id = ${task!.id}`;
    expect(row.status).toBe("failed");
    expect(row.error).toBe("bad input");
  });

  it("applies exponential backoff on successive retries", async () => {
    await enqueue("task", {}, { maxAttempts: 4 });

    // First attempt fails
    const first = await dequeue();
    await nack(first!.id, new Error("fail"));
    const [after1] = await sql`SELECT run_after FROM tasks WHERE id = ${first!.id}`;

    // Manually make it dequeueable again to check second backoff
    await sql`UPDATE tasks SET run_after = now() WHERE id = ${first!.id}`;
    const second = await dequeue();
    await nack(second!.id, new Error("fail"));
    const [after2] = await sql`SELECT run_after FROM tasks WHERE id = ${second!.id}`;

    // Second backoff should be longer than first (2^1 > 2^0)
    const backoff1 = new Date(after1.run_after).getTime() - Date.now();
    const backoff2 = new Date(after2.run_after).getTime() - Date.now();
    expect(backoff2).toBeGreaterThan(backoff1);
  });
});

describe("concurrent dequeue", () => {
  it("two concurrent dequeues do not return the same task", async () => {
    await enqueue("only-one", {});

    const [first, second] = await Promise.all([dequeue(), dequeue()]);

    const results = [first, second].filter(Boolean);
    expect(results).toHaveLength(1);
  });
});
