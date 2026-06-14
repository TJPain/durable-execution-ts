import { describe, it, expect, beforeEach, afterAll } from "vitest";
import sql from "../src/db";
import { enqueue, dequeue, ack, nack } from "../src/queue";

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
});

describe("dequeue", () => {
  it("picks up the oldest pending task and sets it to running", async () => {
    await enqueue("first", {});
    await enqueue("second", {});

    const task = await dequeue();
    expect(task?.name).toBe("first");

    const [row] = await sql`SELECT status FROM tasks WHERE id = ${task!.id}`;
    expect(row.status).toBe("running");
  });

  it("returns null when queue is empty", async () => {
    const task = await dequeue();
    expect(task).toBeNull();
  });

  it("does not return tasks that are already running", async () => {
    const id = await enqueue("task", {});
    await dequeue();

    const second = await dequeue();
    expect(second).toBeNull();
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
  it("marks a task as failed", async () => {
    await enqueue("task", {});
    const task = await dequeue();
    await nack(task!.id);

    const [row] = await sql`SELECT status, completed_at FROM tasks WHERE id = ${task!.id}`;
    expect(row.status).toBe("failed");
    expect(row.completed_at).not.toBeNull();
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
