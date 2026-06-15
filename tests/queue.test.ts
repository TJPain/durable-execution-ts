import { describe, it, expect, beforeEach, afterAll } from "vitest";
import sql from "../src/db";
import {
  enqueue, dequeue, ack, nack, NonRetryableError,
  registerWorker, deregisterWorker, heartbeat,
  reclaimStaleTasks, reclaimOrphanedTasks, failScheduleTimeouts,
} from "../src/queue";

let workerId: string;

beforeEach(async () => {
  await sql`TRUNCATE tasks, workers CASCADE`;
  workerId = await registerWorker("test-worker");
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

  it("accepts custom options", async () => {
    const id = await enqueue("task", {}, {
      maxAttempts: 5,
      timeoutSeconds: 120,
      priority: 7,
      scheduleTimeoutSeconds: 60,
    });

    const [task] = await sql`SELECT max_attempts, timeout_seconds, priority, schedule_timeout_at FROM tasks WHERE id = ${id}`;
    expect(task.max_attempts).toBe(5);
    expect(task.timeout_seconds).toBe(120);
    expect(task.priority).toBe(7);
    expect(task.schedule_timeout_at).not.toBeNull();
  });
});

describe("dequeue", () => {
  it("picks up the oldest pending task and sets it to running", async () => {
    await enqueue("first", {});
    await enqueue("second", {});

    const task = await dequeue(workerId);
    expect(task?.name).toBe("first");

    const [row] = await sql`SELECT status, attempts, worker_id FROM tasks WHERE id = ${task!.id}`;
    expect(row.status).toBe("running");
    expect(row.attempts).toBe(1);
    expect(row.worker_id).toBe(workerId);
  });

  it("returns null when queue is empty", async () => {
    const task = await dequeue(workerId);
    expect(task).toBeNull();
  });

  it("does not return tasks that are already running", async () => {
    await enqueue("task", {});
    await dequeue(workerId);

    const second = await dequeue(workerId);
    expect(second).toBeNull();
  });

  it("does not return tasks whose run_after is in the future", async () => {
    await sql`
      INSERT INTO tasks (name, payload, run_after)
      VALUES ('future-task', '{}', now() + interval '1 hour')
    `;

    const task = await dequeue(workerId);
    expect(task).toBeNull();
  });

  it("dequeues higher priority tasks first", async () => {
    await enqueue("low", {}, { priority: 1 });
    await enqueue("high", {}, { priority: 10 });
    await enqueue("medium", {}, { priority: 5 });

    const first = await dequeue(workerId);
    const second = await dequeue(workerId);
    const third = await dequeue(workerId);

    expect(first?.name).toBe("high");
    expect(second?.name).toBe("medium");
    expect(third?.name).toBe("low");
  });
});

describe("ack", () => {
  it("marks a task as completed and clears worker_id", async () => {
    await enqueue("task", {});
    const task = await dequeue(workerId);
    await ack(task!.id, workerId);

    const [row] = await sql`SELECT status, completed_at, worker_id FROM tasks WHERE id = ${task!.id}`;
    expect(row.status).toBe("completed");
    expect(row.completed_at).not.toBeNull();
    expect(row.worker_id).toBeNull();
  });

  it("does not ack if worker_id does not match", async () => {
    await enqueue("task", {});
    const task = await dequeue(workerId);

    const otherWorkerId = await registerWorker("other-worker");
    await ack(task!.id, otherWorkerId);

    const [row] = await sql`SELECT status FROM tasks WHERE id = ${task!.id}`;
    expect(row.status).toBe("running");
  });
});

describe("nack", () => {
  it("retries if under max attempts", async () => {
    await enqueue("task", {}, { maxAttempts: 3 });
    const task = await dequeue(workerId);
    await nack(task!.id, workerId, task!, new Error("transient failure"));

    const [row] = await sql`SELECT status, error, run_after, worker_id FROM tasks WHERE id = ${task!.id}`;
    expect(row.status).toBe("pending");
    expect(row.error).toBe("transient failure");
    expect(row.worker_id).toBeNull();
    expect(new Date(row.run_after).getTime()).toBeGreaterThan(Date.now());
  });

  it("fails permanently at max attempts", async () => {
    await enqueue("task", {}, { maxAttempts: 1 });
    const task = await dequeue(workerId);
    await nack(task!.id, workerId, task!, new Error("fatal"));

    const [row] = await sql`SELECT status, error FROM tasks WHERE id = ${task!.id}`;
    expect(row.status).toBe("failed");
    expect(row.error).toBe("fatal");
  });

  it("fails permanently for NonRetryableError regardless of attempts", async () => {
    await enqueue("task", {}, { maxAttempts: 5 });
    const task = await dequeue(workerId);
    await nack(task!.id, workerId, task!, new NonRetryableError("bad input"));

    const [row] = await sql`SELECT status, error FROM tasks WHERE id = ${task!.id}`;
    expect(row.status).toBe("failed");
    expect(row.error).toBe("bad input");
  });

  it("applies exponential backoff on successive retries", async () => {
    await enqueue("task", {}, { maxAttempts: 4 });

    const first = await dequeue(workerId);
    await nack(first!.id, workerId, first!, new Error("fail"));
    const [after1] = await sql`SELECT run_after FROM tasks WHERE id = ${first!.id}`;

    await sql`UPDATE tasks SET run_after = now() WHERE id = ${first!.id}`;
    const second = await dequeue(workerId);
    await nack(second!.id, workerId, second!, new Error("fail"));
    const [after2] = await sql`SELECT run_after FROM tasks WHERE id = ${second!.id}`;

    const backoff1 = new Date(after1.run_after).getTime() - Date.now();
    const backoff2 = new Date(after2.run_after).getTime() - Date.now();
    expect(backoff2).toBeGreaterThan(backoff1);
  });

  it("does not update a task that is no longer running", async () => {
    await enqueue("task", {}, { maxAttempts: 3 });
    const task = await dequeue(workerId);
    await ack(task!.id, workerId);

    await nack(task!.id, workerId, task!, new Error("too late"));

    const [row] = await sql`SELECT status FROM tasks WHERE id = ${task!.id}`;
    expect(row.status).toBe("completed");
  });

  it("does not nack if worker_id does not match", async () => {
    await enqueue("task", {}, { maxAttempts: 3 });
    const task = await dequeue(workerId);

    const otherWorkerId = await registerWorker("other-worker");
    await nack(task!.id, otherWorkerId, task!, new Error("not mine"));

    const [row] = await sql`SELECT status FROM tasks WHERE id = ${task!.id}`;
    expect(row.status).toBe("running");
  });
});

describe("concurrent dequeue", () => {
  it("two concurrent dequeues do not return the same task", async () => {
    await enqueue("only-one", {});

    const [first, second] = await Promise.all([dequeue(workerId), dequeue(workerId)]);

    const results = [first, second].filter(Boolean);
    expect(results).toHaveLength(1);
  });
});

describe("worker heartbeat and reclaim", () => {
  it("reclaims tasks from stale workers and decrements attempts", async () => {
    await enqueue("stuck-task", {});
    await dequeue(workerId);

    await sql`UPDATE workers SET last_heartbeat_at = now() - interval '60 seconds' WHERE id = ${workerId}`;

    const reclaimed = await reclaimStaleTasks(15);
    expect(reclaimed).toBe(1);

    const [row] = await sql`SELECT status, worker_id, attempts FROM tasks WHERE name = 'stuck-task'`;
    expect(row.status).toBe("pending");
    expect(row.worker_id).toBeNull();
    expect(row.attempts).toBe(0);
  });

  it("does not reclaim tasks from active workers", async () => {
    await enqueue("active-task", {});
    await dequeue(workerId);
    await heartbeat(workerId);

    const reclaimed = await reclaimStaleTasks(15);
    expect(reclaimed).toBe(0);
  });

  it("reclaimed tasks are not killed by schedule timeout", async () => {
    // Task with a schedule timeout that's now in the past
    await sql`
      INSERT INTO tasks (name, payload, worker_id, status, attempts, schedule_timeout_at)
      VALUES ('reclaimed', '{}', ${workerId}, 'running', 1, now() - interval '10 seconds')
    `;

    await sql`UPDATE workers SET last_heartbeat_at = now() - interval '60 seconds' WHERE id = ${workerId}`;

    await reclaimStaleTasks(15);
    await failScheduleTimeouts();

    const [row] = await sql`SELECT status FROM tasks WHERE name = 'reclaimed'`;
    expect(row.status).toBe("pending");
  });
});

describe("orphaned task reclaim", () => {
  it("reclaims running tasks with no worker_id", async () => {
    await sql`
      INSERT INTO tasks (name, payload, status, worker_id, attempts, started_at)
      VALUES ('orphaned', '{}', 'running', NULL, 2, now() - interval '60 seconds')
    `;

    const reclaimed = await reclaimOrphanedTasks(15);
    expect(reclaimed).toBe(1);

    const [row] = await sql`SELECT status, attempts, started_at FROM tasks WHERE name = 'orphaned'`;
    expect(row.status).toBe("pending");
    expect(row.attempts).toBe(1);
    expect(row.started_at).toBeNull();
  });

  it("does not reclaim recent orphaned tasks", async () => {
    await sql`
      INSERT INTO tasks (name, payload, status, worker_id, attempts, started_at)
      VALUES ('fresh-orphan', '{}', 'running', NULL, 1, now())
    `;

    const reclaimed = await reclaimOrphanedTasks(15);
    expect(reclaimed).toBe(0);
  });
});

describe("schedule timeout", () => {
  it("fails tasks past their schedule_timeout_at", async () => {
    await sql`
      INSERT INTO tasks (name, payload, schedule_timeout_at)
      VALUES ('expired-task', '{}', now() - interval '1 second')
    `;

    const failed = await failScheduleTimeouts();
    expect(failed).toBe(1);

    const [row] = await sql`SELECT status, error FROM tasks WHERE name = 'expired-task'`;
    expect(row.status).toBe("failed");
    expect(row.error).toContain("scheduling timeout");
  });

  it("does not fail tasks that have not yet timed out", async () => {
    await enqueue("fresh-task", {}, { scheduleTimeoutSeconds: 3600 });

    const failed = await failScheduleTimeouts();
    expect(failed).toBe(0);
  });
});
