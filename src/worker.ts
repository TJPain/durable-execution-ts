import {
  dequeue, ack, nack, type Task,
  registerWorker, deregisterWorker, heartbeat,
  reclaimStaleTasks, reclaimOrphanedTasks, failScheduleTimeouts,
} from "./queue";

interface WorkerOptions {
  name?: string;
  concurrency?: number;
  pollIntervalMs?: number;
  heartbeatIntervalMs?: number;
  sweeperIntervalMs?: number;
  inactivityThresholdSeconds?: number;
  signal?: AbortSignal;
}

/** Handlers receive an AbortSignal that fires when the task's timeout expires. */
type TaskHandler = (payload: object, signal: AbortSignal) => Promise<void>;

const handlers = new Map<string, TaskHandler>();

export function register(name: string, handler: TaskHandler): void {
  handlers.set(name, handler);
}

export function clearHandlers(): void {
  handlers.clear();
}

export async function start({
  name = "worker",
  concurrency = 5,
  pollIntervalMs = 1000,
  heartbeatIntervalMs = 5000,
  sweeperIntervalMs,
  inactivityThresholdSeconds = 15,
  signal,
}: WorkerOptions = {}): Promise<void> {
  const resolvedSweeperInterval = sweeperIntervalMs ?? Math.floor(inactivityThresholdSeconds * 1000 / 3);
  const workerId = await registerWorker(name);
  const active = new Set<Promise<void>>();

  console.log(`Worker "${name}" started (id=${workerId}, concurrency=${concurrency})`);

  const heartbeatTimer = setInterval(async () => {
    try {
      await heartbeat(workerId);
    } catch (err) {
      console.error("Heartbeat failed:", err);
    }
  }, heartbeatIntervalMs);

  // Periodic cleanup: reclaim stuck tasks and fail overdue ones
  const sweeperTimer = setInterval(async () => {
    try {
      const reclaimed = await reclaimStaleTasks(inactivityThresholdSeconds);
      if (reclaimed > 0) console.log(`Reclaimed ${reclaimed} tasks from stale workers`);

      const orphaned = await reclaimOrphanedTasks(inactivityThresholdSeconds);
      if (orphaned > 0) console.log(`Reclaimed ${orphaned} orphaned tasks`);

      const timedOut = await failScheduleTimeouts();
      if (timedOut > 0) console.log(`Failed ${timedOut} tasks past scheduling timeout`);
    } catch (err) {
      console.error("Sweeper failed:", err);
    }
  }, resolvedSweeperInterval);

  // Poll loop: dequeue tasks up to concurrency limit, sleep when idle
  while (!signal?.aborted) {
    // Back-pressure: wait for any in-flight task to finish before polling again
    if (active.size >= concurrency) {
      await Promise.race(active);
      continue;
    }

    let task: Task | null;

    try {
      task = await dequeue(workerId);
    } catch (err) {
      console.error("Failed to dequeue:", err);
      await sleep(pollIntervalMs);
      continue;
    }

    // Re-check after await — signal may have fired while we were dequeuing
    if (signal?.aborted) break;

    if (!task) {
      await sleep(pollIntervalMs);
      continue;
    }

    // Fire-and-forget: .finally() removes it from the set when done
    const job = processTask(task, workerId).finally(() => active.delete(job));
    active.add(job);
  }

  // Drain: wait for all in-flight tasks to finish before tearing down
  await Promise.all(active);
  clearInterval(heartbeatTimer);
  clearInterval(sweeperTimer);

  try {
    await deregisterWorker(workerId);
  } catch (err) {
    console.error("Failed to deregister worker:", err);
  }

  console.log("Worker shut down");
}

export async function processTask(task: Task, workerId: string): Promise<void> {
  console.log(`Processing task ${task.id} [${task.name}]`);

  const handler = handlers.get(task.name);

  if (!handler) {
    console.error(`No handler registered for "${task.name}"`);
    await nack(task.id, workerId, task, new Error(`no handler registered for "${task.name}"`));
    return;
  }

  const timeoutController = new AbortController();
  const timer = setTimeout(() => timeoutController.abort(), task.timeout_seconds * 1000);

  try {
    // Suppress unhandled rejection from the handler after timeout settles the race
    const handlerPromise = handler(task.payload, timeoutController.signal);
    handlerPromise.catch(() => {});

    // First promise to settle wins — enforces timeout even if handler ignores the signal
    await Promise.race([
      handlerPromise,
      new Promise<never>((_, reject) => {
        timeoutController.signal.addEventListener("abort", () =>
          reject(new Error(`task timed out after ${task.timeout_seconds}s`))
        );
      }),
    ]);
    clearTimeout(timer);
  } catch (err) {
    clearTimeout(timer);
    const error = err instanceof Error ? err : new Error(String(err));
    console.error(`Failed task ${task.id}:`, error.message);
    await nack(task.id, workerId, task, error);
    return;
  }

  // Ack separately — if this fails, the task ran successfully but stays "running"
  // (the sweeper will eventually reclaim it rather than re-executing)
  try {
    await ack(task.id, workerId);
    console.log(`Completed task ${task.id}`);
  } catch (err) {
    console.error(`Failed to ack task ${task.id} (task completed but ack failed):`, err);
  }
}

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}
