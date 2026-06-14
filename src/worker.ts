import { dequeue, ack, nack, type Task } from "./queue";

interface WorkerOptions {
  concurrency?: number;
  pollIntervalMs?: number;
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

export async function start({ concurrency = 5, pollIntervalMs = 1000, signal }: WorkerOptions = {}): Promise<void> {
  const active = new Set<Promise<void>>();

  console.log(`Worker started (concurrency=${concurrency}), polling for tasks...`);

  while (!signal?.aborted) {
    if (active.size >= concurrency) {
      await Promise.race(active);
      continue;
    }

    let task: Task | null;

    try {
      task = await dequeue();
    } catch (err) {
      console.error("Failed to dequeue:", err);
      await sleep(pollIntervalMs);
      continue;
    }

    if (signal?.aborted) break;

    if (!task) {
      await sleep(pollIntervalMs);
      continue;
    }

    const job = processTask(task).finally(() => active.delete(job));
    active.add(job);
  }

  await Promise.all(active);
  console.log("Worker shut down");
}

export async function processTask(task: Task): Promise<void> {
  console.log(`Processing task ${task.id} [${task.name}]`);

  const handler = handlers.get(task.name);

  if (!handler) {
    console.error(`No handler registered for "${task.name}"`);
    await nack(task.id, task, new Error(`no handler registered for "${task.name}"`));
    return;
  }

  const timeoutController = new AbortController();
  const timer = setTimeout(() => timeoutController.abort(), task.timeout_seconds * 1000);

  try {
    // Promise.race enforces the timeout even if the handler ignores the signal
    await Promise.race([
      handler(task.payload, timeoutController.signal),
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
    await nack(task.id, task, error);
    return;
  }

  // ack separately — if this fails, we don't nack (which would cause duplicate execution)
  try {
    await ack(task.id);
    console.log(`Completed task ${task.id}`);
  } catch (err) {
    console.error(`Failed to ack task ${task.id} (task completed but ack failed):`, err);
  }
}

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}
