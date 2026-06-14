import { dequeue, ack, nack } from "./queue";

interface Task {
  id: string;
  name: string;
  payload: object;
  timeout_seconds: number;
}

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

export async function start({ concurrency = 5, pollIntervalMs = 1000, signal }: WorkerOptions = {}): Promise<void> {
  const active = new Set<Promise<void>>();

  console.log(`Worker started (concurrency=${concurrency}), polling for tasks...`);

  while (!signal?.aborted) {
    if (active.size >= concurrency) {
      await Promise.race(active);
      continue;
    }

    const task = await dequeue();

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
    await nack(task.id, new Error(`no handler registered for "${task.name}"`));
    return;
  }

  const timeoutSignal = AbortSignal.timeout(task.timeout_seconds * 1000);

  try {
    await handler(task.payload, timeoutSignal);
    await ack(task.id);
    console.log(`Completed task ${task.id}`);
  } catch (err) {
    const error = err instanceof Error ? err : new Error(String(err));
    console.error(`Failed task ${task.id}:`, error.message);
    await nack(task.id, error);
  }
}

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}
