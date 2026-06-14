import { dequeue, ack, nack } from "./queue";

type TaskHandler = (payload: object) => Promise<void>;

const handlers = new Map<string, TaskHandler>();

let running = false;

export function register(name: string, handler: TaskHandler): void {
  handlers.set(name, handler);
}

export async function start(pollIntervalMs = 1000): Promise<void> {
  running = true;
  console.log("Worker started, polling for tasks...");

  process.on("SIGINT", () => shutdown());
  process.on("SIGTERM", () => shutdown());

  while (running) {
    const task = await dequeue();

    if (!task) {
      await sleep(pollIntervalMs);
      continue;
    }

    console.log(`Processing task ${task.id} [${task.name}]`);

    const handler = handlers.get(task.name);

    if (!handler) {
      console.error(`No handler registered for "${task.name}"`);
      await nack(task.id);
      continue;
    }

    try {
      await handler(task.payload);
      await ack(task.id);
      console.log(`Completed task ${task.id}`);
    } catch (err) {
      console.error(`Failed task ${task.id}:`, err);
      await nack(task.id);
    }
  }

  console.log("Worker shut down");
}

function shutdown(): void {
  console.log("\nShutting down");
  running = false;
}

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}
