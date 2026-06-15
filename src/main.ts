import sql from "./db";
import { enqueue } from "./queue";
import { register, start } from "./worker";

register("send-welcome-email", async (payload) => {
  const { email } = payload as { email: string };
  await sleep(1000);
  console.log(`Sent welcome email to ${email}`);
});

register("generate-report", async (payload) => {
  const { reportType } = payload as { reportType: string };
  await sleep(2000);
  console.log(`Generated ${reportType} report`);
});

register("process-payment", async (payload) => {
  const { amount } = payload as { amount: number };
  await sleep(500);
  console.log(`Processed payment of £${amount}`);
});

// Low priority — background work
await enqueue("generate-report", { reportType: "monthly" }, { priority: 0 });
await enqueue("send-welcome-email", { email: "user@example.com" }, { priority: 1 });

// High priority — payment processing should jump the queue
await enqueue("process-payment", { amount: 500 }, { priority: 10 });
await enqueue("process-payment", { amount: 250 }, { priority: 10 });

// This one has a scheduling timeout — if not picked up in 30s, it fails
await enqueue("generate-report", { reportType: "ad-hoc" }, { priority: 0, scheduleTimeoutSeconds: 30 });

const controller = new AbortController();
process.on("SIGINT", () => controller.abort());
process.on("SIGTERM", () => controller.abort());

await start({ name: "demo-worker", concurrency: 3, signal: controller.signal });

await sql.end();

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}
