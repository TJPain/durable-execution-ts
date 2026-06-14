import sql from "./db";
import { enqueue } from "./queue";
import { register, start } from "./worker";

register("send-welcome-email", async (payload) => {
  const { email, userId } = payload as { email: string; userId: string };
  await sleep(2000);
  console.log(`Sent welcome email to ${email} (user: ${userId})`);
});

register("generate-report", async (payload) => {
  const { reportType, month } = payload as { reportType: string; month: string };
  await sleep(3000);
  console.log(`Generated ${reportType} report for ${month}`);
});

await enqueue("send-welcome-email", { email: "alice@example.com", userId: "usr_1" });
await enqueue("send-welcome-email", { email: "bob@example.com", userId: "usr_2" });
await enqueue("send-welcome-email", { email: "carol@example.com", userId: "usr_3" });
await enqueue("generate-report", { reportType: "investor", month: "2026-05" });
await enqueue("generate-report", { reportType: "regulatory", month: "2026-05" });

const controller = new AbortController();
process.on("SIGINT", () => controller.abort());
process.on("SIGTERM", () => controller.abort());

await start({ concurrency: 3, signal: controller.signal });

await sql.end();

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}
