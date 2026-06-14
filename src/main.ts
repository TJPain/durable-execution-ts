import sql from "./db";
import { enqueue } from "./queue";
import { register, start } from "./worker";

register("send-welcome-email", async (payload) => {
  const { email, userId } = payload as { email: string; userId: string };
  console.log(`Sending welcome email to ${email} (user: ${userId})`);
  // In reality: call SendGrid/SES/etc
});

register("generate-report", async (payload) => {
  const { reportType, month } = payload as { reportType: string; month: string };
  console.log(`Generating ${reportType} report for ${month}`);
  // In reality: query data, build PDF, upload to S3
});

await enqueue("send-welcome-email", { email: "user@example.com", userId: "usr_123" });
await enqueue("generate-report", { reportType: "investor", month: "2026-05" });
await enqueue("send-welcome-email", { email: "another@example.com", userId: "usr_456" });

await start();

await sql.end();
