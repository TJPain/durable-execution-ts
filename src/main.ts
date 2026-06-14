import sql from "./db";
import { enqueue, NonRetryableError } from "./queue";
import { register, start } from "./worker";

let emailAttempts = 0;

register("send-welcome-email", async (payload) => {
  const { email } = payload as { email: string };
  emailAttempts++;

  if (emailAttempts <= 2) {
    throw new Error("SMTP connection timeout");
  }

  console.log(`Sent welcome email to ${email} (succeeded on attempt ${emailAttempts})`);
});

register("validate-input", async (payload) => {
  const { data } = payload as { data: string };
  if (!data) {
    throw new NonRetryableError("missing required field: data");
  }
  console.log(`Validated input: ${data}`);
});

register("long-running-task", async (_payload, signal) => {
  await new Promise((resolve, reject) => {
    const timer = setTimeout(resolve, 5000);
    signal.addEventListener("abort", () => {
      clearTimeout(timer);
      reject(new Error("task timed out"));
    });
  });
});

// Will fail twice then succeed on third attempt
await enqueue("send-welcome-email", { email: "user@example.com" }, { maxAttempts: 3 });

// Will fail permanently (non-retryable)
await enqueue("validate-input", { data: "" }, { maxAttempts: 5 });

// Will time out after 2 seconds
await enqueue("long-running-task", {}, { maxAttempts: 1, timeoutSeconds: 2 });

const controller = new AbortController();
process.on("SIGINT", () => controller.abort());
process.on("SIGTERM", () => controller.abort());

await start({ concurrency: 3, pollIntervalMs: 500, signal: controller.signal });

await sql.end();
