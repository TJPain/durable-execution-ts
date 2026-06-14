import { defineConfig } from "vitest/config";

export default defineConfig({
  test: {
    env: {
      DATABASE_URL: "postgresql://durable:durable@localhost:5432/durable",
    },
  },
});
