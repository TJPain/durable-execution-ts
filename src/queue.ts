import sql from "./db";

interface Task {
  id: string;
  name: string;
  payload: object;
}

export async function enqueue(name: string, payload: object = {}): Promise<string> {
  const [task] = await sql`
    INSERT INTO tasks (name, payload)
    VALUES (${name}, ${sql.json(payload)})
    RETURNING id
  `;
  return task.id;
}

export async function dequeue(): Promise<Task | null> {
  const [task] = await sql<Task[]>`
    UPDATE tasks
    SET status = 'running', started_at = now()
    WHERE id = (
      SELECT id FROM tasks
      WHERE status = 'pending'
      ORDER BY created_at
      FOR UPDATE SKIP LOCKED
      LIMIT 1
    )
    RETURNING id, name, payload
  `;
  return task ?? null;
}

export async function ack(id: string): Promise<void> {
  await sql`
    UPDATE tasks
    SET status = 'completed', completed_at = now()
    WHERE id = ${id}
  `;
}

export async function nack(id: string): Promise<void> {
  await sql`
    UPDATE tasks
    SET status = 'failed', completed_at = now()
    WHERE id = ${id}
  `;
}
