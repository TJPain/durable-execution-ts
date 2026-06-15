CREATE TABLE IF NOT EXISTS workers (
  id                UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  name              TEXT NOT NULL,
  last_heartbeat_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_workers_heartbeat ON workers (last_heartbeat_at);

CREATE TABLE IF NOT EXISTS tasks (
  id                   UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  name                 TEXT NOT NULL,
  payload              JSONB NOT NULL DEFAULT '{}',
  status               TEXT NOT NULL DEFAULT 'pending' CHECK (status IN ('pending', 'running', 'completed', 'failed')),
  priority             INT NOT NULL DEFAULT 0,
  attempts             INT NOT NULL DEFAULT 0,
  max_attempts         INT NOT NULL DEFAULT 3,
  timeout_seconds      INT NOT NULL DEFAULT 60,
  run_after            TIMESTAMPTZ NOT NULL DEFAULT now(),
  schedule_timeout_at  TIMESTAMPTZ,
  worker_id            UUID REFERENCES workers(id) ON DELETE SET NULL,
  error                TEXT,
  created_at           TIMESTAMPTZ NOT NULL DEFAULT now(),
  started_at           TIMESTAMPTZ,
  completed_at         TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS idx_tasks_dequeue ON tasks (status, priority DESC, run_after, created_at)
  WHERE status = 'pending';

CREATE INDEX IF NOT EXISTS idx_tasks_worker ON tasks (worker_id, status)
  WHERE status = 'running';

CREATE INDEX IF NOT EXISTS idx_tasks_schedule_timeout ON tasks (status, schedule_timeout_at)
  WHERE status = 'pending' AND schedule_timeout_at IS NOT NULL;
