/**
 * Esquema do execution_db (banco próprio do serviço — ADR-007).
 * Idempotente: executado na inicialização de cada réplica.
 */
export const SCHEMA_SQL = `
CREATE TABLE IF NOT EXISTS executions (
  id                    UUID PRIMARY KEY,
  os_id                 TEXT NOT NULL,
  saga_id               TEXT NOT NULL UNIQUE,
  quote_id              TEXT,
  priority              TEXT NOT NULL,
  priority_rank         SMALLINT NOT NULL,
  status                TEXT NOT NULL,
  services              JSONB NOT NULL DEFAULT '[]',
  parts                 JSONB NOT NULL DEFAULT '[]',
  mechanic_id           TEXT,
  failure_reason        TEXT,
  history               JSONB NOT NULL DEFAULT '[]',
  queued_at             TIMESTAMPTZ NOT NULL,
  diagnosis_started_at  TIMESTAMPTZ,
  repair_started_at     TIMESTAMPTZ,
  finished_at           TIMESTAMPTZ,
  updated_at            TIMESTAMPTZ NOT NULL
);
CREATE INDEX IF NOT EXISTS executions_status_queue_idx ON executions (status, priority_rank, queued_at);
CREATE INDEX IF NOT EXISTS executions_os_idx ON executions (os_id);

CREATE TABLE IF NOT EXISTS stock_items (
  part_id      TEXT PRIMARY KEY,
  description  TEXT,
  available    INTEGER NOT NULL CHECK (available >= 0),
  reserved     INTEGER NOT NULL DEFAULT 0 CHECK (reserved >= 0),
  updated_at   TIMESTAMPTZ NOT NULL
);

CREATE TABLE IF NOT EXISTS outbox_messages (
  id            UUID PRIMARY KEY,
  type          TEXT NOT NULL,
  envelope      JSONB NOT NULL,
  created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
  published_at  TIMESTAMPTZ,
  attempts      INTEGER NOT NULL DEFAULT 0,
  last_error    TEXT
);
CREATE INDEX IF NOT EXISTS outbox_pending_idx ON outbox_messages (created_at) WHERE published_at IS NULL;
`;
