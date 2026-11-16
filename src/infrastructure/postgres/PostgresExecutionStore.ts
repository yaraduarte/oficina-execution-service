import { Pool, PoolClient, PoolConfig, QueryResultRow } from 'pg';
import { Execution, ExecutionStatus, Priority, priorityRank } from '../../domain/Execution';
import { StockItem } from '../../domain/StockItem';
import { MessageEnvelope } from '../../domain/messages';
import { ExecutionFilter, ExecutionTx, IExecutionStore } from '../../application/ports';
import { SCHEMA_SQL } from './schema';

type Queryable = Pick<PoolClient, 'query'>;

export interface DatabaseConfig {
  host: string;
  port: number;
  database: string;
  user: string;
  password: string;
  ssl: boolean;
}

export function createPool(config: DatabaseConfig): Pool {
  const options: PoolConfig = {
    host: config.host,
    port: config.port,
    database: config.database,
    user: config.user,
    password: config.password,
    max: 10,
    ssl: config.ssl ? { rejectUnauthorized: false } : undefined,
  };
  return new Pool(options);
}

const EXECUTION_COLUMNS = `id, os_id, saga_id, quote_id, priority, status, services, parts, mechanic_id, failure_reason,
  history, queued_at, diagnosis_started_at, repair_started_at, finished_at, updated_at`;

function toExecution(row: QueryResultRow): Execution {
  return new Execution({
    id: row.id,
    osId: row.os_id,
    sagaId: row.saga_id,
    quoteId: row.quote_id,
    priority: row.priority as Priority,
    status: row.status as ExecutionStatus,
    services: row.services,
    parts: row.parts,
    mechanicId: row.mechanic_id,
    failureReason: row.failure_reason,
    history: row.history,
    queuedAt: new Date(row.queued_at),
    diagnosisStartedAt: row.diagnosis_started_at ? new Date(row.diagnosis_started_at) : null,
    repairStartedAt: row.repair_started_at ? new Date(row.repair_started_at) : null,
    finishedAt: row.finished_at ? new Date(row.finished_at) : null,
    updatedAt: new Date(row.updated_at),
  });
}

function toStock(row: QueryResultRow): StockItem {
  return {
    partId: row.part_id,
    description: row.description,
    available: Number(row.available),
    reserved: Number(row.reserved),
    updatedAt: new Date(row.updated_at),
  };
}

class PostgresTx implements ExecutionTx {
  constructor(private readonly db: Queryable) {}

  async findExecutionById(id: string, forUpdate = false): Promise<Execution | null> {
    const { rows } = await this.db.query(
      `SELECT ${EXECUTION_COLUMNS} FROM executions WHERE id::text = $1${forUpdate ? ' FOR UPDATE' : ''}`,
      [id],
    );
    return rows[0] ? toExecution(rows[0]) : null;
  }

  async findExecutionBySagaId(sagaId: string): Promise<Execution | null> {
    const { rows } = await this.db.query(`SELECT ${EXECUTION_COLUMNS} FROM executions WHERE saga_id = $1`, [sagaId]);
    return rows[0] ? toExecution(rows[0]) : null;
  }

  async insertExecution(e: Execution): Promise<void> {
    await this.db.query(
      `INSERT INTO executions (id, os_id, saga_id, quote_id, priority, priority_rank, status, services, parts, mechanic_id,
         failure_reason, history, queued_at, diagnosis_started_at, repair_started_at, finished_at, updated_at)
       VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17)`,
      [
        e.id, e.osId, e.sagaId, e.quoteId, e.priority, priorityRank(e.priority), e.status,
        JSON.stringify(e.services), JSON.stringify(e.parts), e.mechanicId, e.failureReason, JSON.stringify(e.history),
        e.queuedAt, e.diagnosisStartedAt, e.repairStartedAt, e.finishedAt, e.updatedAt,
      ],
    );
  }

  async updateExecution(e: Execution): Promise<void> {
    await this.db.query(
      `UPDATE executions SET status = $2, mechanic_id = $3, failure_reason = $4, history = $5,
         diagnosis_started_at = $6, repair_started_at = $7, finished_at = $8, updated_at = $9
       WHERE id = $1`,
      [e.id, e.status, e.mechanicId, e.failureReason, JSON.stringify(e.history), e.diagnosisStartedAt, e.repairStartedAt, e.finishedAt, e.updatedAt],
    );
  }

  async queuePosition(e: Execution): Promise<number> {
    const { rows } = await this.db.query(
      `SELECT count(*)::int AS ahead FROM executions
        WHERE status = $1 AND id <> $2
          AND (priority_rank < $3 OR (priority_rank = $3 AND queued_at <= $4))`,
      [ExecutionStatus.EM_FILA, e.id, priorityRank(e.priority), e.queuedAt],
    );
    return Number(rows[0].ahead) + 1;
  }

  async lockStock(partIds: string[]): Promise<Map<string, StockItem>> {
    const result = new Map<string, StockItem>();
    if (partIds.length === 0) return result;
    // Ordem fixa evita deadlock entre execuções concorrentes que disputam as mesmas peças
    const { rows } = await this.db.query(
      `SELECT part_id, description, available, reserved, updated_at FROM stock_items
        WHERE part_id = ANY($1::text[]) ORDER BY part_id FOR UPDATE`,
      [[...new Set(partIds)]],
    );
    for (const row of rows) result.set(row.part_id, toStock(row));
    return result;
  }

  async saveStock(item: StockItem): Promise<void> {
    await this.db.query(
      'UPDATE stock_items SET available = $2, reserved = $3, updated_at = $4 WHERE part_id = $1',
      [item.partId, item.available, item.reserved, item.updatedAt],
    );
  }

  async addOutbox(envelope: MessageEnvelope): Promise<void> {
    await this.db.query('INSERT INTO outbox_messages (id, type, envelope) VALUES ($1, $2, $3)', [
      envelope.messageId,
      envelope.type,
      JSON.stringify(envelope),
    ]);
  }
}

export class PostgresExecutionStore implements IExecutionStore {
  constructor(private readonly pool: Pool) {}

  async migrate(): Promise<void> {
    await this.pool.query(SCHEMA_SQL);
  }

  async transaction<T>(work: (tx: ExecutionTx) => Promise<T>): Promise<T> {
    const client = await this.pool.connect();
    try {
      await client.query('BEGIN');
      const result = await work(new PostgresTx(client));
      await client.query('COMMIT');
      return result;
    } catch (err) {
      await client.query('ROLLBACK').catch(() => undefined);
      throw err;
    } finally {
      client.release();
    }
  }

  findById(id: string): Promise<Execution | null> {
    return new PostgresTx(this.pool).findExecutionById(id);
  }

  async list(filter: ExecutionFilter = {}): Promise<Execution[]> {
    const where: string[] = [];
    const params: unknown[] = [];
    if (filter.status?.length) {
      params.push(filter.status);
      where.push(`status = ANY($${params.length}::text[])`);
    }
    if (filter.osId) {
      params.push(filter.osId);
      where.push(`os_id = $${params.length}`);
    }
    const { rows } = await this.pool.query(
      `SELECT ${EXECUTION_COLUMNS} FROM executions ${where.length ? `WHERE ${where.join(' AND ')}` : ''}
        ORDER BY priority_rank, queued_at LIMIT 200`,
      params,
    );
    return rows.map(toExecution);
  }

  async listStock(): Promise<StockItem[]> {
    const { rows } = await this.pool.query('SELECT part_id, description, available, reserved, updated_at FROM stock_items ORDER BY part_id');
    return rows.map(toStock);
  }

  async upsertStock(partId: string, available: number, description: string | null, now: Date): Promise<StockItem> {
    const { rows } = await this.pool.query(
      `INSERT INTO stock_items (part_id, description, available, reserved, updated_at) VALUES ($1, $2, $3, 0, $4)
       ON CONFLICT (part_id) DO UPDATE SET available = EXCLUDED.available,
         description = COALESCE(EXCLUDED.description, stock_items.description), updated_at = EXCLUDED.updated_at
       RETURNING part_id, description, available, reserved, updated_at`,
      [partId, description, available, now],
    );
    return toStock(rows[0]);
  }

  /**
   * Publica eventos pendentes do outbox. SKIP LOCKED permite várias réplicas sem publicar
   * a mesma mensagem em paralelo; a ordem por created_at preserva a sequência de cada OS.
   */
  async relayOutbox(publish: (envelope: MessageEnvelope) => Promise<void>, limit = 50): Promise<number> {
    const client = await this.pool.connect();
    try {
      await client.query('BEGIN');
      const { rows } = await client.query(
        `SELECT id, envelope FROM outbox_messages WHERE published_at IS NULL
          ORDER BY created_at LIMIT $1 FOR UPDATE SKIP LOCKED`,
        [limit],
      );
      let published = 0;
      for (const row of rows) {
        try {
          await publish(row.envelope as MessageEnvelope);
          await client.query('UPDATE outbox_messages SET published_at = now(), attempts = attempts + 1 WHERE id = $1', [row.id]);
          published += 1;
        } catch (err) {
          await client.query('UPDATE outbox_messages SET attempts = attempts + 1, last_error = $2 WHERE id = $1', [
            row.id,
            err instanceof Error ? err.message : String(err),
          ]);
          break; // mantém a ordem: tenta de novo no próximo ciclo a partir desta mensagem
        }
      }
      await client.query('COMMIT');
      return published;
    } catch (err) {
      await client.query('ROLLBACK').catch(() => undefined);
      throw err;
    } finally {
      client.release();
    }
  }

  async ping(): Promise<boolean> {
    try {
      await this.pool.query('SELECT 1');
      return true;
    } catch {
      return false;
    }
  }

  close(): Promise<void> {
    return this.pool.end();
  }
}
