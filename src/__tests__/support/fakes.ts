import { Execution, ExecutionStatus, priorityRank } from '../../domain/Execution';
import { StockItem } from '../../domain/StockItem';
import { Commands, IniciarExecucaoPayload, MessageEnvelope } from '../../domain/messages';
import { ExecutionFilter, ExecutionTx, IExecutionStore } from '../../application/ports';
import { ExecutionService } from '../../application/ExecutionService';

const clone = <T>(v: T): T => JSON.parse(JSON.stringify(v));
const reviveExecution = (e: Execution) =>
  new Execution({
    ...clone(e),
    queuedAt: new Date(e.queuedAt),
    diagnosisStartedAt: e.diagnosisStartedAt ? new Date(e.diagnosisStartedAt) : null,
    repairStartedAt: e.repairStartedAt ? new Date(e.repairStartedAt) : null,
    finishedAt: e.finishedAt ? new Date(e.finishedAt) : null,
    updatedAt: new Date(e.updatedAt),
  });

/** Store em memória com semântica transacional (commit/rollback por cópia). */
export class InMemoryExecutionStore implements IExecutionStore {
  executions = new Map<string, Execution>();
  stock = new Map<string, StockItem>();
  outbox: MessageEnvelope[] = [];

  async transaction<T>(work: (tx: ExecutionTx) => Promise<T>): Promise<T> {
    const executions = new Map([...this.executions].map(([k, v]) => [k, reviveExecution(v)]));
    const stock = new Map([...this.stock].map(([k, v]) => [k, { ...v }]));
    const outbox = [...this.outbox];
    const tx: ExecutionTx = {
      findExecutionById: async (id) => (executions.has(id) ? reviveExecution(executions.get(id)!) : null),
      findExecutionBySagaId: async (sagaId) => {
        const found = [...executions.values()].find((e) => e.sagaId === sagaId);
        return found ? reviveExecution(found) : null;
      },
      insertExecution: async (e) => {
        executions.set(e.id, reviveExecution(e));
      },
      updateExecution: async (e) => {
        executions.set(e.id, reviveExecution(e));
      },
      queuePosition: async (e) =>
        [...executions.values()].filter(
          (o) =>
            o.status === ExecutionStatus.EM_FILA &&
            o.id !== e.id &&
            (priorityRank(o.priority) < priorityRank(e.priority) ||
              (priorityRank(o.priority) === priorityRank(e.priority) && o.queuedAt <= e.queuedAt)),
        ).length + 1,
      lockStock: async (ids) => new Map(ids.filter((id) => stock.has(id)).map((id) => [id, { ...stock.get(id)! }])),
      saveStock: async (item) => {
        stock.set(item.partId, { ...item });
      },
      addOutbox: async (envelope) => {
        outbox.push(clone(envelope));
      },
    };
    const result = await work(tx);
    this.executions = executions;
    this.stock = stock;
    this.outbox = outbox;
    return result;
  }

  async findById(id: string) {
    return this.executions.has(id) ? reviveExecution(this.executions.get(id)!) : null;
  }

  async list(filter: ExecutionFilter = {}) {
    return [...this.executions.values()]
      .filter((e) => !filter.status?.length || filter.status.includes(e.status))
      .filter((e) => !filter.osId || e.osId === filter.osId)
      .sort((a, b) => priorityRank(a.priority) - priorityRank(b.priority) || a.queuedAt.getTime() - b.queuedAt.getTime())
      .map(reviveExecution);
  }

  async listStock() {
    return [...this.stock.values()].sort((a, b) => a.partId.localeCompare(b.partId));
  }

  async upsertStock(partId: string, available: number, description: string | null, now: Date) {
    const current = this.stock.get(partId);
    const item: StockItem = {
      partId,
      description: description ?? current?.description ?? null,
      available,
      reserved: current?.reserved ?? 0,
      updatedAt: now,
    };
    this.stock.set(partId, item);
    return { ...item };
  }

  seed(partId: string, available: number, reserved = 0) {
    this.stock.set(partId, { partId, description: `Peça ${partId}`, available, reserved, updatedAt: new Date(0) });
  }

  types() {
    return this.outbox.map((e) => e.type);
  }
}

let seq = 0;
export function command<P>(type: string, payload: P, overrides: Partial<MessageEnvelope<P>> = {}): MessageEnvelope<P> {
  seq += 1;
  return {
    messageId: `cmd-${seq}`,
    type,
    version: 1,
    sagaId: 'saga-1',
    correlationId: 'saga-1',
    causationId: null,
    osId: 'os-1',
    occurredAt: '2026-11-16T12:00:00.000Z',
    producer: 'os-service',
    payload,
    ...overrides,
  };
}

export function iniciarExecucao(
  overrides: Partial<MessageEnvelope<IniciarExecucaoPayload>> = {},
  parts = [{ partId: 'p1', quantity: 2 }],
): MessageEnvelope<IniciarExecucaoPayload> {
  return command<IniciarExecucaoPayload>(
    Commands.INICIAR_EXECUCAO,
    { quoteId: 'q-1', priority: 'NORMAL', services: [{ serviceId: 's1', description: 'Troca de óleo' }], parts },
    overrides,
  );
}

export function makeHarness(start = new Date('2026-11-16T12:00:00.000Z')) {
  const store = new InMemoryExecutionStore();
  let now = start.getTime();
  let id = 0;
  const clock = () => new Date(now);
  const advance = (ms = 1000) => {
    now += ms;
  };
  const executions = new ExecutionService(store, { clock, newId: () => `id-${(id += 1)}` });
  return { store, executions, advance, clock };
}

export const mechanic = { sub: 'mech-1', role: 'MECHANIC' as const };
export const admin = { sub: 'admin-1', role: 'ADMIN' as const };
