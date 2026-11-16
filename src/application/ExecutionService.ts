import { randomUUID } from 'crypto';
import {
  aggregateParts,
  Execution,
  ExecutionStatus,
  InvalidTransitionError,
  normalizePriority,
} from '../domain/Execution';
import { MissingPart, StockItem } from '../domain/StockItem';
import {
  CancelarExecucaoPayload,
  Events,
  EventType,
  IniciarExecucaoPayload,
  MessageEnvelope,
  PartQuantity,
  PRODUCER,
} from '../domain/messages';
import { AppError } from '../shared/AppError';
import { Clock, ExecutionFilter, ExecutionTx, IdGenerator, IExecutionStore, Logger, silentLogger } from './ports';

export type StaffRole = 'ADMIN' | 'MECHANIC';

/** Quem chama a API (sempre funcionário: clientes não operam a execução). */
export interface Requester {
  sub: string;
  role: StaffRole;
}

export interface ExecutionServiceOptions {
  clock?: Clock;
  newId?: IdGenerator;
  logger?: Logger;
}

/**
 * Casos de uso do execution-service.
 *
 * Cada mudança de estado acontece numa transação que também grava o evento no outbox,
 * então banco e mensagem nunca divergem (o relay publica no RabbitMQ depois).
 */
export class ExecutionService {
  private readonly clock: Clock;
  private readonly newId: IdGenerator;
  private readonly logger: Logger;

  constructor(
    private readonly store: IExecutionStore,
    options: ExecutionServiceOptions = {},
  ) {
    this.clock = options.clock ?? (() => new Date());
    this.newId = options.newId ?? randomUUID;
    this.logger = options.logger ?? silentLogger;
  }

  // ---------------------------------------------------------------------------
  // Comandos da Saga
  // ---------------------------------------------------------------------------

  /**
   * execution.IniciarExecucao: reserva as peças e coloca a OS na fila.
   * Sem estoque suficiente, responde ExecucaoRecusada e a Saga compensa (estorno + cancelamento).
   * Idempotente por sagaId: reentregas do comando não reservam de novo.
   */
  async startExecution(command: MessageEnvelope<IniciarExecucaoPayload>): Promise<Execution> {
    const payload = command.payload ?? ({} as IniciarExecucaoPayload);
    const parts = aggregateParts(Array.isArray(payload.parts) ? payload.parts : []);
    const services = Array.isArray(payload.services) ? payload.services : [];
    const priority = normalizePriority(payload.priority);

    return this.store.transaction(async (tx) => {
      const existing = await tx.findExecutionBySagaId(command.sagaId);
      if (existing) {
        this.logger.info('IniciarExecucao repetido ignorado', { sagaId: command.sagaId, executionId: existing.id });
        return existing;
      }

      const now = this.clock();
      const stock = await tx.lockStock(parts.map((p) => p.partId));
      const missing = findMissingParts(parts, stock);
      const base = { id: this.newId(), osId: command.osId, sagaId: command.sagaId, quoteId: payload.quoteId ?? null, priority, services, parts, now };

      if (missing.length > 0) {
        const refused = Execution.refused({ ...base, reason: 'ESTOQUE_INSUFICIENTE' });
        await tx.insertExecution(refused);
        await tx.addOutbox(this.event(refused, Events.EXECUCAO_RECUSADA, { reason: 'ESTOQUE_INSUFICIENTE', missingParts: missing }, command));
        this.logger.warn('Execução recusada por falta de estoque', { osId: command.osId, missing });
        return refused;
      }

      for (const p of parts) {
        const item = stock.get(p.partId)!;
        await tx.saveStock({ ...item, available: item.available - p.quantity, reserved: item.reserved + p.quantity, updatedAt: now });
      }
      const execution = Execution.queue(base);
      await tx.insertExecution(execution);
      const queuePosition = await tx.queuePosition(execution);
      await tx.addOutbox(this.event(execution, Events.EXECUCAO_INICIADA, { executionId: execution.id, queuePosition, reservedParts: parts }, command));
      this.logger.info('OS colocada na fila de execução', { osId: command.osId, executionId: execution.id, queuePosition });
      return execution;
    });
  }

  /** execution.CancelarExecucao: libera as peças reservadas. Idempotente. */
  async cancelExecution(command: MessageEnvelope<CancelarExecucaoPayload>): Promise<Execution | null> {
    const executionId = command.payload?.executionId;
    if (!executionId) {
      this.logger.warn('CancelarExecucao sem executionId ignorado', { messageId: command.messageId });
      return null;
    }
    return this.store.transaction(async (tx) => {
      const execution = await tx.findExecutionById(executionId, true);
      if (!execution) {
        this.logger.warn('CancelarExecucao para execução inexistente', { executionId });
        return null;
      }
      if (execution.status === ExecutionStatus.CANCELADA) return execution;
      if (!execution.holdsReservation) {
        this.logger.warn('Execução já encerrada não pode ser cancelada', { executionId, status: execution.status });
        return execution;
      }
      const now = this.clock();
      execution.cancel(command.payload.reason ?? 'Cancelada pela Saga', now);
      await this.releaseParts(tx, execution.parts, now);
      await tx.updateExecution(execution);
      await tx.addOutbox(this.event(execution, Events.EXECUCAO_CANCELADA, { executionId: execution.id }, command));
      return execution;
    });
  }

  // ---------------------------------------------------------------------------
  // Operação da oficina (API REST, funcionários)
  // ---------------------------------------------------------------------------

  startDiagnosis(id: string, requester: Requester, mechanicId?: string): Promise<Execution> {
    const mechanic = mechanicId ?? requester.sub;
    return this.change(id, (execution, now) => {
      execution.startDiagnosis(mechanic, now);
      return { type: Events.DIAGNOSTICO_INICIADO, payload: { executionId: execution.id, mechanicId: mechanic } };
    });
  }

  startRepair(id: string, requester: Requester): Promise<Execution> {
    return this.change(id, (execution, now) => {
      execution.startRepair(now, requester.sub);
      return { type: Events.REPARO_INICIADO, payload: { executionId: execution.id } };
    });
  }

  /** Finaliza o reparo: dá baixa nas peças reservadas e avisa o os-service (OS → FINALIZADA). */
  finish(id: string, requester: Requester): Promise<Execution> {
    return this.change(id, async (execution, now, tx) => {
      execution.finish(now, requester.sub);
      const stock = await tx.lockStock(execution.parts.map((p) => p.partId));
      for (const p of execution.parts) {
        const item = stock.get(p.partId);
        if (item) await tx.saveStock({ ...item, reserved: Math.max(0, item.reserved - p.quantity), updatedAt: now });
      }
      return {
        type: Events.EXECUCAO_FINALIZADA,
        payload: { executionId: execution.id, finishedAt: now.toISOString(), consumedParts: execution.parts },
      };
    });
  }

  /** Falha no reparo: devolve as peças ao estoque e a Saga compensa (estorno + cancelamento). */
  fail(id: string, requester: Requester, reason: string | undefined): Promise<Execution> {
    const why = reason?.trim();
    if (!why) return Promise.reject(new AppError('Informe o motivo da falha (reason)', 422));
    return this.change(id, async (execution, now, tx) => {
      execution.fail(why, now, requester.sub);
      await this.releaseParts(tx, execution.parts, now);
      return { type: Events.EXECUCAO_FALHOU, payload: { executionId: execution.id, reason: why } };
    });
  }

  async get(id: string): Promise<Execution> {
    const execution = await this.store.findById(id);
    if (!execution) throw new AppError('Execução não encontrada', 404);
    return execution;
  }

  list(filter: ExecutionFilter = {}): Promise<Execution[]> {
    return this.store.list(filter);
  }

  /** Fila de trabalho da oficina: o que está aguardando e o que está em andamento. */
  queue(): Promise<Execution[]> {
    return this.store.list({ status: [ExecutionStatus.EM_FILA, ExecutionStatus.EM_DIAGNOSTICO, ExecutionStatus.EM_REPARO] });
  }

  listStock(): Promise<StockItem[]> {
    return this.store.listStock();
  }

  async setStock(partId: string, available: unknown, description: unknown, requester: Requester): Promise<StockItem> {
    if (requester.role !== 'ADMIN') throw new AppError('Somente administradores ajustam o estoque', 403);
    if (!partId?.trim()) throw new AppError('partId é obrigatório', 422);
    if (typeof available !== 'number' || !Number.isInteger(available) || available < 0) {
      throw new AppError('available deve ser um inteiro maior ou igual a zero', 422);
    }
    const desc = typeof description === 'string' && description.trim() ? description.trim() : null;
    return this.store.upsertStock(partId.trim(), available, desc, this.clock());
  }

  // ---------------------------------------------------------------------------

  private async change(
    id: string,
    apply: (execution: Execution, now: Date, tx: ExecutionTx) => { type: EventType; payload: Record<string, unknown> } | Promise<{ type: EventType; payload: Record<string, unknown> }>,
  ): Promise<Execution> {
    return this.store.transaction(async (tx) => {
      const execution = await tx.findExecutionById(id, true);
      if (!execution) throw new AppError('Execução não encontrada', 404);
      const now = this.clock();
      let outcome: { type: EventType; payload: Record<string, unknown> };
      try {
        outcome = await apply(execution, now, tx);
      } catch (err) {
        if (err instanceof InvalidTransitionError) {
          throw new AppError(`Não é possível mudar a execução de ${err.from} para ${err.to}`, 409);
        }
        throw err;
      }
      await tx.updateExecution(execution);
      await tx.addOutbox(this.event(execution, outcome.type, outcome.payload));
      return execution;
    });
  }

  private async releaseParts(tx: ExecutionTx, parts: PartQuantity[], now: Date): Promise<void> {
    const stock = await tx.lockStock(parts.map((p) => p.partId));
    for (const p of parts) {
      const item = stock.get(p.partId);
      if (!item) continue;
      const released = Math.min(item.reserved, p.quantity);
      await tx.saveStock({ ...item, available: item.available + released, reserved: item.reserved - released, updatedAt: now });
    }
  }

  private event(execution: Execution, type: EventType, payload: Record<string, unknown>, cause?: Pick<MessageEnvelope<unknown>, 'messageId' | 'correlationId'>): MessageEnvelope {
    return {
      messageId: this.newId(),
      type,
      version: 1,
      sagaId: execution.sagaId,
      correlationId: cause?.correlationId ?? execution.sagaId,
      causationId: cause?.messageId ?? null,
      osId: execution.osId,
      occurredAt: this.clock().toISOString(),
      producer: PRODUCER,
      payload,
    };
  }
}

export function findMissingParts(parts: PartQuantity[], stock: Map<string, StockItem>): MissingPart[] {
  return parts
    .map((p) => ({ partId: p.partId, requested: p.quantity, available: stock.get(p.partId)?.available ?? 0 }))
    .filter((m) => m.available < m.requested);
}
