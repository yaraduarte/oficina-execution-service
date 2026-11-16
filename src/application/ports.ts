import { Execution, ExecutionStatus } from '../domain/Execution';
import { StockItem } from '../domain/StockItem';
import { MessageEnvelope } from '../domain/messages';

/** Operações dentro de uma transação do banco da execução (execution_db). */
export interface ExecutionTx {
  findExecutionById(id: string, forUpdate?: boolean): Promise<Execution | null>;
  findExecutionBySagaId(sagaId: string): Promise<Execution | null>;
  insertExecution(execution: Execution): Promise<void>;
  updateExecution(execution: Execution): Promise<void>;
  /** Posição (1-based) na fila de execuções EM_FILA, por prioridade e ordem de chegada. */
  queuePosition(execution: Execution): Promise<number>;
  /** Bloqueia as linhas de estoque (SELECT ... FOR UPDATE) e devolve as existentes. */
  lockStock(partIds: string[]): Promise<Map<string, StockItem>>;
  saveStock(item: StockItem): Promise<void>;
  /** Grava o evento no outbox na mesma transação; o relay publica depois. */
  addOutbox(envelope: MessageEnvelope): Promise<void>;
}

export interface ExecutionFilter {
  status?: ExecutionStatus[];
  osId?: string;
}

export interface IExecutionStore {
  transaction<T>(work: (tx: ExecutionTx) => Promise<T>): Promise<T>;
  findById(id: string): Promise<Execution | null>;
  list(filter?: ExecutionFilter): Promise<Execution[]>;
  listStock(): Promise<StockItem[]>;
  /** Cria ou atualiza o saldo disponível de uma peça (o reservado é preservado). */
  upsertStock(partId: string, available: number, description: string | null, now: Date): Promise<StockItem>;
}

export interface IEventPublisher {
  publish(envelope: MessageEnvelope): Promise<void>;
}

export interface Logger {
  info(msg: string, meta?: Record<string, unknown>): void;
  warn(msg: string, meta?: Record<string, unknown>): void;
}

export const silentLogger: Logger = { info: () => undefined, warn: () => undefined };

export type Clock = () => Date;
export type IdGenerator = () => string;
