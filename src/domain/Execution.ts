import { PartQuantity, ServiceRef } from './messages';

export enum ExecutionStatus {
  EM_FILA = 'EM_FILA',
  EM_DIAGNOSTICO = 'EM_DIAGNOSTICO',
  EM_REPARO = 'EM_REPARO',
  FINALIZADA = 'FINALIZADA',
  FALHOU = 'FALHOU',
  CANCELADA = 'CANCELADA',
  /** Não entrou na fila (ex.: estoque insuficiente). Guardada para tornar o comando idempotente. */
  RECUSADA = 'RECUSADA',
}

export const TERMINAL_STATUSES: ExecutionStatus[] = [
  ExecutionStatus.FINALIZADA,
  ExecutionStatus.FALHOU,
  ExecutionStatus.CANCELADA,
  ExecutionStatus.RECUSADA,
];

/** Estados em que há peças reservadas no estoque. */
export const ACTIVE_STATUSES: ExecutionStatus[] = [
  ExecutionStatus.EM_FILA,
  ExecutionStatus.EM_DIAGNOSTICO,
  ExecutionStatus.EM_REPARO,
];

export const PRIORITIES = ['ALTA', 'NORMAL', 'BAIXA'] as const;
export type Priority = (typeof PRIORITIES)[number];

export function normalizePriority(value: unknown): Priority {
  const v = String(value ?? '').toUpperCase();
  return (PRIORITIES as readonly string[]).includes(v) ? (v as Priority) : 'NORMAL';
}

/** Ordem na fila: ALTA antes de NORMAL antes de BAIXA. */
export function priorityRank(priority: Priority): number {
  return PRIORITIES.indexOf(priority);
}

export interface ExecutionHistoryEntry {
  at: string;
  from: ExecutionStatus | null;
  to: ExecutionStatus;
  by?: string;
  note?: string;
}

export class InvalidTransitionError extends Error {
  constructor(
    public readonly from: ExecutionStatus,
    public readonly to: ExecutionStatus,
  ) {
    super(`Transição inválida: ${from} → ${to}`);
    this.name = 'InvalidTransitionError';
  }
}

export interface ExecutionProps {
  id: string;
  osId: string;
  sagaId: string;
  quoteId: string | null;
  priority: Priority;
  status: ExecutionStatus;
  services: ServiceRef[];
  parts: PartQuantity[];
  mechanicId: string | null;
  failureReason: string | null;
  history: ExecutionHistoryEntry[];
  queuedAt: Date;
  diagnosisStartedAt: Date | null;
  repairStartedAt: Date | null;
  finishedAt: Date | null;
  updatedAt: Date;
}

/**
 * Execução de uma OS na oficina: fila → diagnóstico → reparo → finalizada.
 * Máquina de estados pura; reserva/baixa de estoque e eventos ficam no caso de uso.
 */
export class Execution implements ExecutionProps {
  id: string;
  osId: string;
  sagaId: string;
  quoteId: string | null;
  priority: Priority;
  status: ExecutionStatus;
  services: ServiceRef[];
  parts: PartQuantity[];
  mechanicId: string | null;
  failureReason: string | null;
  history: ExecutionHistoryEntry[];
  queuedAt: Date;
  diagnosisStartedAt: Date | null;
  repairStartedAt: Date | null;
  finishedAt: Date | null;
  updatedAt: Date;

  constructor(props: ExecutionProps) {
    Object.assign(this, props);
  }

  static queue(input: {
    id: string;
    osId: string;
    sagaId: string;
    quoteId: string | null;
    priority: Priority;
    services: ServiceRef[];
    parts: PartQuantity[];
    now: Date;
  }): Execution {
    return new Execution({
      ...input,
      status: ExecutionStatus.EM_FILA,
      mechanicId: null,
      failureReason: null,
      history: [{ at: input.now.toISOString(), from: null, to: ExecutionStatus.EM_FILA }],
      queuedAt: input.now,
      diagnosisStartedAt: null,
      repairStartedAt: null,
      finishedAt: null,
      updatedAt: input.now,
    });
  }

  static refused(input: {
    id: string;
    osId: string;
    sagaId: string;
    quoteId: string | null;
    priority: Priority;
    services: ServiceRef[];
    parts: PartQuantity[];
    reason: string;
    now: Date;
  }): Execution {
    const { reason, ...rest } = input;
    return new Execution({
      ...rest,
      status: ExecutionStatus.RECUSADA,
      mechanicId: null,
      failureReason: reason,
      history: [{ at: input.now.toISOString(), from: null, to: ExecutionStatus.RECUSADA, note: reason }],
      queuedAt: input.now,
      diagnosisStartedAt: null,
      repairStartedAt: null,
      finishedAt: input.now,
      updatedAt: input.now,
    });
  }

  get isTerminal(): boolean {
    return TERMINAL_STATUSES.includes(this.status);
  }

  get holdsReservation(): boolean {
    return ACTIVE_STATUSES.includes(this.status);
  }

  startDiagnosis(mechanicId: string, now: Date): void {
    this.transition([ExecutionStatus.EM_FILA], ExecutionStatus.EM_DIAGNOSTICO, now, { by: mechanicId });
    this.mechanicId = mechanicId;
    this.diagnosisStartedAt = now;
  }

  startRepair(now: Date, by?: string): void {
    this.transition([ExecutionStatus.EM_DIAGNOSTICO], ExecutionStatus.EM_REPARO, now, { by });
    this.repairStartedAt = now;
  }

  finish(now: Date, by?: string): void {
    this.transition([ExecutionStatus.EM_REPARO], ExecutionStatus.FINALIZADA, now, { by });
    this.finishedAt = now;
  }

  fail(reason: string, now: Date, by?: string): void {
    this.transition(ACTIVE_STATUSES, ExecutionStatus.FALHOU, now, { by, note: reason });
    this.failureReason = reason;
    this.finishedAt = now;
  }

  cancel(reason: string, now: Date): void {
    this.transition(ACTIVE_STATUSES, ExecutionStatus.CANCELADA, now, { note: reason });
    this.failureReason = reason;
    this.finishedAt = now;
  }

  private transition(from: ExecutionStatus[], to: ExecutionStatus, now: Date, meta: { by?: string; note?: string }): void {
    if (!from.includes(this.status)) throw new InvalidTransitionError(this.status, to);
    this.history.push({
      at: now.toISOString(),
      from: this.status,
      to,
      ...(meta.by ? { by: meta.by } : {}),
      ...(meta.note ? { note: meta.note } : {}),
    });
    this.status = to;
    this.updatedAt = now;
  }
}

/** Soma quantidades repetidas da mesma peça (a OS pode listar a peça mais de uma vez). */
export function aggregateParts(parts: PartQuantity[]): PartQuantity[] {
  const totals = new Map<string, number>();
  for (const p of parts) {
    if (!p?.partId || !Number.isInteger(p.quantity) || p.quantity <= 0) continue;
    totals.set(p.partId, (totals.get(p.partId) ?? 0) + p.quantity);
  }
  return [...totals.entries()].map(([partId, quantity]) => ({ partId, quantity })).sort((a, b) => a.partId.localeCompare(b.partId));
}
