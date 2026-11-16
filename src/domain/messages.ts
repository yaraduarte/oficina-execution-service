/**
 * Contratos de mensagens da Saga da OS (cópia local do contrato publicado em
 * oficinaMarimbondo/docs/fase4/EVENTOS.md — fonte da verdade).
 */

export const Exchanges = {
  COMMANDS: 'oficina.commands',
  EVENTS: 'oficina.events',
  DLX: 'oficina.dlx',
} as const;

export const EXECUTION_ROUTING_KEY = 'execution';
export const PRODUCER = 'execution-service';

export const Commands = {
  INICIAR_EXECUCAO: 'execution.IniciarExecucao',
  CANCELAR_EXECUCAO: 'execution.CancelarExecucao',
} as const;

export const Events = {
  EXECUCAO_INICIADA: 'execution.ExecucaoIniciada',
  EXECUCAO_RECUSADA: 'execution.ExecucaoRecusada',
  DIAGNOSTICO_INICIADO: 'execution.DiagnosticoIniciado',
  REPARO_INICIADO: 'execution.ReparoIniciado',
  EXECUCAO_FINALIZADA: 'execution.ExecucaoFinalizada',
  EXECUCAO_FALHOU: 'execution.ExecucaoFalhou',
  EXECUCAO_CANCELADA: 'execution.ExecucaoCancelada',
} as const;

export type EventType = (typeof Events)[keyof typeof Events];

export interface MessageEnvelope<P = Record<string, unknown>> {
  messageId: string;
  type: string;
  version: number;
  sagaId: string;
  correlationId: string;
  causationId: string | null;
  osId: string;
  occurredAt: string;
  producer: string;
  payload: P;
}

export interface PartQuantity {
  partId: string;
  quantity: number;
}

export interface ServiceRef {
  serviceId: string;
  description: string;
}

export interface IniciarExecucaoPayload {
  [key: string]: unknown;
  quoteId: string | null;
  priority?: string;
  services?: ServiceRef[];
  parts?: PartQuantity[];
}

export interface CancelarExecucaoPayload {
  [key: string]: unknown;
  executionId?: string;
  reason?: string;
}
