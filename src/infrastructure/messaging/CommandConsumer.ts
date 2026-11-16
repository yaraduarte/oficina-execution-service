import { MessageEnvelope } from '../../domain/messages';
import { traceMessage } from '../observability/tracing';
import { MAX_DELIVERY_ATTEMPTS } from './topology';

export const RETRY_HEADER = 'x-retry-count';

export interface IncomingMessage {
  content: Buffer;
  headers: Record<string, unknown>;
  routingKey: string;
}

export interface MessageAcks {
  ack(): void;
  deadLetter(): void;
  retry(headers: Record<string, unknown>): Promise<void>;
}

export interface CommandHandler {
  handle(envelope: MessageEnvelope): Promise<unknown>;
}

export interface ConsumerLogger {
  warn(msg: string, meta?: Record<string, unknown>): void;
  error(msg: string, meta?: Record<string, unknown>): void;
}

function isEnvelope(value: unknown): value is MessageEnvelope {
  const v = value as Partial<MessageEnvelope>;
  return Boolean(v && typeof v.messageId === 'string' && typeof v.type === 'string' && v.payload && typeof v.sagaId === 'string');
}

/**
 * Consumidor de execution.commands.
 * Malformado → DLQ direto; erro → até MAX_DELIVERY_ATTEMPTS tentativas, depois DLQ.
 * Os casos de uso são idempotentes, então reprocessar um comando é seguro.
 */
export class CommandConsumer {
  constructor(
    private readonly handler: CommandHandler,
    private readonly logger: ConsumerLogger = console,
  ) {}

  async onMessage(message: IncomingMessage, acks: MessageAcks): Promise<void> {
    let envelope: unknown;
    try {
      envelope = JSON.parse(message.content.toString('utf8'));
    } catch {
      this.logger.error('[ExecutionConsumer] JSON inválido enviado para a DLQ', { routingKey: message.routingKey });
      acks.deadLetter();
      return;
    }
    if (!isEnvelope(envelope)) {
      this.logger.error('[ExecutionConsumer] Envelope malformado enviado para a DLQ', { routingKey: message.routingKey });
      acks.deadLetter();
      return;
    }

    const attempt = Number(message.headers[RETRY_HEADER] ?? 0);
    try {
      await traceMessage(
        `Execution/${envelope.type}`,
        message.headers,
        { sagaId: envelope.sagaId, osId: envelope.osId, messageId: envelope.messageId },
        () => this.handler.handle(envelope as MessageEnvelope),
      );
      acks.ack();
    } catch (err) {
      const error = err instanceof Error ? err.message : String(err);
      if (attempt + 1 >= MAX_DELIVERY_ATTEMPTS) {
        this.logger.error('[ExecutionConsumer] Tentativas esgotadas, enviado para a DLQ', { messageId: envelope.messageId, type: envelope.type, error });
        acks.deadLetter();
        return;
      }
      this.logger.warn('[ExecutionConsumer] Falha ao processar, nova tentativa', { messageId: envelope.messageId, attempt: attempt + 1, error });
      await acks.retry({ ...message.headers, [RETRY_HEADER]: attempt + 1 });
    }
  }
}
