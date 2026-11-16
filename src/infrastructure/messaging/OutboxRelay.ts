import { MessageEnvelope } from '../../domain/messages';

export interface OutboxSource {
  relayOutbox(publish: (envelope: MessageEnvelope) => Promise<void>, limit?: number): Promise<number>;
}

export interface RelayLogger {
  warn(msg: string, meta?: Record<string, unknown>): void;
}

/**
 * Publica periodicamente os eventos gravados no outbox (padrão Transactional Outbox):
 * a mudança de estado e o evento são gravados na mesma transação, e o relay garante
 * a entrega ao RabbitMQ mesmo que o broker esteja fora no momento da operação.
 */
export class OutboxRelay {
  private timer: NodeJS.Timeout | null = null;
  private running = false;

  constructor(
    private readonly source: OutboxSource,
    private readonly publish: (envelope: MessageEnvelope) => Promise<void>,
    private readonly intervalMs = 1000,
    private readonly logger: RelayLogger = console,
  ) {}

  /** Um ciclo: publica o que estiver pendente. Ciclos não se sobrepõem. */
  async tick(): Promise<number> {
    if (this.running) return 0;
    this.running = true;
    try {
      return await this.source.relayOutbox(this.publish);
    } catch (err) {
      this.logger.warn('[Outbox] Falha ao publicar eventos pendentes', { error: err instanceof Error ? err.message : String(err) });
      return 0;
    } finally {
      this.running = false;
    }
  }

  start(): void {
    if (this.timer) return;
    this.timer = setInterval(() => void this.tick(), this.intervalMs);
    this.timer.unref();
  }

  stop(): void {
    if (this.timer) clearInterval(this.timer);
    this.timer = null;
  }
}
