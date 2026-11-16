import { CancelarExecucaoPayload, Commands, IniciarExecucaoPayload, MessageEnvelope } from '../domain/messages';
import { ExecutionService } from './ExecutionService';
import { Logger, silentLogger } from './ports';

/** Encaminha os comandos recebidos do orquestrador para o caso de uso correspondente. */
export class CommandDispatcher {
  constructor(
    private readonly executions: ExecutionService,
    private readonly logger: Logger = silentLogger,
  ) {}

  async handle(envelope: MessageEnvelope): Promise<void> {
    switch (envelope.type) {
      case Commands.INICIAR_EXECUCAO:
        await this.executions.startExecution(envelope as unknown as MessageEnvelope<IniciarExecucaoPayload>);
        return;
      case Commands.CANCELAR_EXECUCAO:
        await this.executions.cancelExecution(envelope as MessageEnvelope<CancelarExecucaoPayload>);
        return;
      default:
        this.logger.warn('Comando desconhecido ignorado', { type: envelope.type, messageId: envelope.messageId });
    }
  }
}
