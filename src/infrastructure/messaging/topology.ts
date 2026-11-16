import { EXECUTION_ROUTING_KEY, Exchanges } from '../../domain/messages';

export const EXECUTION_COMMANDS_QUEUE = 'execution.commands';
export const EXECUTION_COMMANDS_DLQ = 'execution.commands.dlq';
export const MAX_DELIVERY_ATTEMPTS = 3;

export interface TopologyChannel {
  assertExchange(name: string, type: string, options?: object): Promise<unknown>;
  assertQueue(name: string, options?: object): Promise<unknown>;
  bindQueue(queue: string, exchange: string, pattern: string): Promise<unknown>;
}

/**
 * Topologia do execution-service (docs/fase4/EVENTOS.md): consome execution.commands
 * (exchange de comandos, routing key "execution") e publica eventos execution.* na exchange de eventos.
 */
export async function assertExecutionTopology(channel: TopologyChannel): Promise<void> {
  await channel.assertExchange(Exchanges.COMMANDS, 'direct', { durable: true });
  await channel.assertExchange(Exchanges.EVENTS, 'topic', { durable: true });
  await channel.assertExchange(Exchanges.DLX, 'fanout', { durable: true });

  await channel.assertQueue(EXECUTION_COMMANDS_DLQ, { durable: true });
  await channel.bindQueue(EXECUTION_COMMANDS_DLQ, Exchanges.DLX, '');

  await channel.assertQueue(EXECUTION_COMMANDS_QUEUE, {
    durable: true,
    arguments: { 'x-dead-letter-exchange': Exchanges.DLX },
  });
  await channel.bindQueue(EXECUTION_COMMANDS_QUEUE, Exchanges.COMMANDS, EXECUTION_ROUTING_KEY);
}
