import { Config } from './config';
import { ExecutionService } from './application/ExecutionService';
import { CommandDispatcher } from './application/CommandDispatcher';
import { Logger } from './application/ports';
import { createPool, PostgresExecutionStore } from './infrastructure/postgres/PostgresExecutionStore';
import { RabbitMQClient } from './infrastructure/messaging/RabbitMQClient';
import { CommandConsumer } from './infrastructure/messaging/CommandConsumer';
import { OutboxRelay } from './infrastructure/messaging/OutboxRelay';
import { EXECUTION_COMMANDS_QUEUE } from './infrastructure/messaging/topology';
import { createApp } from './infrastructure/web/app';

const logger: Logger = {
  info: (msg, meta) => console.log(`[Execution] ${msg}`, meta ? JSON.stringify(meta) : ''),
  warn: (msg, meta) => console.warn(`[Execution] ${msg}`, meta ? JSON.stringify(meta) : ''),
};

export interface ExecutionRuntime {
  app: ReturnType<typeof createApp>;
  stop(): Promise<void>;
}

export interface BootstrapDeps {
  store?: PostgresExecutionStore;
  rabbit?: RabbitMQClient;
}

/** Composição do serviço: PostgreSQL (execution_db), outbox, RabbitMQ e HTTP. */
export async function bootstrap(config: Config, deps: BootstrapDeps = {}): Promise<ExecutionRuntime> {
  const store = deps.store ?? new PostgresExecutionStore(createPool(config.database));
  await store.migrate();
  logger.info('Esquema do execution_db verificado');

  const executions = new ExecutionService(store, { logger });

  const rabbit = deps.rabbit ?? (config.rabbitUrl ? new RabbitMQClient(config.rabbitUrl) : undefined);
  let relay: OutboxRelay | undefined;
  if (rabbit) {
    await rabbit.connect();
    const consumer = new CommandConsumer(new CommandDispatcher(executions, logger));
    await rabbit.consume(EXECUTION_COMMANDS_QUEUE, (msg, acks) => consumer.onMessage(msg, acks));
    logger.info(`Consumindo ${EXECUTION_COMMANDS_QUEUE}`);
    relay = new OutboxRelay(store, (envelope) => rabbit.publish(envelope), config.outboxIntervalMs);
    relay.start();
  } else {
    logger.warn('RABBITMQ_URL não definida: comandos da Saga não serão consumidos e eventos ficam no outbox');
  }

  const app = createApp({
    executions,
    jwtSecret: config.jwtSecret,
    originVerifySecret: config.originVerifySecret,
    isReady: async () => (rabbit ? rabbit.isConnected : true) && (await store.ping()),
  });

  return {
    app,
    stop: async () => {
      relay?.stop();
      await rabbit?.close();
      await store.close();
    },
  };
}
