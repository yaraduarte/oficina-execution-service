import request from 'supertest';
import { bootstrap } from '../bootstrap';
import { loadConfig } from '../config';
import { createPool, PostgresExecutionStore } from '../infrastructure/postgres/PostgresExecutionStore';
import { RabbitMQClient } from '../infrastructure/messaging/RabbitMQClient';
import { testDbConfig } from './support/db';

jest.spyOn(console, 'log').mockImplementation(() => undefined);
jest.spyOn(console, 'warn').mockImplementation(() => undefined);

const config = () => ({ ...loadConfig({ NODE_ENV: 'test' }), database: testDbConfig, outboxIntervalMs: 5 });

describe('bootstrap', () => {
  it('sem RabbitMQ: migra o banco e sobe a API saudável', async () => {
    const runtime = await bootstrap(config());
    expect((await request(runtime.app).get('/health')).status).toBe(200);
    await runtime.stop();
  });

  it('com RabbitMQ: consome execution.commands e o relay publica o outbox', async () => {
    const rabbit = {
      connect: jest.fn().mockResolvedValue(undefined),
      consume: jest.fn().mockResolvedValue(undefined),
      publish: jest.fn().mockResolvedValue(undefined),
      close: jest.fn().mockResolvedValue(undefined),
      isConnected: false,
    } as unknown as RabbitMQClient;
    const store = new PostgresExecutionStore(createPool(testDbConfig));
    const relay = jest.spyOn(store, 'relayOutbox');

    const runtime = await bootstrap({ ...config(), rabbitUrl: 'amqp://x' }, { rabbit, store });
    expect(rabbit.connect).toHaveBeenCalled();
    expect((rabbit.consume as jest.Mock).mock.calls[0][0]).toBe('execution.commands');
    // o handler registrado encaminha a mensagem ao consumidor
    const onMessage = (rabbit.consume as jest.Mock).mock.calls[0][1];
    const acks = { ack: jest.fn(), deadLetter: jest.fn(), retry: jest.fn() };
    await onMessage({ content: Buffer.from('{x'), headers: {}, routingKey: 'execution' }, acks);
    expect(acks.deadLetter).toHaveBeenCalled();

    await new Promise((r) => setTimeout(r, 30));
    expect(relay).toHaveBeenCalled();
    await relay.mock.calls[0][0]({ type: 't' } as never);
    expect(rabbit.publish).toHaveBeenCalled();

    expect((await request(runtime.app).get('/health')).status).toBe(503); // broker desconectado
    await runtime.stop();
    expect(rabbit.close).toHaveBeenCalled();
  });
});
