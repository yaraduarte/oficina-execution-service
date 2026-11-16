import { EventEmitter } from 'events';
import { CommandConsumer, IncomingMessage, MessageAcks, RETRY_HEADER } from '../infrastructure/messaging/CommandConsumer';
import { RabbitMQClient } from '../infrastructure/messaging/RabbitMQClient';
import { assertExecutionTopology, EXECUTION_COMMANDS_QUEUE } from '../infrastructure/messaging/topology';
import { OutboxRelay } from '../infrastructure/messaging/OutboxRelay';
import { getNewRelic, outgoingTraceHeaders, setNewRelicForTesting, traceMessage } from '../infrastructure/observability/tracing';
import { CommandDispatcher } from '../application/CommandDispatcher';
import { loadConfig } from '../config';
import { command, iniciarExecucao, makeHarness } from './support/fakes';

const quiet = { info: jest.fn(), warn: jest.fn(), error: jest.fn() };

describe('Mensageria', () => {
  const incoming = (content: string | object, headers: Record<string, unknown> = {}): IncomingMessage => ({
    content: Buffer.from(typeof content === 'string' ? content : JSON.stringify(content)),
    headers,
    routingKey: 'execution',
  });
  const acks = (): jest.Mocked<MessageAcks> => ({ ack: jest.fn(), deadLetter: jest.fn(), retry: jest.fn().mockResolvedValue(undefined) });

  it('consumidor: ack, DLQ para malformados, retry e DLQ após esgotar', async () => {
    const handler = { handle: jest.fn().mockResolvedValueOnce(undefined).mockRejectedValueOnce(new Error('x')).mockRejectedValueOnce('y') };
    const consumer = new CommandConsumer(handler, quiet);

    let a = acks();
    await consumer.onMessage(incoming(iniciarExecucao()), a);
    expect(a.ack).toHaveBeenCalled();

    a = acks();
    await consumer.onMessage(incoming('{x'), a);
    expect(a.deadLetter).toHaveBeenCalled();
    a = acks();
    await consumer.onMessage(incoming({ type: 'x' }), a);
    expect(a.deadLetter).toHaveBeenCalled();

    a = acks();
    await consumer.onMessage(incoming(iniciarExecucao(), { h: 1 }), a);
    expect(a.retry).toHaveBeenCalledWith({ h: 1, [RETRY_HEADER]: 1 });

    a = acks();
    await consumer.onMessage(incoming(iniciarExecucao(), { [RETRY_HEADER]: 2 }), a);
    expect(a.deadLetter).toHaveBeenCalled();
    expect(new CommandConsumer(handler)).toBeDefined();
  });

  it('dispatcher encaminha cada comando ao caso de uso', async () => {
    const h = makeHarness();
    h.store.seed('p1', 5);
    const logger = { info: jest.fn(), warn: jest.fn() };
    const d = new CommandDispatcher(h.executions, logger);
    await d.handle(iniciarExecucao());
    const executionId = h.store.outbox[0].payload.executionId as string;
    await d.handle(command('execution.CancelarExecucao', { executionId }));
    await d.handle(command('execution.Outro', {}));
    expect(h.store.types()).toEqual(['execution.ExecucaoIniciada', 'execution.ExecucaoCancelada']);
    expect(logger.warn).toHaveBeenCalledWith('Comando desconhecido ignorado', expect.any(Object));
    expect(new CommandDispatcher(h.executions)).toBeDefined();
  });

  it('topologia: fila execution.commands ligada à exchange de comandos com DLX', async () => {
    const ch = { assertExchange: jest.fn(), assertQueue: jest.fn(), bindQueue: jest.fn() };
    await assertExecutionTopology(ch);
    expect(ch.assertQueue).toHaveBeenCalledWith(EXECUTION_COMMANDS_QUEUE, { durable: true, arguments: { 'x-dead-letter-exchange': 'oficina.dlx' } });
    expect(ch.bindQueue).toHaveBeenCalledWith(EXECUTION_COMMANDS_QUEUE, 'oficina.commands', 'execution');
    expect(ch.bindQueue).toHaveBeenCalledWith('execution.commands.dlq', 'oficina.dlx', '');
  });

  function fakeChannel() {
    return {
      assertExchange: jest.fn(), assertQueue: jest.fn(), bindQueue: jest.fn(), prefetch: jest.fn(), consume: jest.fn(),
      ack: jest.fn(), nack: jest.fn(), close: jest.fn().mockResolvedValue(undefined),
      publish: jest.fn((_e, _r, _c, _o, cb: (err?: Error) => void) => cb()),
    };
  }
  function fakeConnection(channel: ReturnType<typeof fakeChannel>) {
    const conn = new EventEmitter() as EventEmitter & Record<string, jest.Mock>;
    conn.createConfirmChannel = jest.fn().mockResolvedValue(channel);
    conn.close = jest.fn().mockResolvedValue(undefined);
    return conn;
  }

  it('RabbitMQClient: reconecta, publica evento na exchange de eventos e consome', async () => {
    const channel = fakeChannel();
    const conn = fakeConnection(channel);
    const client = new RabbitMQClient('amqp://x', jest.fn().mockRejectedValueOnce(new Error('down')).mockResolvedValue(conn) as never, quiet);
    expect(client.isConnected).toBe(false);
    await client.connect(3, 1);
    expect(client.isConnected).toBe(true);

    const evt = { ...iniciarExecucao(), type: 'execution.ExecucaoIniciada' };
    await client.publish(evt);
    expect(channel.publish.mock.calls[0].slice(0, 2)).toEqual(['oficina.events', 'execution.ExecucaoIniciada']);
    expect(channel.publish.mock.calls[0][3]).toMatchObject({ persistent: true, messageId: evt.messageId });

    const handler = jest.fn(async (_m: IncomingMessage, a: MessageAcks) => {
      a.ack();
      a.deadLetter();
      await a.retry({ [RETRY_HEADER]: 1 });
    });
    await client.consume(EXECUTION_COMMANDS_QUEUE, handler, 4);
    const onRaw = channel.consume.mock.calls[0][1];
    onRaw(null);
    const raw = { content: Buffer.from(JSON.stringify(evt)), properties: {}, fields: { routingKey: 'execution', exchange: 'oficina.commands' } };
    onRaw(raw);
    await new Promise((r) => setImmediate(r));
    expect(channel.prefetch).toHaveBeenCalledWith(4);
    expect(channel.ack).toHaveBeenCalledTimes(2);
    expect(channel.nack).toHaveBeenCalledWith(raw, false, false);

    conn.emit('error', new Error('x'));
    conn.emit('close');
    await client.close();
    expect(client.isConnected).toBe(false);
  });

  it('RabbitMQClient: erros de conexão, publicação e handler', async () => {
    await expect(new RabbitMQClient('amqp://x', jest.fn().mockRejectedValue(new Error('fora')) as never, quiet).connect(1, 1)).rejects.toThrow('fora');
    await expect(new RabbitMQClient('amqp://x', undefined, quiet).publish(iniciarExecucao())).rejects.toThrow('não conectado');
    await expect(new RabbitMQClient('amqp://x', undefined, quiet).close()).resolves.toBeUndefined();
    expect(new RabbitMQClient('amqp://x')).toBeDefined();

    const channel = fakeChannel();
    channel.publish.mockImplementation((_e, _r, _c, _o, cb: (err?: Error) => void) => cb(new Error('nack')));
    const client = new RabbitMQClient('amqp://x', jest.fn().mockResolvedValue(fakeConnection(channel)) as never, quiet);
    await client.connect();
    await expect(client.publish(iniciarExecucao())).rejects.toThrow('nack');

    await client.consume('q', async () => {
      throw new Error('boom');
    });
    const raw = { content: Buffer.from('{}'), properties: { headers: {} }, fields: { routingKey: 'k', exchange: 'e' } };
    channel.consume.mock.calls[0][1](raw);
    await new Promise((r) => setImmediate(r));
    expect(channel.nack).toHaveBeenCalledWith(raw, false, false);
  });

  it('OutboxRelay: publica pendentes, não sobrepõe ciclos, registra falhas e para', async () => {
    let release!: () => void;
    const source = {
      relayOutbox: jest
        .fn()
        .mockImplementationOnce(() => new Promise<number>((r) => (release = () => r(2))))
        .mockRejectedValueOnce(new Error('db'))
        .mockResolvedValue(0),
    };
    const logger = { warn: jest.fn() };
    const relay = new OutboxRelay(source, jest.fn(), 5, logger);
    const first = relay.tick();
    expect(await relay.tick()).toBe(0); // ciclo em andamento
    release();
    expect(await first).toBe(2);
    expect(await relay.tick()).toBe(0);
    expect(logger.warn).toHaveBeenCalledWith('[Outbox] Falha ao publicar eventos pendentes', { error: 'db' });

    source.relayOutbox.mockRejectedValueOnce('texto');
    await relay.tick();
    expect(logger.warn).toHaveBeenLastCalledWith('[Outbox] Falha ao publicar eventos pendentes', { error: 'texto' });

    relay.start();
    relay.start();
    const before = source.relayOutbox.mock.calls.length;
    await new Promise((r) => setTimeout(r, 60));
    relay.stop();
    relay.stop();
    expect(source.relayOutbox.mock.calls.length).toBeGreaterThan(before);
    expect(new OutboxRelay(source, jest.fn())).toBeDefined();
  });
});

describe('tracing', () => {
  it('sem licença executa direto; com agente propaga o trace', async () => {
    const saved = process.env.NEW_RELIC_LICENSE_KEY;
    delete process.env.NEW_RELIC_LICENSE_KEY;
    setNewRelicForTesting(undefined);
    expect(getNewRelic()).toBeNull();
    expect(outgoingTraceHeaders()).toEqual({});
    expect(await traceMessage('x', {}, {}, async () => 1)).toBe(1);

    const tx = { insertDistributedTraceHeaders: jest.fn((h: Record<string, unknown>) => (h.traceparent = 't')), acceptDistributedTraceHeaders: jest.fn() };
    const api = {
      getTransaction: () => tx,
      startBackgroundTransaction: jest.fn((_n: string, _g: string, fn: () => unknown) => fn()) as unknown as <T>(n: string, g: string, fn: () => T) => T,
      addCustomAttributes: jest.fn(),
      noticeError: jest.fn(),
    };
    setNewRelicForTesting(api);
    expect(outgoingTraceHeaders()).toEqual({ traceparent: 't' });
    expect(await traceMessage('x', { traceparent: 'in' }, { a: 'b' }, async () => 2)).toBe(2);
    await expect(traceMessage('x', {}, {}, async () => Promise.reject(new Error('e')))).rejects.toThrow('e');
    expect(api.noticeError).toHaveBeenCalled();
    setNewRelicForTesting(undefined);
    process.env.NEW_RELIC_LICENSE_KEY = saved;
  });

  it('com licença e sem agente instalado degrada para nulo', () => {
    process.env.NEW_RELIC_LICENSE_KEY = 'fake';
    jest.isolateModules(() => {
      jest.doMock('newrelic', () => {
        throw new Error('sem agente');
      });
      expect(jest.requireActual('../infrastructure/observability/tracing').getNewRelic()).toBeNull();
    });
    delete process.env.NEW_RELIC_LICENSE_KEY;
  });

  it('com licença e agente instalado usa o agente', () => {
    process.env.NEW_RELIC_LICENSE_KEY = 'fake';
    jest.isolateModules(() => {
      jest.doMock('newrelic', () => ({ marker: true }));
      expect(jest.requireActual('../infrastructure/observability/tracing').getNewRelic()).toEqual({ marker: true });
    });
    delete process.env.NEW_RELIC_LICENSE_KEY;
  });
});

describe('config', () => {
  it('valores padrão em desenvolvimento', () => {
    const c = loadConfig({ NODE_ENV: 'development' });
    expect(c).toMatchObject({ port: 3000, isProduction: false, jwtSecret: 'dev-secret', outboxIntervalMs: 1000 });
    expect(c.database).toEqual({ host: 'localhost', port: 5432, database: 'execution_db', user: 'execution', password: 'execution123', ssl: false });
    expect(c.rabbitUrl).toBeUndefined();
  });

  it('em produção exige segredos e lê todas as variáveis', () => {
    expect(() => loadConfig({ NODE_ENV: 'production' })).toThrow('JWT_SECRET');
    const c = loadConfig({
      NODE_ENV: 'production', PORT: '8080', JWT_SECRET: 'j', ORIGIN_VERIFY_SECRET: 'o', RABBITMQ_URL: 'amqp://r', OUTBOX_INTERVAL_MS: '500',
      DB_HOST: 'h', DB_PORT: '6543', DB_NAME: 'n', DB_USER: 'u', DB_PASS: 'p', DB_SSL: 'true',
    });
    expect(c).toMatchObject({ port: 8080, isProduction: true, originVerifySecret: 'o', rabbitUrl: 'amqp://r', outboxIntervalMs: 500 });
    expect(c.database).toEqual({ host: 'h', port: 6543, database: 'n', user: 'u', password: 'p', ssl: true });
  });
});
