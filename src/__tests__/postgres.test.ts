/**
 * Testes de integração com PostgreSQL real (docker local ou service container no CI).
 * Conexão: TEST_DB_HOST / TEST_DB_PORT / TEST_DB_NAME / TEST_DB_USER / TEST_DB_PASS.
 */
import { Pool } from 'pg';
import { createPool, PostgresExecutionStore } from '../infrastructure/postgres/PostgresExecutionStore';
import { ExecutionService } from '../application/ExecutionService';
import { Execution, ExecutionStatus } from '../domain/Execution';
import { Commands } from '../domain/messages';
import { admin, command, iniciarExecucao, mechanic } from './support/fakes';

import { testDbConfig } from "./support/db";

describe('PostgresExecutionStore (integração)', () => {
  let pool: Pool;
  let store: PostgresExecutionStore;
  let svc: ExecutionService;

  beforeAll(async () => {
    pool = createPool(testDbConfig);
    store = new PostgresExecutionStore(pool);
    await pool.query('DROP TABLE IF EXISTS executions, stock_items, outbox_messages');
    await store.migrate();
    await store.migrate(); // idempotente
    svc = new ExecutionService(store);
  });

  beforeEach(async () => {
    await pool.query('TRUNCATE executions, stock_items, outbox_messages');
  });

  afterAll(async () => {
    await store.close();
  });

  const outbox = async () => (await pool.query('SELECT type, envelope, published_at, attempts, last_error FROM outbox_messages ORDER BY created_at')).rows;

  it('fluxo completo: reserva, fila, diagnóstico, reparo e baixa, com eventos no outbox', async () => {
    await svc.setStock('p1', 5, 'Filtro de óleo', admin);
    const e = await svc.startExecution(iniciarExecucao({ sagaId: 'pg-1' }));
    expect((await store.listStock())[0]).toMatchObject({ partId: 'p1', available: 3, reserved: 2, description: 'Filtro de óleo' });

    await svc.startDiagnosis(e.id, mechanic);
    await svc.startRepair(e.id, mechanic);
    const done = await svc.finish(e.id, mechanic);
    expect(done.status).toBe(ExecutionStatus.FINALIZADA);

    const loaded = (await store.findById(e.id))!;
    expect(loaded).toBeInstanceOf(Execution);
    expect(loaded).toMatchObject({ status: 'FINALIZADA', mechanicId: 'mech-1', sagaId: 'pg-1', quoteId: 'q-1' });
    expect(loaded.history).toHaveLength(4);
    expect(loaded.diagnosisStartedAt).toBeInstanceOf(Date);
    expect(loaded.finishedAt).toBeInstanceOf(Date);
    expect((await store.listStock())[0]).toMatchObject({ available: 3, reserved: 0 });
    expect((await outbox()).map((r) => r.type)).toEqual([
      'execution.ExecucaoIniciada',
      'execution.DiagnosticoIniciado',
      'execution.ReparoIniciado',
      'execution.ExecucaoFinalizada',
    ]);
  });

  it('idempotência por sagaId, recusa por estoque e cancelamento', async () => {
    await svc.setStock('p1', 1, null, admin);
    const refused = await svc.startExecution(iniciarExecucao({ sagaId: 'pg-2' }));
    expect(refused.status).toBe(ExecutionStatus.RECUSADA);
    expect((await svc.startExecution(iniciarExecucao({ sagaId: 'pg-2' }))).id).toBe(refused.id);

    await svc.setStock('p1', 4, null, admin);
    const e = await svc.startExecution(iniciarExecucao({ sagaId: 'pg-3' }));
    await svc.cancelExecution(command(Commands.CANCELAR_EXECUCAO, { executionId: e.id }));
    expect((await store.listStock())[0]).toMatchObject({ available: 4, reserved: 0 });
    expect(await store.findById('não-é-uuid')).toBeNull();
    expect(await store.list({ osId: 'os-1', status: [ExecutionStatus.CANCELADA] })).toHaveLength(1);
    expect(await store.list()).toHaveLength(2);
  });

  it('fila por prioridade e posição', async () => {
    await svc.startExecution(command(Commands.INICIAR_EXECUCAO, { quoteId: null, priority: 'BAIXA' }, { sagaId: 'b' }));
    await svc.startExecution(command(Commands.INICIAR_EXECUCAO, { quoteId: null, priority: 'ALTA' }, { sagaId: 'a' }));
    expect((await svc.queue()).map((e) => e.sagaId)).toEqual(['a', 'b']);
    expect((await outbox()).map((r) => r.envelope.payload.queuePosition)).toEqual([1, 1]);
  });

  it('reservas concorrentes da mesma peça não ultrapassam o saldo (FOR UPDATE)', async () => {
    await svc.setStock('p1', 3, null, admin);
    const results = await Promise.all(
      [1, 2, 3].map((i) => svc.startExecution(iniciarExecucao({ sagaId: `c-${i}` }, [{ partId: 'p1', quantity: 2 }]))),
    );
    expect(results.filter((r) => r.status === ExecutionStatus.EM_FILA)).toHaveLength(1);
    expect(results.filter((r) => r.status === ExecutionStatus.RECUSADA)).toHaveLength(2);
    expect((await store.listStock())[0]).toMatchObject({ available: 1, reserved: 2 });
  });

  it('transação faz rollback em erro', async () => {
    await svc.setStock('p1', 3, null, admin);
    await expect(
      store.transaction(async (tx) => {
        const s = await tx.lockStock(['p1']);
        await tx.saveStock({ ...s.get('p1')!, available: 0 });
        throw new Error('falha no meio');
      }),
    ).rejects.toThrow('falha no meio');
    expect((await store.listStock())[0].available).toBe(3);
    expect(await store.transaction(async (tx) => tx.lockStock([]))).toEqual(new Map());
  });

  it('relay do outbox publica em ordem, marca publicados e para no primeiro erro', async () => {
    await svc.setStock('p1', 10, null, admin);
    await svc.startExecution(iniciarExecucao({ sagaId: 'r-1' }));
    await svc.startExecution(iniciarExecucao({ sagaId: 'r-2' }));
    await svc.startExecution(iniciarExecucao({ sagaId: 'r-3' }));

    const sent: string[] = [];
    const publish = jest.fn(async (env: { sagaId: string }) => {
      if (env.sagaId === 'r-2' && !sent.includes('r-2-falhou')) {
        sent.push('r-2-falhou');
        throw new Error('broker fora');
      }
      sent.push(env.sagaId);
    });
    expect(await store.relayOutbox(publish as never)).toBe(1);
    const rows = await outbox();
    expect(rows[0].published_at).not.toBeNull();
    expect(rows[1]).toMatchObject({ published_at: null, attempts: 1, last_error: 'broker fora' });

    const fail = jest.fn().mockRejectedValue('texto');
    await store.relayOutbox(fail);
    expect((await outbox())[1].last_error).toBe('texto');

    expect(await store.relayOutbox(publish as never)).toBe(2);
    expect(sent).toEqual(['r-1', 'r-2-falhou', 'r-2', 'r-3']);
    expect(await store.relayOutbox(publish as never)).toBe(0);
  });

  it('relay faz rollback quando o banco falha e ping indica saúde', async () => {
    expect(await store.ping()).toBe(true);
    const broken = new PostgresExecutionStore(createPool({ ...testDbConfig, ssl: true, port: 1 }));
    await expect(broken.relayOutbox(jest.fn())).rejects.toBeDefined();
    expect(await broken.ping()).toBe(false);
    await broken.close();

    const spy = jest.spyOn(pool, 'connect');
    const client = { query: jest.fn().mockResolvedValueOnce({}).mockRejectedValueOnce(new Error('select falhou')).mockResolvedValue({}), release: jest.fn() };
    spy.mockResolvedValueOnce(client as never);
    await expect(store.relayOutbox(jest.fn())).rejects.toThrow('select falhou');
    expect(client.query).toHaveBeenLastCalledWith('ROLLBACK');
    expect(client.release).toHaveBeenCalled();
    spy.mockRestore();
  });
});
