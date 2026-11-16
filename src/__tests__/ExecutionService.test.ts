import { aggregateParts, Execution, ExecutionStatus, InvalidTransitionError, normalizePriority } from '../domain/Execution';
import { ExecutionService, findMissingParts } from '../application/ExecutionService';
import { Commands, Events } from '../domain/messages';
import { admin, command, iniciarExecucao, makeHarness, mechanic, InMemoryExecutionStore } from './support/fakes';

describe('Execution (domínio)', () => {
  const base = () =>
    Execution.queue({ id: 'e1', osId: 'os', sagaId: 's', quoteId: null, priority: 'NORMAL', services: [], parts: [], now: new Date(0) });

  it('segue fila → diagnóstico → reparo → finalizada registrando histórico', () => {
    const e = base();
    expect(e.holdsReservation).toBe(true);
    e.startDiagnosis('m1', new Date(1));
    e.startRepair(new Date(2), 'm1');
    e.finish(new Date(3));
    expect(e.status).toBe(ExecutionStatus.FINALIZADA);
    expect(e.isTerminal).toBe(true);
    expect(e.history.map((h) => h.to)).toEqual(['EM_FILA', 'EM_DIAGNOSTICO', 'EM_REPARO', 'FINALIZADA']);
    expect(e.history[1]).toMatchObject({ by: 'm1' });
    expect(e.mechanicId).toBe('m1');
  });

  it('recusa transições inválidas', () => {
    const e = base();
    expect(() => e.finish(new Date())).toThrow(InvalidTransitionError);
    expect(() => e.startRepair(new Date())).toThrow('EM_FILA → EM_REPARO');
    e.cancel('saga', new Date());
    expect(() => e.fail('x', new Date())).toThrow(InvalidTransitionError);
  });

  it('normaliza prioridade e agrega peças repetidas ignorando inválidas', () => {
    expect(normalizePriority('alta')).toBe('ALTA');
    expect(normalizePriority(undefined)).toBe('NORMAL');
    expect(normalizePriority('urgente')).toBe('NORMAL');
    expect(
      aggregateParts([
        { partId: 'b', quantity: 1 },
        { partId: 'a', quantity: 2 },
        { partId: 'b', quantity: 3 },
        { partId: '', quantity: 1 },
        { partId: 'c', quantity: 0 },
        { partId: 'd', quantity: 1.5 },
      ]),
    ).toEqual([
      { partId: 'a', quantity: 2 },
      { partId: 'b', quantity: 4 },
    ]);
  });
});

describe('ExecutionService — comandos da Saga', () => {
  it('IniciarExecucao reserva peças, enfileira e publica ExecucaoIniciada', async () => {
    const h = makeHarness();
    h.store.seed('p1', 5);
    const cmd = iniciarExecucao();
    const e = await h.executions.startExecution(cmd);

    expect(e.status).toBe(ExecutionStatus.EM_FILA);
    expect(h.store.stock.get('p1')).toMatchObject({ available: 3, reserved: 2 });
    expect(h.store.outbox).toHaveLength(1);
    expect(h.store.outbox[0]).toMatchObject({
      type: Events.EXECUCAO_INICIADA,
      sagaId: 'saga-1',
      osId: 'os-1',
      causationId: cmd.messageId,
      producer: 'execution-service',
      payload: { executionId: e.id, queuePosition: 1, reservedParts: [{ partId: 'p1', quantity: 2 }] },
    });
  });

  it('é idempotente por sagaId (reentrega não reserva de novo)', async () => {
    const h = makeHarness();
    h.store.seed('p1', 5);
    const first = await h.executions.startExecution(iniciarExecucao());
    const again = await h.executions.startExecution(iniciarExecucao());
    expect(again.id).toBe(first.id);
    expect(h.store.stock.get('p1')).toMatchObject({ available: 3, reserved: 2 });
    expect(h.store.outbox).toHaveLength(1);
  });

  it('sem estoque responde ExecucaoRecusada com as peças faltantes e não reserva nada', async () => {
    const h = makeHarness();
    h.store.seed('p1', 5);
    const e = await h.executions.startExecution(
      iniciarExecucao({}, [
        { partId: 'p1', quantity: 2 },
        { partId: 'p2', quantity: 1 },
      ]),
    );
    expect(e.status).toBe(ExecutionStatus.RECUSADA);
    expect(h.store.stock.get('p1')).toMatchObject({ available: 5, reserved: 0 });
    expect(h.store.outbox[0]).toMatchObject({
      type: Events.EXECUCAO_RECUSADA,
      payload: { reason: 'ESTOQUE_INSUFICIENTE', missingParts: [{ partId: 'p2', requested: 1, available: 0 }] },
    });
    // reentrega do mesmo comando continua recusada, sem novo evento
    await h.executions.startExecution(iniciarExecucao({}, [{ partId: 'p2', quantity: 1 }]));
    expect(h.store.outbox).toHaveLength(1);
  });

  it('calcula a posição na fila por prioridade; payload incompleto vira execução sem peças', async () => {
    const h = makeHarness();
    await h.executions.startExecution(command(Commands.INICIAR_EXECUCAO, { quoteId: null } as never, { sagaId: 'a' }));
    h.advance();
    await h.executions.startExecution(command(Commands.INICIAR_EXECUCAO, { quoteId: 'q', priority: 'BAIXA', parts: [] }, { sagaId: 'b' }));
    h.advance();
    await h.executions.startExecution(command(Commands.INICIAR_EXECUCAO, { quoteId: 'q', priority: 'ALTA' }, { sagaId: 'c' }));
    await h.executions.startExecution(command(Commands.INICIAR_EXECUCAO, undefined as never, { sagaId: 'd' }));
    const positions = h.store.outbox.map((m) => m.payload.queuePosition);
    expect(positions).toEqual([1, 2, 1, 3]);
    expect((await h.executions.queue()).map((e) => e.sagaId)).toEqual(['c', 'a', 'd', 'b']);
  });

  it('CancelarExecucao libera as peças e é idempotente', async () => {
    const h = makeHarness();
    h.store.seed('p1', 5);
    const e = await h.executions.startExecution(iniciarExecucao());
    const cancel = command(Commands.CANCELAR_EXECUCAO, { executionId: e.id, reason: 'compensação' });

    const cancelled = await h.executions.cancelExecution(cancel);
    expect(cancelled?.status).toBe(ExecutionStatus.CANCELADA);
    expect(h.store.stock.get('p1')).toMatchObject({ available: 5, reserved: 0 });
    expect(h.store.types()).toEqual([Events.EXECUCAO_INICIADA, Events.EXECUCAO_CANCELADA]);

    await h.executions.cancelExecution(cancel);
    expect(h.store.outbox).toHaveLength(2);
  });

  it('CancelarExecucao ignora comando sem id, execução inexistente ou já encerrada', async () => {
    const h = makeHarness();
    expect(await h.executions.cancelExecution(command(Commands.CANCELAR_EXECUCAO, {}))).toBeNull();
    expect(await h.executions.cancelExecution(command(Commands.CANCELAR_EXECUCAO, { executionId: 'nope' }))).toBeNull();

    h.store.seed('p1', 5);
    const e = await h.executions.startExecution(iniciarExecucao());
    await h.executions.startDiagnosis(e.id, mechanic);
    await h.executions.startRepair(e.id, mechanic);
    await h.executions.finish(e.id, mechanic);
    const r = await h.executions.cancelExecution(command(Commands.CANCELAR_EXECUCAO, { executionId: e.id }));
    expect(r?.status).toBe(ExecutionStatus.FINALIZADA);
    expect(h.store.types()).not.toContain(Events.EXECUCAO_CANCELADA);
  });
});

describe('ExecutionService — operação da oficina', () => {
  async function queued() {
    const h = makeHarness();
    h.store.seed('p1', 5);
    const e = await h.executions.startExecution(iniciarExecucao());
    return { h, e };
  }

  it('diagnóstico → reparo → finalizar publica os eventos e dá baixa no reservado', async () => {
    const { h, e } = await queued();
    await h.executions.startDiagnosis(e.id, mechanic);
    await h.executions.startRepair(e.id, mechanic);
    const done = await h.executions.finish(e.id, mechanic);

    expect(done.status).toBe(ExecutionStatus.FINALIZADA);
    expect(h.store.stock.get('p1')).toMatchObject({ available: 3, reserved: 0 });
    expect(h.store.types()).toEqual([
      Events.EXECUCAO_INICIADA,
      Events.DIAGNOSTICO_INICIADO,
      Events.REPARO_INICIADO,
      Events.EXECUCAO_FINALIZADA,
    ]);
    const finished = h.store.outbox[3];
    expect(finished).toMatchObject({ causationId: null, correlationId: 'saga-1', payload: { executionId: e.id, consumedParts: [{ partId: 'p1', quantity: 2 }] } });
    expect(h.store.outbox[1].payload).toEqual({ executionId: e.id, mechanicId: 'mech-1' });
  });

  it('aceita mecânico explícito no diagnóstico', async () => {
    const { h, e } = await queued();
    const d = await h.executions.startDiagnosis(e.id, admin, 'mech-9');
    expect(d.mechanicId).toBe('mech-9');
  });

  it('falha devolve as peças e publica ExecucaoFalhou; exige motivo', async () => {
    const { h, e } = await queued();
    await expect(h.executions.fail(e.id, mechanic, '  ')).rejects.toMatchObject({ statusCode: 422 });
    const failed = await h.executions.fail(e.id, mechanic, 'Peça incompatível');
    expect(failed.status).toBe(ExecutionStatus.FALHOU);
    expect(h.store.stock.get('p1')).toMatchObject({ available: 5, reserved: 0 });
    expect(h.store.outbox.at(-1)).toMatchObject({ type: Events.EXECUCAO_FALHOU, payload: { reason: 'Peça incompatível' } });
  });

  it('transição inválida vira 409 e execução inexistente 404, sem publicar nada', async () => {
    const { h, e } = await queued();
    await expect(h.executions.finish(e.id, mechanic)).rejects.toMatchObject({ statusCode: 409 });
    await expect(h.executions.startRepair('nope', mechanic)).rejects.toMatchObject({ statusCode: 404 });
    await expect(h.executions.get('nope')).rejects.toMatchObject({ statusCode: 404 });
    expect(h.store.outbox).toHaveLength(1);
    expect((await h.executions.get(e.id)).id).toBe(e.id);
  });

  it('erro inesperado no caso de uso é propagado', async () => {
    const store = new InMemoryExecutionStore();
    const svc = new ExecutionService(store);
    store.seed('p1', 1);
    const e = await svc.startExecution(iniciarExecucao({}, [{ partId: 'p1', quantity: 1 }]));
    store.stock.delete('p1'); // estoque some entre a reserva e a finalização: baixa é ignorada
    await svc.startDiagnosis(e.id, mechanic);
    await svc.startRepair(e.id, mechanic);
    await expect(svc.finish(e.id, mechanic)).resolves.toMatchObject({ status: 'FINALIZADA' });
    jest.spyOn(store, 'transaction').mockRejectedValueOnce(new Error('db down'));
    await expect(svc.startDiagnosis(e.id, mechanic)).rejects.toThrow('db down');
  });

  it('falha com peça removida do estoque não quebra a liberação', async () => {
    const { h, e } = await queued();
    h.store.stock.delete('p1');
    await expect(h.executions.fail(e.id, mechanic, 'x')).resolves.toMatchObject({ status: 'FALHOU' });
  });

  it('estoque: só ADMIN ajusta, com validação; preserva reservado', async () => {
    const { h } = await queued();
    await expect(h.executions.setStock('p1', 10, null, mechanic)).rejects.toMatchObject({ statusCode: 403 });
    await expect(h.executions.setStock(' ', 10, null, admin)).rejects.toMatchObject({ statusCode: 422 });
    await expect(h.executions.setStock('p1', -1, null, admin)).rejects.toMatchObject({ statusCode: 422 });
    await expect(h.executions.setStock('p1', 1.5, null, admin)).rejects.toMatchObject({ statusCode: 422 });
    await expect(h.executions.setStock('p1', '3', null, admin)).rejects.toMatchObject({ statusCode: 422 });
    expect(await h.executions.setStock('p1', 10, ' Filtro ', admin)).toMatchObject({ available: 10, reserved: 2, description: 'Filtro' });
    expect(await h.executions.setStock('p9', 0, '', admin)).toMatchObject({ description: null });
    expect((await h.executions.listStock()).map((s) => s.partId)).toEqual(['p1', 'p9']);
    expect(await h.executions.list({ osId: 'os-1' })).toHaveLength(1);
  });

  it('findMissingParts compara pedido e disponível', () => {
    const stock = new Map([['a', { partId: 'a', description: null, available: 1, reserved: 0, updatedAt: new Date() }]]);
    expect(findMissingParts([{ partId: 'a', quantity: 2 }, { partId: 'b', quantity: 1 }], stock)).toEqual([
      { partId: 'a', requested: 2, available: 1 },
      { partId: 'b', requested: 1, available: 0 },
    ]);
  });
});
