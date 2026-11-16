import request from 'supertest';
import jwt from 'jsonwebtoken';
import { createApp } from '../infrastructure/web/app';
import { errorHandler } from '../infrastructure/web/middlewares';
import { iniciarExecucao, makeHarness } from './support/fakes';

const SECRET = 'test-secret';
const token = (claims: Record<string, unknown>, opts: jwt.SignOptions = {}) =>
  jwt.sign({ sub: 'u1', ...claims }, SECRET, { issuer: 'oficina-marimb0ndo-os', audience: 'oficina-marimb0ndo-api', expiresIn: '1h', ...opts });
const mechanicToken = token({ type: 'staff', role: 'MECHANIC' });
const adminToken = token({ type: 'staff', role: 'ADMIN' });

async function setup(extra: Partial<Parameters<typeof createApp>[0]> = {}) {
  const h = makeHarness();
  h.store.seed('p1', 5);
  const e = await h.executions.startExecution(iniciarExecucao());
  const app = createApp({ executions: h.executions, jwtSecret: SECRET, ...extra });
  return { h, e, app };
}

describe('API do execution-service', () => {
  it('health e Swagger sem autenticação; readiness assíncrona', async () => {
    const { app } = await setup();
    expect((await request(app).get('/health')).body).toMatchObject({ status: 'ok', service: 'execution-service' });
    expect((await request(app).get('/api-docs/')).status).toBe(200);
    const degraded = createApp({ executions: makeHarness().executions, jwtSecret: SECRET, isReady: async () => false });
    expect((await request(degraded).get('/health')).status).toBe(503);
  });

  it('exige token de funcionário válido', async () => {
    const { app } = await setup();
    expect((await request(app).get('/execucoes')).status).toBe(401);
    expect((await request(app).get('/execucoes').set('Authorization', 'Bearer lixo')).status).toBe(401);
    const wrongAud = token({ type: 'staff' }, { audience: 'outra' });
    expect((await request(app).get('/execucoes').set('Authorization', `Bearer ${wrongAud}`)).status).toBe(401);
    const client = token({ type: 'client', status: 'ATIVO' }, { issuer: 'oficina-marimb0ndo-auth' });
    const res = await request(app).get('/execucoes').set('Authorization', `Bearer ${client}`);
    expect(res.status).toBe(403);
    expect(res.body.message).toMatch(/funcionários/);
  });

  it('fila, listagem com filtros e detalhe', async () => {
    const { app, e } = await setup();
    const auth = { Authorization: `Bearer ${mechanicToken}` };
    expect((await request(app).get('/execucoes/fila').set(auth)).body.data).toHaveLength(1);
    expect((await request(app).get('/execucoes?status=em_fila,EM_REPARO&osId=os-1').set(auth)).body.data).toHaveLength(1);
    expect((await request(app).get('/execucoes?status=FINALIZADA').set(auth)).body.data).toHaveLength(0);
    const bad = await request(app).get('/execucoes?status=XPTO').set(auth);
    expect(bad.status).toBe(422);
    expect((await request(app).get(`/execucoes/${e.id}`).set(auth)).body.data.id).toBe(e.id);
    expect((await request(app).get('/execucoes/nao-existe').set(auth)).status).toBe(404);
  });

  it('fluxo diagnóstico → reparo → finalizar e conflito de transição', async () => {
    const { app, e, h } = await setup();
    const auth = { Authorization: `Bearer ${mechanicToken}` };
    expect((await request(app).post(`/execucoes/${e.id}/reparo`).set(auth)).status).toBe(409);
    const d = await request(app).post(`/execucoes/${e.id}/diagnostico`).set(auth).send({ mechanicId: 'mech-7' });
    expect(d.body.data).toMatchObject({ status: 'EM_DIAGNOSTICO', mechanicId: 'mech-7' });
    expect((await request(app).post(`/execucoes/${e.id}/reparo`).set(auth)).body.data.status).toBe('EM_REPARO');
    expect((await request(app).post(`/execucoes/${e.id}/finalizar`).set(auth)).body.data.status).toBe('FINALIZADA');
    expect(h.store.types()).toContain('execution.ExecucaoFinalizada');
  });

  it('diagnóstico sem corpo usa o usuário do token; falha exige motivo', async () => {
    const { app, e } = await setup();
    const auth = { Authorization: `Bearer ${mechanicToken}` };
    expect((await request(app).post(`/execucoes/${e.id}/diagnostico`).set(auth)).body.data.mechanicId).toBe('u1');
    expect((await request(app).post(`/execucoes/${e.id}/falha`).set(auth).send({})).status).toBe(422);
    const f = await request(app).post(`/execucoes/${e.id}/falha`).set(auth).send({ reason: 'Motor travado' });
    expect(f.body.data).toMatchObject({ status: 'FALHOU', failureReason: 'Motor travado' });
  });

  it('estoque: leitura para funcionários e ajuste só para ADMIN', async () => {
    const { app } = await setup();
    expect((await request(app).get('/estoque').set('Authorization', `Bearer ${mechanicToken}`)).body.data[0]).toMatchObject({ partId: 'p1', reserved: 2 });
    expect((await request(app).put('/estoque/p1').set('Authorization', `Bearer ${mechanicToken}`).send({ available: 9 })).status).toBe(403);
    const ok = await request(app).put('/estoque/p2').set('Authorization', `Bearer ${adminToken}`).send({ available: 4, description: 'Vela' });
    expect(ok.body.data).toMatchObject({ partId: 'p2', available: 4, description: 'Vela' });
  });

  it('JSON inválido → 400; x-origin-verify obrigatório quando configurado', async () => {
    const { app: open } = await setup();
    const bad = await request(open).put('/estoque/p1').set('Authorization', `Bearer ${adminToken}`).set('Content-Type', 'application/json').send('{x');
    expect(bad.status).toBe(400);

    const { app } = await setup({ originVerifySecret: 'origem' });
    expect((await request(app).get('/health')).status).toBe(200);
    expect((await request(app).get('/estoque').set('Authorization', `Bearer ${adminToken}`)).status).toBe(403);
    const ok = await request(app).get('/estoque').set('Authorization', `Bearer ${adminToken}`).set('x-origin-verify', 'origem');
    expect(ok.status).toBe(200);
  });

  it('erro não tratado vira 500 sem vazar detalhes', () => {
    const res = { status: jest.fn().mockReturnThis(), json: jest.fn() };
    const spy = jest.spyOn(console, 'error').mockImplementation(() => undefined);
    errorHandler(new Error('segredo'), {} as never, res as never, jest.fn());
    expect(res.status).toHaveBeenCalledWith(500);
    expect(res.json).toHaveBeenCalledWith({ status: 'error', message: 'Erro interno do servidor' });
    spy.mockRestore();
  });
});
