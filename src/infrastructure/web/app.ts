import 'express-async-errors';
import express, { Router } from 'express';
import cors from 'cors';
import helmet from 'helmet';
import swaggerUi from 'swagger-ui-express';
import { ExecutionService } from '../../application/ExecutionService';
import { ExecutionStatus } from '../../domain/Execution';
import { AppError } from '../../shared/AppError';
import { errorHandler, originVerifyMiddleware, staffAuthMiddleware } from './middlewares';
import { buildSwaggerSpec } from './swagger';

export interface AppDependencies {
  executions: ExecutionService;
  jwtSecret: string;
  originVerifySecret?: string;
  isReady?: () => boolean | Promise<boolean>;
}

const first = (v: unknown): string | undefined => (Array.isArray(v) ? (v[0] as string) : (v as string | undefined));

function parseStatuses(raw: string | undefined): ExecutionStatus[] | undefined {
  if (!raw) return undefined;
  const all = Object.values(ExecutionStatus) as string[];
  const statuses = raw.split(',').map((s) => s.trim().toUpperCase()).filter(Boolean);
  const invalid = statuses.filter((s) => !all.includes(s));
  if (invalid.length) throw new AppError(`Status inválido: ${invalid.join(', ')}`, 422);
  return statuses as ExecutionStatus[];
}

export function createApp(deps: AppDependencies) {
  const app = express();

  app.use(cors());
  app.use(express.json({ limit: '100kb' }));
  app.use(originVerifyMiddleware(deps.originVerifySecret));

  app.use('/api-docs', swaggerUi.serve, swaggerUi.setup(buildSwaggerSpec()));
  app.use(helmet());

  app.get('/health', async (_req, res) => {
    const ready = deps.isReady ? await deps.isReady() : true;
    res.status(ready ? 200 : 503).json({
      status: ready ? 'ok' : 'degraded',
      service: 'execution-service',
      environment: process.env.APP_ENV ?? process.env.NODE_ENV ?? 'development',
      uptime: Math.floor(process.uptime()),
      timestamp: new Date().toISOString(),
    });
  });

  const api = Router();
  api.use(staffAuthMiddleware(deps.jwtSecret));

  /**
   * @swagger
   * /execucoes/fila:
   *   get:
   *     summary: Fila de trabalho da oficina (aguardando, em diagnóstico e em reparo)
   *     description: Ordenada por prioridade (ALTA, NORMAL, BAIXA) e ordem de chegada.
   *     tags: [Execuções]
   *     security: [{ bearerAuth: [] }]
   *     responses:
   *       200: { description: Execuções ativas }
   *       403: { description: Token de cliente }
   */
  api.get('/execucoes/fila', async (_req, res) => {
    res.json({ status: 'success', data: await deps.executions.queue() });
  });

  /**
   * @swagger
   * /execucoes:
   *   get:
   *     summary: Lista execuções (filtros por status e OS)
   *     tags: [Execuções]
   *     security: [{ bearerAuth: [] }]
   *     parameters:
   *       - { in: query, name: status, schema: { type: string, example: "EM_FILA,EM_REPARO" } }
   *       - { in: query, name: osId, schema: { type: string } }
   *     responses:
   *       200: { description: Lista de execuções }
   *       422: { description: Status inválido }
   */
  api.get('/execucoes', async (req, res) => {
    const data = await deps.executions.list({ status: parseStatuses(first(req.query.status)), osId: first(req.query.osId) });
    res.json({ status: 'success', data });
  });

  /**
   * @swagger
   * /execucoes/{id}:
   *   get:
   *     summary: Detalha uma execução, com histórico de status
   *     tags: [Execuções]
   *     security: [{ bearerAuth: [] }]
   *     parameters:
   *       - { in: path, name: id, required: true, schema: { type: string } }
   *     responses:
   *       200: { description: Execução }
   *       404: { description: Não encontrada }
   */
  api.get('/execucoes/:id', async (req, res) => {
    res.json({ status: 'success', data: await deps.executions.get(req.params.id) });
  });

  /**
   * @swagger
   * /execucoes/{id}/diagnostico:
   *   post:
   *     summary: Inicia o diagnóstico (EM_FILA → EM_DIAGNOSTICO; OS → EM_DIAGNOSTICO)
   *     tags: [Execuções]
   *     security: [{ bearerAuth: [] }]
   *     parameters:
   *       - { in: path, name: id, required: true, schema: { type: string } }
   *     requestBody:
   *       content:
   *         application/json:
   *           schema: { type: object, properties: { mechanicId: { type: string, description: "Padrão: usuário do token" } } }
   *     responses:
   *       200: { description: Diagnóstico iniciado (publica execution.DiagnosticoIniciado) }
   *       409: { description: Transição inválida }
   */
  api.post('/execucoes/:id/diagnostico', async (req, res) => {
    const mechanicId = typeof req.body?.mechanicId === 'string' && req.body.mechanicId ? req.body.mechanicId : undefined;
    res.json({ status: 'success', data: await deps.executions.startDiagnosis(req.params.id, req.requester!, mechanicId) });
  });

  /**
   * @swagger
   * /execucoes/{id}/reparo:
   *   post:
   *     summary: Inicia o reparo (EM_DIAGNOSTICO → EM_REPARO; OS → EM_EXECUCAO)
   *     tags: [Execuções]
   *     security: [{ bearerAuth: [] }]
   *     parameters:
   *       - { in: path, name: id, required: true, schema: { type: string } }
   *     responses:
   *       200: { description: Reparo iniciado (publica execution.ReparoIniciado) }
   *       409: { description: Transição inválida }
   */
  api.post('/execucoes/:id/reparo', async (req, res) => {
    res.json({ status: 'success', data: await deps.executions.startRepair(req.params.id, req.requester!) });
  });

  /**
   * @swagger
   * /execucoes/{id}/finalizar:
   *   post:
   *     summary: Finaliza a execução, dá baixa nas peças e comunica o os-service (OS → FINALIZADA)
   *     tags: [Execuções]
   *     security: [{ bearerAuth: [] }]
   *     parameters:
   *       - { in: path, name: id, required: true, schema: { type: string } }
   *     responses:
   *       200: { description: Finalizada (publica execution.ExecucaoFinalizada) }
   *       409: { description: Transição inválida }
   */
  api.post('/execucoes/:id/finalizar', async (req, res) => {
    res.json({ status: 'success', data: await deps.executions.finish(req.params.id, req.requester!) });
  });

  /**
   * @swagger
   * /execucoes/{id}/falha:
   *   post:
   *     summary: Registra falha no reparo; peças voltam ao estoque e a Saga compensa (estorno + cancelamento)
   *     tags: [Execuções]
   *     security: [{ bearerAuth: [] }]
   *     parameters:
   *       - { in: path, name: id, required: true, schema: { type: string } }
   *     requestBody:
   *       required: true
   *       content:
   *         application/json:
   *           schema: { type: object, required: [reason], properties: { reason: { type: string, example: "Peça incompatível com o veículo" } } }
   *     responses:
   *       200: { description: Falha registrada (publica execution.ExecucaoFalhou) }
   *       409: { description: Transição inválida }
   *       422: { description: Motivo obrigatório }
   */
  api.post('/execucoes/:id/falha', async (req, res) => {
    res.json({ status: 'success', data: await deps.executions.fail(req.params.id, req.requester!, req.body?.reason) });
  });

  /**
   * @swagger
   * /estoque:
   *   get:
   *     summary: Saldo de peças (disponível e reservado para execuções ativas)
   *     tags: [Estoque]
   *     security: [{ bearerAuth: [] }]
   *     responses:
   *       200: { description: Itens de estoque }
   */
  api.get('/estoque', async (_req, res) => {
    res.json({ status: 'success', data: await deps.executions.listStock() });
  });

  /**
   * @swagger
   * /estoque/{partId}:
   *   put:
   *     summary: Define o saldo disponível de uma peça (somente ADMIN)
   *     description: partId é o id da peça no catálogo do os-service.
   *     tags: [Estoque]
   *     security: [{ bearerAuth: [] }]
   *     parameters:
   *       - { in: path, name: partId, required: true, schema: { type: string } }
   *     requestBody:
   *       required: true
   *       content:
   *         application/json:
   *           schema:
   *             type: object
   *             required: [available]
   *             properties:
   *               available: { type: integer, minimum: 0, example: 10 }
   *               description: { type: string, example: "Pastilha de freio dianteira" }
   *     responses:
   *       200: { description: Saldo atualizado }
   *       403: { description: Não é ADMIN }
   *       422: { description: Quantidade inválida }
   */
  api.put('/estoque/:partId', async (req, res) => {
    const item = await deps.executions.setStock(req.params.partId, req.body?.available, req.body?.description, req.requester!);
    res.json({ status: 'success', data: item });
  });

  app.use(api);
  app.use(errorHandler);
  return app;
}
