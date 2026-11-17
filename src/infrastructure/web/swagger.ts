import path from 'path';
import swaggerJsdoc from 'swagger-jsdoc';

export function buildSwaggerSpec() {
  return swaggerJsdoc({
    definition: {
      openapi: '3.0.0',
      info: {
        title: 'Oficina Marimb0ndo — Execution Service',
        version: '1.0.1',
        description:
          'Fila de execução, diagnóstico, reparo e estoque de peças. Participa da Saga orquestrada pelo os-service: ' +
          'recebe comandos via RabbitMQ (execution.commands) e publica eventos execution.* na exchange oficina.events. ' +
          'Rotas restritas a funcionários da oficina (JWT do os-service).',
      },
      servers: [{ url: process.env.API_URL ?? '/execucao', description: 'Via API Gateway (/execucao)' }],
      components: {
        securitySchemes: { bearerAuth: { type: 'http', scheme: 'bearer', bearerFormat: 'JWT' } },
      },
      tags: [{ name: 'Execuções' }, { name: 'Estoque' }],
    },
    apis: [path.join(__dirname, 'app.ts'), path.join(__dirname, 'app.js')],
  });
}
