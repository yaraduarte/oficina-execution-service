# oficina-execution-service

Microsserviço de **Execução e Produção** da Oficina Marimb0ndo (Tech Challenge FIAP — Fase 4).

É responsável por:
- gerenciar a **fila de execução** das OS pagas;
- atualizar o status durante o **diagnóstico** e o **reparo**;
- **comunicar a finalização** ao os-service;
- controlar o **estoque de peças**: reserva, baixa e devolução.

O serviço participa da **Saga orquestrada** pelo os-service. Recebe comandos pelo RabbitMQ e publica eventos `execution.*`. A visão geral, os contratos e as decisões estão em [oficinaMarimbondo/docs/fase4](https://github.com/yaraduarte/oficinaMarimbondo/tree/main/docs/fase4):
- ADR-005: Saga;
- ADR-006: RabbitMQ;
- ADR-007: banco por serviço.

## Tecnologias

| Tecnologia | Uso |
|---|---|
| Node.js 22 + TypeScript + Express | API REST |
| PostgreSQL 15 (`execution_db`, usuário próprio na instância RDS) | Fila de execução, estoque e outbox |
| RabbitMQ (`amqplib`) | Comandos e eventos da Saga |
| Transactional Outbox | O evento é gravado na mesma transação da mudança de estado e o relay publica depois |
| New Relic | APM e distributed tracing propagado nos headers AMQP |
| Jest + Supertest + PostgreSQL real no CI | Testes unitários e de integração (cobertura mínima de 80%) |
| SonarQube (efêmero no CI) | Quality gate a cada PR |
| Docker + k3s (Kubernetes) | Deploy automatizado |

## Arquitetura

```
src/
├── domain/            Execution (máquina de estados), StockItem, contratos de mensagens
├── application/       ExecutionService (casos de uso), CommandDispatcher, ports
└── infrastructure/
    ├── postgres/      PostgresExecutionStore (transações, FOR UPDATE, outbox com SKIP LOCKED)
    ├── messaging/     RabbitMQClient, CommandConsumer (retry + DLQ), OutboxRelay, topologia
    ├── observability/ tracing New Relic
    └── web/           Express: execuções, estoque, Swagger, JWT de funcionário, x-origin-verify
```

### Por que PostgreSQL

A reserva de estoque precisa de consistência forte. Duas OS pagas ao mesmo tempo não podem reservar a mesma peça. O serviço bloqueia as linhas de estoque com `SELECT ... FOR UPDATE`, sempre em ordem de `part_id` para evitar deadlock. Isso é testado com reservas concorrentes em `src/__tests__/postgres.test.ts`.

O `execution_db` fica na mesma instância RDS do os-service, mas é um database separado, com usuário próprio e `CONNECT` revogado de `PUBLIC`. Nenhum outro serviço tem a connection string.

### Por que outbox

O orquestrador só avança quando recebe o evento. Se o serviço gravasse no banco e o RabbitMQ estivesse fora, o evento se perderia. Com o outbox, o evento é gravado na mesma transação da mudança de estado. O `OutboxRelay` publica os pendentes a cada segundo, em ordem e com `FOR UPDATE SKIP LOCKED`, o que funciona com várias réplicas.

## Participação na Saga

| Comando recebido (`execution.commands`) | O que faz | Evento publicado |
|---|---|---|
| `execution.IniciarExecucao` | Reserva as peças e coloca a OS na fila, por prioridade e ordem de chegada | `ExecucaoIniciada` `{ executionId, queuePosition, reservedParts }` |
| (mesmo comando, sem estoque) | Não reserva nada | `ExecucaoRecusada` `{ reason: "ESTOQUE_INSUFICIENTE", missingParts }`, e a Saga **compensa** com estorno e cancelamento do orçamento |
| `execution.CancelarExecucao` | Libera as peças reservadas | `ExecucaoCancelada` |

| Ação na oficina (REST) | Evento | Efeito no os-service |
|---|---|---|
| `POST /execucoes/{id}/diagnostico` | `DiagnosticoIniciado` | OS → `EM_DIAGNOSTICO` |
| `POST /execucoes/{id}/reparo` | `ReparoIniciado` | OS → `EM_EXECUCAO` |
| `POST /execucoes/{id}/finalizar` | `ExecucaoFinalizada`, com baixa das peças | OS → `FINALIZADA`, e a Saga termina como `CONCLUIDA` |
| `POST /execucoes/{id}/falha` | `ExecucaoFalhou`, com as peças devolvidas ao estoque | A Saga **compensa** com estorno do pagamento e cancelamento do orçamento |

Garantias do processamento:
- **Idempotência:** `IniciarExecucao` é idempotente por `sagaId`. A recusa também é gravada (status `RECUSADA`), então uma reentrega não reavalia o estoque.
- **Retry e DLQ:** o consumidor tenta até 3 vezes e depois manda para `execution.commands.dlq`.

Máquina de estados: `EM_FILA → EM_DIAGNOSTICO → EM_REPARO → FINALIZADA`. A partir de qualquer estado ativo, a execução pode ir para `FALHOU` ou `CANCELADA`.

## API (via API Gateway em `/execucao`)

Todas as rotas exigem **JWT de funcionário** (login do os-service). Token de cliente recebe `403`.

| Método | Rota | Perfil | Descrição |
|---|---|---|---|
| GET | `/health` | público | Saúde do serviço (banco + broker) |
| GET | `/execucoes/fila` | funcionário | Fila de trabalho (aguardando, em diagnóstico, em reparo) |
| GET | `/execucoes?status=&osId=` | funcionário | Lista com filtros |
| GET | `/execucoes/{id}` | funcionário | Detalhe com histórico |
| POST | `/execucoes/{id}/diagnostico` | funcionário | Inicia o diagnóstico (`mechanicId` opcional) |
| POST | `/execucoes/{id}/reparo` | funcionário | Inicia o reparo |
| POST | `/execucoes/{id}/finalizar` | funcionário | Finaliza e dá baixa nas peças |
| POST | `/execucoes/{id}/falha` | funcionário | Registra a falha (`reason` obrigatório) |
| GET | `/estoque` | funcionário | Saldos disponível e reservado |
| PUT | `/estoque/{partId}` | ADMIN | Define o saldo de uma peça do catálogo do os-service |

O Swagger fica em `/api-docs`; pelo gateway, em `https://<api-gateway>/execucao/api-docs`.

## Executar localmente

```bash
cp .env.example .env
docker compose up -d execution-db rabbitmq
npm install
npm run dev            # http://localhost:3002/api-docs
```

Ou tudo em containers: `docker compose up --build`.

## Testes e cobertura

```bash
docker compose up -d execution-db            # PostgreSQL para os testes de integração
TEST_DB_PORT=5433 TEST_DB_NAME=execution_db npm test
```

O Jest exige **mínimo de 80%** em statements, branches, funções e linhas (`coverageThreshold` em `jest.config.ts`), e o pipeline falha abaixo disso.

Última execução: 45 testes em 5 suítes.

| Métrica | Cobertura |
|---|---|
| Statements | 98,78% |
| Branches | 95,45% |
| Funções | 96,50% |
| Linhas | 99,80% |

Cada execução do pipeline publica a tabela de cobertura e as métricas do SonarQube no **resumo do job** (aba *Actions* → execução → *Summary*). O relatório completo fica no artefato `coverage`.

## CI/CD

`.github/workflows/ci-cd.yml`:
1. **Testes:** lint, build, testes com PostgreSQL real (service container) e cobertura mínima de 80%.
2. **Qualidade (SonarQube):** sobe o SonarQube como service container, analisa com a cobertura dos testes e **falha se o quality gate reprovar**.
3. **Build e Push Docker:** só em push. `yaraduarte/oficina-execution-service:<sha>` na `main` e `:hml-<sha>` na `homolog`.
4. **Deploy:** `scripts/deploy.sh` na EC2 via SSH aplica `k8s/execution.yaml` (Deployment, Service, HPA e Ingress `/execucao` com strip-prefix) no namespace do ambiente.

Fluxo de branches: `feature/*` → PR → `homolog` → PR → `main`. As duas branches são protegidas e exigem os checks verdes.

Secrets necessários:
- `DOCKER_USERNAME`
- `DOCKER_PASSWORD`
- `K3S_HOST`
- `K3S_SSH_PRIVATE_KEY`
- `NEW_RELIC_LICENSE_KEY`

As credenciais do banco **não** ficam no GitHub. Elas vêm do Secret `execution-db`, criado no cluster pelo bootstrap do [oficina-infra-k8s](https://github.com/yaraduarte/oficina-infra-k8s).
