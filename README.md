# oficina-execution-service

**Execution Service** da Oficina Marimb0ndo (FIAP SOAT — Tech Challenge Fase 4).

Fila de execução, diagnóstico, reparo e estoque de peças com PostgreSQL.

Arquitetura geral, contratos de eventos e decisões: [oficinaMarimbondo/docs/fase4](https://github.com/yaraduarte/oficinaMarimbondo/tree/homolog/docs/fase4).

## Fluxo de branches

`feature/*` → PR → `homolog` (deploy homologação) → PR → `main` (deploy produção). `main` e `homolog` são protegidas.
