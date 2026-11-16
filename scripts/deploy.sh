#!/bin/bash
# Deploy do execution-service no k3s (executado na EC2 via SSH pelo pipeline).
# Uso: deploy.sh <namespace> <imagem> <manifesto>
# Variáveis esperadas: NEW_RELIC_LICENSE_KEY (opcional), APP_ENV
# Pré-requisito: Secret execution-db no namespace (criado pelo bootstrap do oficina-infra-k8s:
# database execution_db com usuário próprio na instância RDS — ADR-007).
set -euo pipefail

NS="$1"
IMAGE="$2"
MANIFEST="$3"
K="sudo kubectl"
SUFFIX=$([ "$NS" = "oficina-marimb0ndo" ] && echo "" || echo "-hml")

secret() { $K get secret "$1" -n "$NS" -o jsonpath="{.data.$2}" 2>/dev/null | base64 -d || true; }

# Segredos compartilhados (já existentes no namespace)
JWT_SECRET=$(secret oficina-secret JWT_SECRET)
ORIGIN_VERIFY_SECRET=$(secret oficina-secret ORIGIN_VERIFY_SECRET)
RABBITMQ_URL=$(secret rabbitmq-credentials url)
DB_HOST=$(secret execution-db host)
DB_NAME=$(secret execution-db database)
DB_USER=$(secret execution-db user)
DB_PASS=$(secret execution-db password)
[ -n "$DB_HOST" ] || { echo "Secret execution-db ausente em $NS (rode o bootstrap do oficina-infra-k8s)"; exit 1; }

$K create configmap execution-config -n "$NS" \
  --from-literal=NODE_ENV=production \
  --from-literal=APP_ENV="${APP_ENV:-production}" \
  --from-literal=PORT=3000 \
  --from-literal=DB_HOST="$DB_HOST" \
  --from-literal=DB_PORT=5432 \
  --from-literal=DB_NAME="$DB_NAME" \
  --from-literal=DB_USER="$DB_USER" \
  --from-literal=DB_SSL=true \
  --from-literal="NEW_RELIC_APP_NAME=Oficina Marimbondo Execution${SUFFIX:+ (HML)}" \
  --dry-run=client -o yaml | $K apply -f -

$K create secret generic execution-secret -n "$NS" \
  --from-literal=JWT_SECRET="$JWT_SECRET" \
  --from-literal=ORIGIN_VERIFY_SECRET="$ORIGIN_VERIFY_SECRET" \
  --from-literal=RABBITMQ_URL="$RABBITMQ_URL" \
  --from-literal=DB_PASS="$DB_PASS" \
  --from-literal=NEW_RELIC_LICENSE_KEY="${NEW_RELIC_LICENSE_KEY:-}" \
  --dry-run=client -o yaml | $K apply -f -

sed -e "s|__NAMESPACE__|${NS}|g" -e "s|__IMAGE__|${IMAGE}|g" "$MANIFEST" | $K apply -f -
$K rollout restart deployment/execution-service -n "$NS"
$K rollout status deployment/execution-service -n "$NS" --timeout=180s
echo "execution-service atualizado em ${NS} com ${IMAGE}"
