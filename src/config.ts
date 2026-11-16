import 'dotenv/config';
import { DatabaseConfig } from './infrastructure/postgres/PostgresExecutionStore';

function required(name: string, env: NodeJS.ProcessEnv): string {
  const value = env[name];
  if (!value) throw new Error(`Variável de ambiente obrigatória ausente: ${name}`);
  return value;
}

export interface Config {
  port: number;
  isProduction: boolean;
  jwtSecret: string;
  originVerifySecret?: string;
  rabbitUrl?: string;
  outboxIntervalMs: number;
  database: DatabaseConfig;
}

export function loadConfig(env: NodeJS.ProcessEnv = process.env): Config {
  const isProduction = env.NODE_ENV === 'production';
  const req = (name: string, fallback: string) => (isProduction ? required(name, env) : env[name] ?? fallback);
  return {
    port: Number(env.PORT ?? 3000),
    isProduction,
    jwtSecret: req('JWT_SECRET', 'dev-secret'),
    originVerifySecret: env.ORIGIN_VERIFY_SECRET || undefined,
    rabbitUrl: env.RABBITMQ_URL || undefined,
    outboxIntervalMs: Number(env.OUTBOX_INTERVAL_MS ?? 1000),
    database: {
      host: req('DB_HOST', 'localhost'),
      port: Number(env.DB_PORT ?? 5432),
      database: req('DB_NAME', 'execution_db'),
      user: req('DB_USER', 'execution'),
      password: req('DB_PASS', 'execution123'),
      ssl: env.DB_SSL === 'true',
    },
  };
}
