/** Conexão do Postgres de teste: docker local ou service container no CI. */
export const testDbConfig = {
  host: process.env.TEST_DB_HOST ?? 'localhost',
  port: Number(process.env.TEST_DB_PORT ?? 5432),
  database: process.env.TEST_DB_NAME ?? 'execution_test',
  user: process.env.TEST_DB_USER ?? 'execution',
  password: process.env.TEST_DB_PASS ?? 'execution123',
  ssl: false,
};
