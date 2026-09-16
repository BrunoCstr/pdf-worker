import IORedis, { type RedisOptions } from "ioredis";

import { config } from "./config";

// Certificado do Redis self-hosted (Coolify) expirado desde 2026-08-19 e sem
// renovação automática (Traefik ACME quebrado) — só troca metadados aqui
// (jobId/fileId/userId + HMAC), nunca o binário do PDF, então aceitar sem
// validar o certificado é um trade-off aceitável.
const tlsOptions: RedisOptions = config.redis.url.startsWith("rediss://")
  ? { tls: { rejectUnauthorized: false } }
  : {};

const bullmqOptions: RedisOptions = {
  maxRetriesPerRequest: null,
  ...tlsOptions,
};

const probeOptions: RedisOptions = {
  maxRetriesPerRequest: 1,
  connectTimeout: 10_000,
  lazyConnect: true,
  retryStrategy: () => null,
  ...tlsOptions,
};

export function createRedisConnection(options?: RedisOptions): IORedis {
  return new IORedis(config.redis.url, { ...bullmqOptions, ...options });
}

/** Conexão de curta duração para healthcheck — sem retry infinito. */
export function createRedisProbeConnection(): IORedis {
  return new IORedis(config.redis.url, probeOptions);
}

export function getRedisHostnameForLogs(): string {
  try {
    return new URL(config.redis.url).hostname;
  } catch {
    return "(REDIS_URL inválida)";
  }
}
