import IORedis, { type RedisOptions } from "ioredis";

import { config } from "./config";

const bullmqOptions: RedisOptions = {
  maxRetriesPerRequest: null,
};

const probeOptions: RedisOptions = {
  maxRetriesPerRequest: 1,
  connectTimeout: 10_000,
  lazyConnect: true,
  retryStrategy: () => null,
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
