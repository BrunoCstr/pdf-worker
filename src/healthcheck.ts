import { config } from "./config";
import { createRedisProbeConnection, getRedisHostnameForLogs } from "./redis";
import { supabase } from "./services/supabase";

function formatProbeError(error: unknown): string {
  const host = getRedisHostnameForLogs();
  const base = error instanceof Error ? error.message : String(error);
  const code = (error as NodeJS.ErrnoException)?.code;
  const redisUnreachable =
    base.includes("ENOTFOUND") ||
    code === "ENOTFOUND" ||
    base.includes("Connection is closed") ||
    base.includes("ECONNREFUSED");

  if (redisUnreachable) {
    return [
      `Redis inacessível (host: ${host}).`,
      "Use a URL pública do Upstash (ex.: rediss://default:***@nome-regiao.upstash.io:6379).",
      "Host interno do Coolify (ex.: i4dbn9x19j0...) só funciona dentro da rede Docker do servidor.",
      base !== "Connection is closed" ? `Detalhe: ${base}` : undefined,
    ]
      .filter(Boolean)
      .join(" ");
  }

  return base;
}

async function main(): Promise<void> {
  const redis = createRedisProbeConnection();
  redis.on("error", () => {});

  try {
    await redis.connect();
    await redis.ping();

    const { error } = await supabase.storage.from(config.supabase.bucket).list("", {
      limit: 1,
    });
    if (error) {
      throw new Error(
        `Supabase storage check failed for bucket "${config.supabase.bucket}": ${error.message}`,
      );
    }
  } finally {
    redis.disconnect();
  }
}

main().catch((error) => {
  console.error(formatProbeError(error));
  process.exit(1);
});
