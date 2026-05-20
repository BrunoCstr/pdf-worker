import { config } from "./config";
import IORedis from "ioredis";
import { supabase } from "./services/supabase";

async function main(): Promise<void> {
  const redis = new IORedis({
    host: config.redis.host,
    port: config.redis.port,
    password: config.redis.password,
    tls: config.redis.tls ? {} : undefined,
    maxRetriesPerRequest: null,
  });

  try {
    await redis.ping();

    const { error } = await supabase.storage.getBucket(config.supabase.bucket);
    if (error) {
      throw new Error(`Supabase bucket check failed: ${error.message}`);
    }
  } finally {
    await redis.quit();
  }
}

main().catch((error) => {
  console.error(error);
  process.exit(1);
});
