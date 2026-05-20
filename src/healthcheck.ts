import { config } from "./config";
import { createRedisConnection } from "./queue";
import { supabase } from "./services/supabase";

async function main(): Promise<void> {
  const redis = createRedisConnection();

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
