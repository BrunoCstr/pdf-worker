import { config } from "./config";
import { createRedisConnection } from "./queue";
import { supabase } from "./services/supabase";

async function main(): Promise<void> {
  const redis = createRedisConnection();

  try {
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
    await redis.quit();
  }
}

main().catch((error) => {
  console.error(error);
  process.exit(1);
});
