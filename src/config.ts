type NodeEnv = "development" | "test" | "production";

export type AppConfig = {
  nodeEnv: NodeEnv;
  redis: {
    url: string;
  };
  queueName: string;
  dlqName: string;
  workerConcurrency: number;
  supabase: {
    url: string;
    /** Chave server-side (SUPABASE_SECRET_KEY no app principal). */
    secretKey: string;
    bucket: string;
  };
  limits: {
    maxPdfBytes: number;
    minCompressionReduction: number;
    ghostscriptTimeoutMs: number;
    downloadTimeoutMs: number;
  };
  ghostscriptBinary: string;
  workerApiSecret?: string;
};

function readRequired(name: string): string {
  const value = process.env[name];

  if (!value || value.trim() === "") {
    throw new Error(`Missing required environment variable: ${name}`);
  }

  return value;
}

function readOptional(name: string): string | undefined {
  const value = process.env[name]?.trim();
  return value ? value : undefined;
}

function readNumber(name: string, fallback: number, options?: { min?: number; max?: number }): number {
  const raw = readOptional(name);
  const value = raw === undefined ? fallback : Number(raw);

  if (!Number.isFinite(value)) {
    throw new Error(`Environment variable ${name} must be a finite number`);
  }

  if (options?.min !== undefined && value < options.min) {
    throw new Error(`Environment variable ${name} must be >= ${options.min}`);
  }

  if (options?.max !== undefined && value > options.max) {
    throw new Error(`Environment variable ${name} must be <= ${options.max}`);
  }

  return value;
}

function readBoolean(name: string, fallback: boolean): boolean {
  const raw = readOptional(name);

  if (raw === undefined) {
    return fallback;
  }

  if (["1", "true", "yes", "on"].includes(raw.toLowerCase())) {
    return true;
  }

  if (["0", "false", "no", "off"].includes(raw.toLowerCase())) {
    return false;
  }

  throw new Error(`Environment variable ${name} must be boolean-like`);
}

function readRedisUrl(): string {
  const url = readOptional("REDIS_URL");
  if (url) {
    if (!/^rediss?:\/\//i.test(url)) {
      throw new Error("REDIS_URL must start with redis:// or rediss://");
    }

    return url;
  }

  const host = readOptional("REDIS_HOST");
  if (host) {
    const port = readNumber("REDIS_PORT", 6379, { min: 1, max: 65535 });
    const password = readOptional("REDIS_PASSWORD");
    const tls = readBoolean("REDIS_TLS", false);
    const scheme = tls ? "rediss" : "redis";
    const auth = password ? `:${encodeURIComponent(password)}@` : "";

    return `${scheme}://${auth}${host}:${port}`;
  }

  throw new Error("Missing REDIS_URL (ex.: redis://localhost:6379 ou rediss://... do Upstash)");
}

function readSupabaseSecretKey(): string {
  const secretKey = readOptional("SUPABASE_SECRET_KEY");
  if (secretKey) {
    return secretKey;
  }

  const legacyServiceRoleKey = readOptional("SUPABASE_SERVICE_ROLE_KEY");
  if (legacyServiceRoleKey) {
    return legacyServiceRoleKey;
  }

  throw new Error(
    "Missing Supabase server key: set SUPABASE_SECRET_KEY (same value as in the Next.js app)",
  );
}

function readNodeEnv(): NodeEnv {
  const value = readOptional("NODE_ENV") ?? "production";

  if (value === "development" || value === "test" || value === "production") {
    return value;
  }

  throw new Error("NODE_ENV must be development, test, or production");
}

export const config: AppConfig = {
  nodeEnv: readNodeEnv(),
  redis: {
    url: readRedisUrl(),
  },
  queueName: readOptional("BULLMQ_QUEUE_NAME") ?? "drive-pdf-optimize",
  dlqName: readOptional("BULLMQ_DLQ_NAME") ?? "drive-pdf-failed",
  workerConcurrency: readNumber("WORKER_CONCURRENCY", 1, { min: 1 }),
  supabase: {
    url: readRequired("SUPABASE_URL"),
    secretKey: readSupabaseSecretKey(),
    bucket: readOptional("SUPABASE_BUCKET") ?? "user-files",
  },
  limits: {
    maxPdfBytes: readNumber("DRIVE_PDF_COMPRESS_MAX_BYTES", 524_288_000, { min: 1 }),
    minCompressionReduction: readNumber("DRIVE_MIN_COMPRESSION_REDUCTION", 0.05, {
      min: 0,
      max: 1,
    }),
    ghostscriptTimeoutMs: readNumber("DRIVE_PDF_OPTIMIZER_TIMEOUT_MS", 300_000, { min: 1 }),
    downloadTimeoutMs: readNumber("DRIVE_DOWNLOAD_TIMEOUT_MS", 120_000, { min: 1 }),
  },
  ghostscriptBinary: readOptional("GHOSTSCRIPT_BINARY") ?? "gs",
  workerApiSecret: readOptional("WORKER_API_SECRET"),
};
