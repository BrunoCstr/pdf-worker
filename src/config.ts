type NodeEnv = "development" | "test" | "production";

export type AppConfig = {
  nodeEnv: NodeEnv;
  redis: {
    host: string;
    port: number;
    password?: string;
    tls: boolean;
  };
  queueName: string;
  dlqName: string;
  workerConcurrency: number;
  supabase: {
    url: string;
    serviceRoleKey: string;
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
    host: readRequired("REDIS_HOST"),
    port: readNumber("REDIS_PORT", 6379, { min: 1, max: 65535 }),
    password: readOptional("REDIS_PASSWORD"),
    tls: readBoolean("REDIS_TLS", false),
  },
  queueName: readOptional("BULLMQ_QUEUE_NAME") ?? "drive-pdf-optimize",
  dlqName: readOptional("BULLMQ_DLQ_NAME") ?? "drive-pdf-failed",
  workerConcurrency: readNumber("WORKER_CONCURRENCY", 1, { min: 1 }),
  supabase: {
    url: readRequired("SUPABASE_URL"),
    serviceRoleKey: readRequired("SUPABASE_SERVICE_ROLE_KEY"),
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
