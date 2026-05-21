import { createHmac, timingSafeEqual } from "node:crypto";

import { Queue } from "bullmq";

import { config } from "./config";
import { createRedisConnection } from "./redis";

export type DrivePdfOptimizeJob = {
  jobId: string;
  fileId: string;
  userId: string;
  storagePath: string;
  mimeType: "application/pdf";
  originalSizeBytes: number;
  bucket: string;
  enqueuedAt: string;
  signature?: string;
};

export type FailedDrivePdfJob = {
  originalJobId?: string;
  queueName: string;
  payload: DrivePdfOptimizeJob;
  failedReason: string;
  attemptsMade: number;
  failedAt: string;
};

export { createRedisConnection } from "./redis";

export const redisConnection = createRedisConnection();

export const pdfOptimizeQueue = new Queue<DrivePdfOptimizeJob>(config.queueName, {
  connection: redisConnection,
});

export const deadLetterQueue = new Queue<FailedDrivePdfJob>(config.dlqName, {
  connection: redisConnection,
});

export function validateJobData(data: unknown): asserts data is DrivePdfOptimizeJob {
  if (!data || typeof data !== "object") {
    throw new Error("Job payload must be an object");
  }

  const payload = data as Partial<DrivePdfOptimizeJob>;

  if (!payload.jobId || typeof payload.jobId !== "string") {
    throw new Error("Job payload is missing jobId");
  }

  if (!payload.fileId || typeof payload.fileId !== "string") {
    throw new Error("Job payload is missing fileId");
  }

  if (!payload.userId || typeof payload.userId !== "string") {
    throw new Error("Job payload is missing userId");
  }

  if (!payload.storagePath || typeof payload.storagePath !== "string") {
    throw new Error("Job payload is missing storagePath");
  }

  if (!payload.storagePath.startsWith(`${payload.userId}/`)) {
    throw new Error("storagePath must start with the userId prefix required by Storage RLS");
  }

  if (payload.mimeType !== "application/pdf") {
    throw new Error("Only application/pdf jobs are supported");
  }

  if (
    typeof payload.originalSizeBytes !== "number" ||
    !Number.isFinite(payload.originalSizeBytes) ||
    payload.originalSizeBytes <= 0
  ) {
    throw new Error("originalSizeBytes must be a positive number");
  }

  if (!payload.bucket || typeof payload.bucket !== "string") {
    throw new Error("Job payload is missing bucket");
  }

  if (!payload.enqueuedAt || Number.isNaN(Date.parse(payload.enqueuedAt))) {
    throw new Error("enqueuedAt must be a valid ISO timestamp");
  }

  validateSignature(payload as DrivePdfOptimizeJob);
}

function validateSignature(payload: DrivePdfOptimizeJob): void {
  if (!config.workerApiSecret) {
    return;
  }

  if (!payload.signature) {
    throw new Error("Job signature is required when WORKER_API_SECRET is configured");
  }

  const expected = signPayload(payload, config.workerApiSecret);
  const providedBuffer = Buffer.from(payload.signature, "hex");
  const expectedBuffer = Buffer.from(expected, "hex");

  if (providedBuffer.length !== expectedBuffer.length || !timingSafeEqual(providedBuffer, expectedBuffer)) {
    throw new Error("Invalid job signature");
  }
}

export function signPayload(payload: DrivePdfOptimizeJob, secret: string): string {
  return createHmac("sha256", secret)
    .update(`${payload.fileId}|${payload.userId}|${payload.storagePath}|${payload.enqueuedAt}`)
    .digest("hex");
}

export async function addFailedJobToDlq(
  payload: DrivePdfOptimizeJob,
  options: {
    originalJobId?: string;
    failedReason: string;
    attemptsMade: number;
  },
): Promise<void> {
  await deadLetterQueue.add(
    "failed",
    {
      originalJobId: options.originalJobId,
      queueName: config.queueName,
      payload,
      failedReason: options.failedReason,
      attemptsMade: options.attemptsMade,
      failedAt: new Date().toISOString(),
    },
    {
      jobId: `${payload.fileId}:${Date.now()}`,
      removeOnComplete: 1_000,
    },
  );
}
