import { Job, Worker } from "bullmq";

import { config } from "./config";
import { optimizeDrivePdfJob } from "./jobs/optimizeDrivePdf";
import {
  addFailedJobToDlq,
  createRedisConnection,
  deadLetterQueue,
  pdfOptimizeQueue,
  redisConnection,
  validateJobData,
  type DrivePdfOptimizeJob,
} from "./queue";
import { insertCompressionAuditLog, markFileFailed } from "./services/filesDb";
import { markPdfJobDownloading, markPdfJobFailed } from "./services/pdfJobsDb";
import { logger } from "./utils/logger";

const workerConnection = createRedisConnection();

const worker = new Worker<DrivePdfOptimizeJob>(
  config.queueName,
  async (job) => {
    validateJobData(job.data);

    const queueWaitMs = Math.max(0, Date.now() - Date.parse(job.data.enqueuedAt));
    // BullMQ increments attemptsMade before the processor runs, so attemptsMade
    // already reflects the current attempt number (1-based).
    const attempt = job.attemptsMade;

    logger.info(
      {
        jobId: job.id,
        jobDbId: job.data.jobId,
        fileId: job.data.fileId,
        userId: job.data.userId,
        storagePath: job.data.storagePath,
        queue_wait_ms: queueWaitMs,
        attempt,
      },
      "Starting PDF optimization job",
    );

    await markPdfJobDownloading(job.data.jobId, { queueWaitMs, attempt });

    let terminalReached = false;
    try {
      const result = await optimizeDrivePdfJob(job.data, queueWaitMs);
      terminalReached = true;
      return result;
    } finally {
      if (!terminalReached) {
        const maxAttempts = job.opts.attempts ?? 1;
        if (attempt >= maxAttempts) {
          // Final attempt ended without a terminal DB status — force failed to
          // avoid leaving the row orphaned in a non-terminal state.
          await markPdfJobFailed(
            job.data.jobId,
            "worker exited without terminal status",
            attempt,
          ).catch((err) =>
            logger.error({ err, jobDbId: job.data.jobId }, "Orphan guard update failed"),
          );
        }
      }
    }
  },
  {
    connection: workerConnection,
    concurrency: config.workerConcurrency,
  },
);

worker.on("completed", (job, result) => {
  logger.info(
    {
      jobId: job.id,
      jobDbId: job.data.jobId,
      fileId: job.data.fileId,
      storagePath: job.data.storagePath,
      queue_wait_ms: result.queueWaitMs,
      download_ms: result.downloadMs,
      ghostscript_ms: result.ghostscriptMs,
      upload_ms: result.uploadMs,
      duration_ms: result.durationMs,
      applied: result.applied,
      skipped: result.skipped,
      original_size: result.originalSize,
      compressed_size: result.compressedSize,
    },
    "Completed PDF optimization job",
  );
});

worker.on("failed", (job, error) => {
  void handleFailedJob(job, error);
});

worker.on("error", (error) => {
  logger.error({ error }, "Worker error");
});

async function handleFailedJob(job: Job<DrivePdfOptimizeJob> | undefined, error: Error): Promise<void> {
  if (!job) {
    logger.error({ error }, "PDF optimization job failed before BullMQ job was available");
    return;
  }

  const attempts = job.opts.attempts ?? 1;
  const finalFailure = job.attemptsMade >= attempts;

  logger.error(
    {
      error,
      jobId: job.id,
      jobDbId: job.data?.jobId,
      fileId: job.data?.fileId,
      storagePath: job.data?.storagePath,
      attemptsMade: job.attemptsMade,
      attempts,
      finalFailure,
    },
    "PDF optimization job failed",
  );

  if (!finalFailure) {
    return;
  }

  try {
    validateJobData(job.data);
  } catch (validationError) {
    logger.error({ validationError, jobId: job.id }, "Cannot persist failure for invalid job payload");
    return;
  }

  await persistFinalFailure(job, error);
}

async function persistFinalFailure(job: Job<DrivePdfOptimizeJob>, error: Error): Promise<void> {
  const message = error.message || "PDF optimization failed";
  const queueWaitMs = Math.max(0, Date.now() - Date.parse(job.data.enqueuedAt));
  const attempt = job.attemptsMade;

  try {
    await markFileFailed(job.data.fileId, message);
  } catch (markError) {
    logger.error({ markError, jobId: job.id, fileId: job.data.fileId }, "Failed to mark file as failed");
  }

  try {
    await markPdfJobFailed(job.data.jobId, message, attempt);
  } catch (markError) {
    logger.error(
      { markError, jobId: job.id, jobDbId: job.data.jobId },
      "Failed to mark pdf job as failed",
    );
  }

  try {
    await insertCompressionAuditLog({
      fileId: job.data.fileId,
      userId: job.data.userId,
      metadata: {
        applied: false,
        setting: "ghostscript-300dpi",
        original_size: job.data.originalSizeBytes,
        compressed_size: job.data.originalSizeBytes,
        reduction_ratio: 0,
        duration_ms: 0,
        queue_wait_ms: queueWaitMs,
        error: message,
      },
    });
  } catch (auditError) {
    logger.error({ auditError, jobId: job.id, fileId: job.data.fileId }, "Failed to audit final failure");
  }

  try {
    await addFailedJobToDlq(job.data, {
      originalJobId: job.id,
      failedReason: message,
      attemptsMade: job.attemptsMade,
    });
  } catch (dlqError) {
    logger.error({ dlqError, jobId: job.id, fileId: job.data.fileId }, "Failed to add job to DLQ");
  }
}

async function shutdown(signal: NodeJS.Signals): Promise<void> {
  logger.info({ signal }, "Shutting down PDF worker");

  await worker.close();
  await pdfOptimizeQueue.close();
  await deadLetterQueue.close();
  await workerConnection.quit();
  await redisConnection.quit();

  logger.info("PDF worker stopped");
}

process.on("SIGTERM", () => {
  void shutdown("SIGTERM").then(() => process.exit(0));
});

process.on("SIGINT", () => {
  void shutdown("SIGINT").then(() => process.exit(0));
});

logger.info(
  {
    queueName: config.queueName,
    dlqName: config.dlqName,
    concurrency: config.workerConcurrency,
    bucket: config.supabase.bucket,
    maxPdfBytes: config.limits.maxPdfBytes,
  },
  "PDF worker started",
);
