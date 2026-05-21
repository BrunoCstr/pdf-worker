import { Job, Worker } from "bullmq";

import { config } from "./config";
import { isJobCancelledError } from "./errors";
import { optimizeDrivePdfJob } from "./jobs/optimizeDrivePdf";
import {
  addFailedJobToDlq,
  createRedisConnection,
  deadLetterQueue,
  dlqConnection,
  pdfOptimizeQueue,
  redisConnection,
  validateJobData,
  type DrivePdfOptimizeJob,
} from "./queue";
import { GHOSTSCRIPT_SETTING, smokeTestGhostscript } from "./services/compressPdf";
import { insertCompressionAuditLog, markFileFailed } from "./services/filesDb";
import { getPdfJob, markPdfJobDownloading, markPdfJobFailed } from "./services/pdfJobsDb";
import { logger } from "./utils/logger";

const workerLockDurationMs = Math.min(
  config.limits.ghostscriptTimeoutMaxMs,
  Math.max(120_000, config.limits.ghostscriptTimeoutMs),
);

const workerConnection = createRedisConnection();

let worker: Worker<DrivePdfOptimizeJob> | undefined;

function createPdfOptimizeWorker(): Worker<DrivePdfOptimizeJob> {
  const w = new Worker<DrivePdfOptimizeJob>(
    config.queueName,
    async (job) => {
      validateJobData(job.data);

      const queueWaitMs = Math.max(0, Date.now() - Date.parse(job.data.enqueuedAt));
      // job.attemptsMade inside the processor is 0-based (incremented only after
      // the attempt fails). Add 1 to get a 1-based attempt number for storage.
      const attempt = job.attemptsMade + 1;

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

      if (await isPdfJobCancelled(job.data.jobId)) {
        return buildCancelledResult(job.data, queueWaitMs);
      }

      await markPdfJobDownloading(job.data.jobId, { queueWaitMs, attempt });

      try {
        return await optimizeDrivePdfJob(job.data, queueWaitMs);
      } catch (err) {
        // Update DB on every failed attempt so the row never stays orphaned
        // in a non-terminal state between retries. markPdfJobFailed guards
        // against overwriting completed/skipped, so this is safe to call
        // unconditionally. persistFinalFailure (called by the 'failed' event)
        // will overwrite this on the last attempt with the same data.
        const message = err instanceof Error ? err.message : "PDF optimization failed";

        if (isJobCancelledError(err) || (await isPdfJobCancelled(job.data.jobId))) {
          logger.info({ jobId: job.id, jobDbId: job.data.jobId }, "PDF optimization job cancelled");
          return buildCancelledResult(job.data, queueWaitMs);
        }

        await markPdfJobFailed(job.data.jobId, message, attempt).catch((dbErr) =>
          logger.error({ dbErr, jobDbId: job.data.jobId }, "Failed to mark pdf job as failed before retry"),
        );
        throw err;
      }
    },
    {
      connection: workerConnection,
      concurrency: config.workerConcurrency,
      // BullMQ renews the lock every lockDuration/2 automatically. The event
      // loop stays responsive because GS runs as a child process (non-blocking).
      lockDuration: workerLockDurationMs,
    },
  );

  w.on("completed", (job, result) => {
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

  w.on("failed", (job, error) => {
    void handleFailedJob(job, error);
  });

  w.on("error", (error) => {
    logger.error({ error }, "Worker error");
  });

  return w;
}

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

  if (isJobCancelledError(error) || (await isPdfJobCancelled(job.data.jobId))) {
    logger.info({ jobId: job.id, jobDbId: job.data.jobId }, "Skipping final failure persistence for cancelled job");
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
        setting: GHOSTSCRIPT_SETTING,
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

async function isPdfJobCancelled(jobId: string): Promise<boolean> {
  const row = await getPdfJob(jobId).catch((error) => {
    logger.warn({ error, jobDbId: jobId }, "Failed to check pdf job cancellation");
    return null;
  });

  return row?.status === "cancelled";
}

function buildCancelledResult(job: DrivePdfOptimizeJob, queueWaitMs: number) {
  return {
    applied: false,
    skipped: false,
    cancelled: true,
    message: "cancelled",
    originalSize: job.originalSizeBytes,
    compressedSize: job.originalSizeBytes,
    reductionRatio: 0,
    queueWaitMs,
    durationMs: 0,
  };
}

async function shutdown(signal: NodeJS.Signals): Promise<void> {
  logger.info({ signal }, "Shutting down PDF worker");

  if (worker) {
    await worker.close();
  }
  await pdfOptimizeQueue.close();
  await deadLetterQueue.close();
  await workerConnection.quit();
  await redisConnection.quit();
  await dlqConnection.quit();

  logger.info("PDF worker stopped");
}

process.on("SIGTERM", () => {
  void shutdown("SIGTERM").then(() => process.exit(0));
});

process.on("SIGINT", () => {
  void shutdown("SIGINT").then(() => process.exit(0));
});

async function bootstrap(): Promise<void> {
  // Validate Ghostscript argv against a synthetic PDF before accepting jobs.
  // Catches malformed flags (e.g. wrong -s vs -d prefix, deprecated params on
  // older GS versions, missing binary) at boot time so misconfiguration never
  // hits a real user job.
  try {
    const smoke = await smokeTestGhostscript();
    logger.info(
      {
        duration_ms: smoke.durationMs,
        input_bytes: smoke.inputBytes,
        output_bytes: smoke.outputBytes,
        gs_binary: config.ghostscriptBinary,
      },
      "Ghostscript smoke test passed",
    );
  } catch (err) {
    logger.fatal(
      {
        err,
        gs_binary: config.ghostscriptBinary,
        gs_config: config.ghostscript,
      },
      "Ghostscript smoke test failed — refusing to start worker. Fix the configuration and redeploy.",
    );
    process.exit(1);
  }

  worker = createPdfOptimizeWorker();

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
}

void bootstrap();
