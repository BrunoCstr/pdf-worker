import { performance } from "node:perf_hooks";

import { computeGhostscriptTimeoutMs, config } from "../config";
import { JobCancelledError } from "../errors";
import type { DrivePdfOptimizeJob } from "../queue";
import { compressPdfWithGhostscript } from "../services/compressPdf";
import {
  adjustUserStorageUsedBytes,
  insertCompressionAuditLog,
  markFileProcessing,
  markFileReadyApplied,
  markFileReadyNotSmaller,
  markFileSkipped,
} from "../services/filesDb";
import {
  markPdfJobCompleted,
  markPdfJobCompressing,
  markPdfJobSkipped,
  markPdfJobUploading,
  getPdfJob,
  updatePdfJobProgress,
} from "../services/pdfJobsDb";
import { downloadStorageFile, uploadStorageFile } from "../services/storage";
import { logger } from "../utils/logger";
import { createJobTempDir } from "../utils/tempFiles";

export type OptimizeDrivePdfResult = {
  applied: boolean;
  skipped: boolean;
  message?: string;
  originalSize: number;
  compressedSize: number;
  reductionRatio: number;
  queueWaitMs: number;
  durationMs: number;
  downloadMs?: number;
  ghostscriptMs?: number;
  uploadMs?: number;
};

export async function optimizeDrivePdfJob(
  payload: DrivePdfOptimizeJob,
  queueWaitMs: number,
): Promise<OptimizeDrivePdfResult> {
  const startedAt = performance.now();

  await assertPdfJobNotCancelled(payload.jobId);

  if (payload.originalSizeBytes > config.limits.maxPdfBytes) {
    const message = `pdf_too_large:${payload.originalSizeBytes}>${config.limits.maxPdfBytes}`;

    await markFileSkipped(payload.fileId, message);
    const durationMs = Math.round(performance.now() - startedAt);
    await markPdfJobSkippedBestEffort(payload.jobId, message);

    const result: OptimizeDrivePdfResult = {
      applied: false,
      skipped: true,
      message,
      originalSize: payload.originalSizeBytes,
      compressedSize: payload.originalSizeBytes,
      reductionRatio: 0,
      queueWaitMs,
      durationMs,
    };

    await insertAuditLogBestEffort(payload, result);
    return result;
  }

  await assertPdfJobNotCancelled(payload.jobId);
  await markFileProcessing(payload.fileId);

  const temp = await createJobTempDir(payload.fileId);

  try {
    await assertPdfJobNotCancelled(payload.jobId);
    const download = await downloadStorageFile({
      bucket: payload.bucket,
      storagePath: payload.storagePath,
      destinationPath: temp.inputPath,
    });

    await assertPdfJobNotCancelled(payload.jobId);
    await markPdfJobCompressingBestEffort(payload.jobId);

    const ghostscriptTimeoutMs = computeGhostscriptTimeoutMs(payload.originalSizeBytes);

    const compression = await compressPdfWithGhostscriptAndReportProgress(
      payload.jobId,
      {
        inputPath: temp.inputPath,
        outputPath: temp.outputPath,
        timeoutMs: ghostscriptTimeoutMs,
      },
      ghostscriptTimeoutMs,
    );

    await assertPdfJobNotCancelled(payload.jobId);

    if (!compression.applied) {
      await markFileReadyNotSmaller(payload.fileId);
      const durationMs = Math.round(performance.now() - startedAt);

      await markPdfJobCompletedBestEffort(payload.jobId, {
        compressedSizeBytes: compression.originalSize,
        compressionRatio: null,
        compressionSetting: null,
        compressionMessage: "not_smaller",
        processingMs: durationMs,
      });

      const result: OptimizeDrivePdfResult = {
        applied: false,
        skipped: false,
        message: "not_smaller",
        originalSize: compression.originalSize,
        compressedSize: compression.compressedSize,
        reductionRatio: compression.reductionRatio,
        queueWaitMs,
        durationMs,
        downloadMs: download.downloadMs,
        ghostscriptMs: compression.ghostscriptMs,
      };

      await insertAuditLogBestEffort(payload, result);
      return result;
    }

    await assertPdfJobNotCancelled(payload.jobId);
    await markPdfJobUploadingBestEffort(payload.jobId);

    const upload = await uploadStorageFile({
      bucket: payload.bucket,
      storagePath: payload.storagePath,
      sourcePath: temp.outputPath,
      contentType: payload.mimeType,
    });

    await assertPdfJobNotCancelled(payload.jobId);

    await markFileReadyApplied({
      fileId: payload.fileId,
      sizeBytes: compression.compressedSize,
      compressionRatio: compression.reductionRatio,
      compressionSetting: compression.setting,
    });

    const quotaDelta = compression.compressedSize - payload.originalSizeBytes;
    if (quotaDelta < 0) {
      await adjustUserStorageUsedBytes(payload.userId, quotaDelta);
    }

    const durationMs = Math.round(performance.now() - startedAt);

    await markPdfJobCompletedBestEffort(payload.jobId, {
      compressedSizeBytes: compression.compressedSize,
      compressionRatio: compression.reductionRatio,
      compressionSetting: compression.setting,
      compressionMessage: null,
      processingMs: durationMs,
    });

    const result: OptimizeDrivePdfResult = {
      applied: true,
      skipped: false,
      originalSize: compression.originalSize,
      compressedSize: compression.compressedSize,
      reductionRatio: compression.reductionRatio,
      queueWaitMs,
      durationMs,
      downloadMs: download.downloadMs,
      ghostscriptMs: compression.ghostscriptMs,
      uploadMs: upload.uploadMs,
    };

    await insertAuditLogBestEffort(payload, result);
    return result;
  } finally {
    await temp.cleanup();
  }
}

/**
 * Runs Ghostscript compression while reporting DB progress every 30 s.
 * Progress moves linearly from 31 → 75 over the GS timeout window so the
 * client never sees the job frozen at 30% during a long compression run.
 */
async function compressPdfWithGhostscriptAndReportProgress(
  jobId: string,
  options: Parameters<typeof compressPdfWithGhostscript>[0],
  ghostscriptTimeoutMs: number,
): Promise<Awaited<ReturnType<typeof compressPdfWithGhostscript>>> {
  const PROGRESS_START = 31;
  const PROGRESS_END = 75;
  const TICK_MS = 30_000;
  const totalTicks = Math.floor(ghostscriptTimeoutMs / TICK_MS);
  const stepPerTick = totalTicks > 0 ? (PROGRESS_END - PROGRESS_START) / totalTicks : 0;

  let currentProgress = PROGRESS_START;
  let tick = 0;
  let checkingCancellation = false;
  const abortController = new AbortController();

  const timer = setInterval(() => {
    if (checkingCancellation) {
      return;
    }

    checkingCancellation = true;
    tick += 1;
    currentProgress = Math.min(PROGRESS_START + Math.round(stepPerTick * tick), PROGRESS_END);

    void (async () => {
      try {
        await assertPdfJobNotCancelled(jobId);
        await updatePdfJobProgress(jobId, currentProgress);
      } catch (error) {
        if (error instanceof JobCancelledError) {
          abortController.abort(error);
          return;
        }

        logger.warn({ error, jobId }, "Failed to update pdf job progress");
      } finally {
        checkingCancellation = false;
      }
    })();
  }, TICK_MS);

  try {
    await assertPdfJobNotCancelled(jobId);
    return await compressPdfWithGhostscript({ ...options, signal: abortController.signal });
  } finally {
    clearInterval(timer);
  }
}

async function assertPdfJobNotCancelled(jobId: string): Promise<void> {
  const row = await getPdfJob(jobId);

  if (row?.status === "cancelled") {
    throw new JobCancelledError();
  }
}

async function markPdfJobCompressingBestEffort(jobId: string): Promise<void> {
  try {
    await markPdfJobCompressing(jobId);
  } catch (error) {
    logger.warn({ error, jobId }, "Failed to mark pdf job as compressing");
  }
}

async function markPdfJobUploadingBestEffort(jobId: string): Promise<void> {
  try {
    await markPdfJobUploading(jobId);
  } catch (error) {
    logger.warn({ error, jobId }, "Failed to mark pdf job as uploading");
  }
}

async function markPdfJobCompletedBestEffort(
  jobId: string,
  options: Parameters<typeof markPdfJobCompleted>[1],
): Promise<void> {
  try {
    await markPdfJobCompleted(jobId, options);
  } catch (error) {
    logger.warn({ error, jobId }, "Failed to mark pdf job as completed");
  }
}

async function markPdfJobSkippedBestEffort(jobId: string, message: string): Promise<void> {
  try {
    await markPdfJobSkipped(jobId, message);
  } catch (error) {
    logger.warn({ error, jobId }, "Failed to mark pdf job as skipped");
  }
}

async function insertAuditLogBestEffort(
  payload: DrivePdfOptimizeJob,
  result: OptimizeDrivePdfResult,
): Promise<void> {
  try {
    await insertCompressionAuditLog({
      fileId: payload.fileId,
      userId: payload.userId,
      metadata: {
        applied: result.applied,
        setting: "ghostscript-300dpi",
        original_size: result.originalSize,
        compressed_size: result.compressedSize,
        reduction_ratio: result.reductionRatio,
        duration_ms: result.durationMs,
        queue_wait_ms: result.queueWaitMs,
        download_ms: result.downloadMs,
        ghostscript_ms: result.ghostscriptMs,
        upload_ms: result.uploadMs,
        message: result.message,
      },
    });
  } catch (error) {
    logger.warn(
      {
        error,
        fileId: payload.fileId,
        userId: payload.userId,
        storagePath: payload.storagePath,
      },
      "Failed to insert compression audit log",
    );
  }
}
