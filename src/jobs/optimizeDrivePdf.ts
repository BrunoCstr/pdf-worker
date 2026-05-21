import { performance } from "node:perf_hooks";

import { computeGhostscriptTimeoutMs, config } from "../config";
import { JobCancelledError } from "../errors";
import type { DrivePdfOptimizeJob } from "../queue";
import {
  compressPdfWithGhostscript,
  GHOSTSCRIPT_SETTING,
  type GhostscriptProgress,
} from "../services/compressPdf";
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
 * Runs Ghostscript and reports REAL progress by parsing GS stderr output.
 *
 * Progress strategy:
 * - `compressPdf` invokes `onProgress` every time GS emits a new page or
 *   when the total page count is first parsed ("Processing pages 1 through N.").
 * - We map (currentPage / totalPages) linearly into the [31, 75] window so the
 *   user-visible progress reflects actual work done, not elapsed time.
 * - DB writes are throttled (min interval + min delta) to avoid hammering
 *   Supabase on PDFs with hundreds of small pages.
 *
 * Fallback:
 * - If GS never emits "Processing pages ..." (rare, e.g. for some malformed
 *   PDFs), we tick a slow time-based progress from 31 → 60 so the bar still
 *   moves. As soon as a real Page line is parsed, real progress takes over.
 *
 * Cancellation:
 * - An independent low-frequency timer checks if the job was cancelled and
 *   aborts the GS process via AbortController.
 */
async function compressPdfWithGhostscriptAndReportProgress(
  jobId: string,
  options: Omit<Parameters<typeof compressPdfWithGhostscript>[0], "signal" | "onProgress">,
  ghostscriptTimeoutMs: number,
): Promise<Awaited<ReturnType<typeof compressPdfWithGhostscript>>> {
  const PROGRESS_START = 31;
  const PROGRESS_END = 75;
  const FALLBACK_PROGRESS_CEILING = 60;
  const MIN_DB_INTERVAL_MS = 1_500;
  const MIN_DB_DELTA = 1;
  const CANCEL_CHECK_INTERVAL_MS = 5_000;

  const abortController = new AbortController();
  const startedAt = performance.now();

  let lastReportedProgress = PROGRESS_START;
  let lastDbWriteAt = 0;
  let realProgressSeen = false;
  let pendingWrite: Promise<void> | null = null;

  const reportProgress = (progress: number) => {
    const clamped = Math.max(PROGRESS_START, Math.min(PROGRESS_END, Math.round(progress)));
    const now = Date.now();
    const delta = clamped - lastReportedProgress;

    if (delta < MIN_DB_DELTA || now - lastDbWriteAt < MIN_DB_INTERVAL_MS) {
      return;
    }

    lastReportedProgress = clamped;
    lastDbWriteAt = now;

    if (pendingWrite) {
      return;
    }

    pendingWrite = (async () => {
      try {
        await updatePdfJobProgress(jobId, clamped);
      } catch (error) {
        logger.warn({ error, jobId, progress: clamped }, "Failed to update pdf job progress");
      } finally {
        pendingWrite = null;
      }
    })();
  };

  const onProgress = (info: GhostscriptProgress) => {
    if (!info.totalPages || info.totalPages <= 0) {
      return;
    }

    realProgressSeen = true;
    const ratio = Math.min(1, info.currentPage / info.totalPages);
    reportProgress(PROGRESS_START + ratio * (PROGRESS_END - PROGRESS_START));
  };

  const fallbackTimer = setInterval(() => {
    if (realProgressSeen) {
      return;
    }

    // Without a totalPages signal, advance slowly toward FALLBACK_PROGRESS_CEILING
    // proportional to elapsed time vs configured timeout. Real progress will
    // overwrite this the moment GS emits its first "Page N" line.
    const elapsed = performance.now() - startedAt;
    const ratio = Math.min(1, elapsed / ghostscriptTimeoutMs);
    const projected = PROGRESS_START + ratio * (FALLBACK_PROGRESS_CEILING - PROGRESS_START);
    reportProgress(projected);
  }, MIN_DB_INTERVAL_MS);

  const cancelTimer = setInterval(() => {
    void (async () => {
      try {
        await assertPdfJobNotCancelled(jobId);
      } catch (error) {
        if (error instanceof JobCancelledError) {
          abortController.abort(error);
        }
      }
    })();
  }, CANCEL_CHECK_INTERVAL_MS);

  try {
    await assertPdfJobNotCancelled(jobId);
    return await compressPdfWithGhostscript({
      ...options,
      signal: abortController.signal,
      onProgress,
    });
  } finally {
    clearInterval(fallbackTimer);
    clearInterval(cancelTimer);

    // The IIFE inside reportProgress already catches its own errors, so we
    // just need to wait for any in-flight DB write to finish before resolving.
    const inFlight: Promise<void> | null = pendingWrite;
    if (inFlight !== null) {
      await inFlight;
    }
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
        setting: GHOSTSCRIPT_SETTING,
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
