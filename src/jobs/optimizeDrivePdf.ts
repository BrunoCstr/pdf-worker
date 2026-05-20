import { performance } from "node:perf_hooks";

import { config } from "../config";
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

  if (payload.originalSizeBytes > config.limits.maxPdfBytes) {
    const message = `pdf_too_large:${payload.originalSizeBytes}>${config.limits.maxPdfBytes}`;
    await markFileSkipped(payload.fileId, message);

    const result: OptimizeDrivePdfResult = {
      applied: false,
      skipped: true,
      message,
      originalSize: payload.originalSizeBytes,
      compressedSize: payload.originalSizeBytes,
      reductionRatio: 0,
      queueWaitMs,
      durationMs: Math.round(performance.now() - startedAt),
    };

    await insertAuditLogBestEffort(payload, result);
    return result;
  }

  await markFileProcessing(payload.fileId);

  const temp = await createJobTempDir(payload.fileId);

  try {
    const download = await downloadStorageFile({
      bucket: payload.bucket,
      storagePath: payload.storagePath,
      destinationPath: temp.inputPath,
    });

    const compression = await compressPdfWithGhostscript({
      inputPath: temp.inputPath,
      outputPath: temp.outputPath,
    });

    if (!compression.applied) {
      await markFileReadyNotSmaller(payload.fileId);

      const result: OptimizeDrivePdfResult = {
        applied: false,
        skipped: false,
        message: "not_smaller",
        originalSize: compression.originalSize,
        compressedSize: compression.compressedSize,
        reductionRatio: compression.reductionRatio,
        queueWaitMs,
        durationMs: Math.round(performance.now() - startedAt),
        downloadMs: download.downloadMs,
        ghostscriptMs: compression.ghostscriptMs,
      };

      await insertAuditLogBestEffort(payload, result);
      return result;
    }

    const upload = await uploadStorageFile({
      bucket: payload.bucket,
      storagePath: payload.storagePath,
      sourcePath: temp.outputPath,
      contentType: payload.mimeType,
    });

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

    const result: OptimizeDrivePdfResult = {
      applied: true,
      skipped: false,
      originalSize: compression.originalSize,
      compressedSize: compression.compressedSize,
      reductionRatio: compression.reductionRatio,
      queueWaitMs,
      durationMs: Math.round(performance.now() - startedAt),
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
