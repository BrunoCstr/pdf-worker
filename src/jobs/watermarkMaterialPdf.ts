import { performance } from "node:perf_hooks";

import { config } from "../config";
import type { MaterialPdfWatermarkJob } from "../materialQueue";
import {
  getMaterialDownloadContext,
  markMaterialDownloadProcessing,
  markMaterialDownloadReady,
} from "../services/materialDownloadsDb";
import { downloadHttpFile, downloadStorageFile, uploadStorageFile } from "../services/storage";
import { watermarkMaterialPdf } from "../services/watermarkPdf";
import { createJobTempDir } from "../utils/tempFiles";

export async function watermarkMaterialPdfJob(payload: MaterialPdfWatermarkJob, attempt: number) {
  const startedAt = performance.now();
  const context = await getMaterialDownloadContext(payload.jobId, payload.materialId, payload.userId);

  if (context.job.status === "ready") {
    return { idempotent: true, durationMs: 0, outputBytes: 0 };
  }
  if (context.job.status === "expired" || new Date(context.job.expiresAt) <= new Date()) {
    throw new Error("Material download job expired before processing");
  }

  const sizeHint = context.material.fileSizeBytes;
  if (sizeHint != null && sizeHint > config.materialDownloads.maxPdfBytes) {
    throw new Error(`Material PDF exceeds watermark limit: ${sizeHint}`);
  }

  await markMaterialDownloadProcessing(payload.jobId, attempt);
  const temp = await createJobTempDir(payload.jobId);

  try {
    const source = context.material.url.trim();
    const download = source.startsWith("http://") || source.startsWith("https://")
      ? await downloadHttpFile({ url: source, destinationPath: temp.inputPath })
      : await downloadStorageFile({
          bucket: config.materialDownloads.sourceBucket,
          storagePath: source,
          destinationPath: temp.inputPath,
        });

    if (download.bytes > config.materialDownloads.maxPdfBytes) {
      throw new Error(`Material PDF exceeds watermark limit: ${download.bytes}`);
    }

    const downloadedAtLabel = new Intl.DateTimeFormat("pt-BR", {
      timeZone: "America/Sao_Paulo",
      dateStyle: "short",
      timeStyle: "medium",
    }).format(new Date(context.job.requestedAt));

    const output = await watermarkMaterialPdf({
      inputPath: temp.inputPath,
      outputPath: temp.outputPath,
      displayName: context.profile.name?.trim() || context.profile.email || payload.userId,
      email: context.profile.email || "",
      phone: context.profile.phone,
      userId: payload.userId,
      downloadedAtLabel,
      materialId: payload.materialId,
      jobId: payload.jobId,
    });

    const upload = await uploadStorageFile({
      bucket: config.materialDownloads.outputBucket,
      storagePath: context.job.outputPath,
      sourcePath: temp.outputPath,
      contentType: "application/pdf",
    });
    await markMaterialDownloadReady(payload.jobId, output.bytes, attempt);

    return {
      idempotent: false,
      outputBytes: output.bytes,
      pageCount: output.pageCount,
      downloadMs: download.downloadMs,
      uploadMs: upload.uploadMs,
      durationMs: Math.round(performance.now() - startedAt),
    };
  } finally {
    await temp.cleanup();
  }
}
