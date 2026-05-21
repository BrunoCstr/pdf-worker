import { supabase } from "./supabase";

export type PdfJobStatus = "queued" | "downloading" | "compressing" | "uploading" | "completed" | "failed" | "skipped";

export async function markPdfJobDownloading(
  jobId: string,
  options: { queueWaitMs: number; attempt: number },
): Promise<void> {
  await updatePdfJob(jobId, {
    status: "downloading",
    started_at: new Date().toISOString(),
    queue_wait_ms: options.queueWaitMs,
    attempt: options.attempt,
    progress: 5,
  });
}

export async function markPdfJobCompressing(jobId: string): Promise<void> {
  await updatePdfJob(jobId, {
    status: "compressing",
    progress: 30,
  });
}

export async function markPdfJobUploading(jobId: string): Promise<void> {
  await updatePdfJob(jobId, {
    status: "uploading",
    progress: 80,
  });
}

export async function markPdfJobCompleted(
  jobId: string,
  options: {
    compressedSizeBytes: number;
    compressionRatio: number | null;
    compressionSetting: string | null;
    compressionMessage: string | null;
    processingMs: number;
  },
): Promise<void> {
  await updatePdfJob(jobId, {
    status: "completed",
    progress: 100,
    compressed_size_bytes: options.compressedSizeBytes,
    compression_ratio: options.compressionRatio,
    compression_setting: options.compressionSetting,
    compression_message: options.compressionMessage,
    processing_ms: options.processingMs,
    completed_at: new Date().toISOString(),
  });
}

export async function markPdfJobFailed(
  jobId: string,
  errorMessage: string,
  attempt: number,
): Promise<void> {
  const { error } = await supabase
    .from("drive_pdf_jobs")
    .update({
      status: "failed",
      error_message: errorMessage,
      attempt,
      completed_at: new Date().toISOString(),
    })
    .eq("id", jobId)
    .not("status", "in", '("completed","skipped")');

  if (error) {
    throw new Error(`Failed to update drive_pdf_jobs row ${jobId}: ${error.message}`);
  }
}

export async function markPdfJobSkipped(jobId: string, compressionMessage: string): Promise<void> {
  await updatePdfJob(jobId, {
    status: "skipped",
    progress: 100,
    compression_message: compressionMessage,
    completed_at: new Date().toISOString(),
  });
}

async function updatePdfJob(jobId: string, patch: Record<string, unknown>): Promise<void> {
  const { error } = await supabase.from("drive_pdf_jobs").update(patch).eq("id", jobId);

  if (error) {
    throw new Error(`Failed to update drive_pdf_jobs row ${jobId}: ${error.message}`);
  }
}
