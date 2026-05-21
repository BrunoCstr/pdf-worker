import { logger } from "../utils/logger";
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
    error_message: null,
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
  const { data, error } = await supabase
    .from("drive_pdf_jobs")
    .update({
      status: "failed",
      error_message: errorMessage,
      attempt,
      completed_at: new Date().toISOString(),
    })
    .eq("id", jobId)
    .neq("status", "completed")
    .neq("status", "skipped")
    .select("id");

  if (error) {
    logger.error({ err: error, jobId }, "markPdfJobFailed: supabase error");
    throw new Error(`Failed to update drive_pdf_jobs row ${jobId}: ${error.message}`);
  }

  if (!data || data.length === 0) {
    // Query the current status to explain WHY no rows were affected.
    const { data: current } = await supabase
      .from("drive_pdf_jobs")
      .select("status, attempt")
      .eq("id", jobId)
      .maybeSingle();

    logger.error(
      { jobId, currentStatus: current?.status ?? "row_not_found", currentAttempt: current?.attempt },
      "markPdfJobFailed: 0 rows updated — row missing or status is already terminal",
    );

    throw new Error(
      `markPdfJobFailed: no rows updated for job ${jobId} — status=${current?.status ?? "not_found"}`,
    );
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

export async function updatePdfJobProgress(jobId: string, progress: number): Promise<void> {
  await supabase.from("drive_pdf_jobs").update({ progress }).eq("id", jobId);
}

async function updatePdfJob(jobId: string, patch: Record<string, unknown>): Promise<void> {
  const { error } = await supabase.from("drive_pdf_jobs").update(patch).eq("id", jobId);

  if (error) {
    throw new Error(`Failed to update drive_pdf_jobs row ${jobId}: ${error.message}`);
  }
}
