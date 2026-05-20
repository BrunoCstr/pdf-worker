import { supabase } from "./supabase";

export type ProcessingStatus = "pending" | "processing" | "ready" | "failed" | "skipped";

export type CompressionAuditMetadata = {
  applied: boolean;
  setting: string;
  original_size: number;
  compressed_size: number;
  reduction_ratio: number;
  queue_wait_ms: number;
  duration_ms: number;
  download_ms?: number;
  ghostscript_ms?: number;
  upload_ms?: number;
  message?: string;
  error?: string;
};

export async function markFileProcessing(fileId: string): Promise<void> {
  await updateFile(fileId, {
    processing_status: "processing",
    compression_message: null,
    updated_at: new Date().toISOString(),
  });
}

export async function markFileReadyApplied(options: {
  fileId: string;
  sizeBytes: number;
  compressionRatio: number;
  compressionSetting: string;
}): Promise<void> {
  await updateFile(options.fileId, {
    size_bytes: options.sizeBytes,
    compression_ratio: options.compressionRatio,
    compression_setting: options.compressionSetting,
    compression_message: null,
    processing_status: "ready",
    processed_at: new Date().toISOString(),
    updated_at: new Date().toISOString(),
  });
}

export async function markFileReadyNotSmaller(fileId: string, message = "not_smaller"): Promise<void> {
  await updateFile(fileId, {
    compression_ratio: null,
    compression_setting: null,
    compression_message: message,
    processing_status: "ready",
    processed_at: new Date().toISOString(),
    updated_at: new Date().toISOString(),
  });
}

export async function markFileSkipped(fileId: string, message: string): Promise<void> {
  await updateFile(fileId, {
    compression_ratio: null,
    compression_setting: null,
    compression_message: message,
    processing_status: "skipped",
    processed_at: new Date().toISOString(),
    updated_at: new Date().toISOString(),
  });
}

export async function markFileFailed(fileId: string, message: string): Promise<void> {
  await updateFile(fileId, {
    compression_message: message,
    processing_status: "failed",
    processed_at: new Date().toISOString(),
    updated_at: new Date().toISOString(),
  });
}

export async function adjustUserStorageUsedBytes(userId: string, deltaBytes: number): Promise<void> {
  if (deltaBytes === 0) {
    return;
  }

  const { error } = await supabase.rpc("increment_user_storage_used_bytes", {
    p_user_id: userId,
    p_delta_bytes: deltaBytes,
  });

  if (error) {
    throw new Error(`Failed to adjust user storage quota: ${error.message}`);
  }
}

export async function insertCompressionAuditLog(options: {
  fileId: string;
  userId: string;
  metadata: CompressionAuditMetadata;
}): Promise<void> {
  const { error } = await supabase.from("file_audit_logs").insert({
    file_id: options.fileId,
    user_id: options.userId,
    action: "compress",
    metadata: options.metadata,
  });

  if (error) {
    throw new Error(`Failed to insert compression audit log: ${error.message}`);
  }
}

async function updateFile(fileId: string, patch: Record<string, unknown>): Promise<void> {
  const { error } = await supabase.from("files").update(patch).eq("id", fileId);

  if (error) {
    throw new Error(`Failed to update files row ${fileId}: ${error.message}`);
  }
}
