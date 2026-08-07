import { supabase } from "./supabase";

export type MaterialDownloadStatus = "queued" | "processing" | "ready" | "failed" | "expired";

export type MaterialDownloadContext = {
  job: {
    id: string;
    materialId: string;
    userId: string;
    status: MaterialDownloadStatus;
    outputPath: string;
    requestedAt: string;
    expiresAt: string;
  };
  material: { url: string; fileSizeBytes: number | null };
  profile: { name: string | null; email: string | null; phone: string | null };
};

export async function getMaterialDownloadContext(
  jobId: string,
  materialId: string,
  userId: string,
): Promise<MaterialDownloadContext> {
  const { data: job, error: jobError } = await supabase
    .from("material_download_jobs")
    .select("id, material_id, user_id, status, output_path, requested_at, expires_at")
    .eq("id", jobId)
    .maybeSingle();

  if (jobError || !job) throw new Error(`Material download job not found: ${jobError?.message ?? jobId}`);
  if (job.material_id !== materialId || job.user_id !== userId) {
    throw new Error("Material job identity does not match queue payload");
  }

  const [{ data: material, error: materialError }, { data: profile, error: profileError }] =
    await Promise.all([
      supabase.from("materials").select("url, file_size_bytes").eq("id", materialId).maybeSingle(),
      supabase.from("users").select("name, email, phone").eq("id", userId).maybeSingle(),
    ]);

  if (materialError || !material?.url) {
    throw new Error(`Material source not found: ${materialError?.message ?? materialId}`);
  }
  if (profileError || !profile) {
    throw new Error(`Material user profile not found: ${profileError?.message ?? userId}`);
  }

  return {
    job: {
      id: job.id,
      materialId: job.material_id,
      userId: job.user_id,
      status: job.status as MaterialDownloadStatus,
      outputPath: job.output_path,
      requestedAt: job.requested_at,
      expiresAt: job.expires_at,
    },
    material: { url: material.url, fileSizeBytes: material.file_size_bytes },
    profile: { name: profile.name, email: profile.email, phone: profile.phone },
  };
}

export async function markMaterialDownloadProcessing(jobId: string, attempt: number): Promise<void> {
  await updateJob(jobId, {
    status: "processing",
    attempt,
    started_at: new Date().toISOString(),
    error_message: null,
    updated_at: new Date().toISOString(),
  });
}

export async function markMaterialDownloadQueuedForRetry(
  jobId: string,
  attempt: number,
  message: string,
): Promise<void> {
  await updateJob(jobId, {
    status: "queued",
    attempt,
    error_message: message,
    updated_at: new Date().toISOString(),
  });
}

export async function markMaterialDownloadReady(
  jobId: string,
  fileSizeBytes: number,
  attempt: number,
): Promise<void> {
  await updateJob(jobId, {
    status: "ready",
    file_size_bytes: fileSizeBytes,
    attempt,
    error_message: null,
    ready_at: new Date().toISOString(),
    updated_at: new Date().toISOString(),
  });
}

export async function markMaterialDownloadFailed(
  jobId: string,
  attempt: number,
  message: string,
): Promise<void> {
  await updateJob(jobId, {
    status: "failed",
    attempt,
    error_message: message,
    updated_at: new Date().toISOString(),
  });
}

export async function listExpiredMaterialDownloads(limit = 100): Promise<Array<{ id: string; outputPath: string }>> {
  const { data, error } = await supabase
    .from("material_download_jobs")
    .select("id, output_path")
    .eq("status", "ready")
    .lt("expires_at", new Date().toISOString())
    .limit(limit);

  if (error) throw new Error(`Failed to list expired material downloads: ${error.message}`);
  return (data ?? []).map((row) => ({ id: row.id, outputPath: row.output_path }));
}

export async function markMaterialDownloadsExpired(ids: string[]): Promise<void> {
  if (ids.length === 0) return;
  const { error } = await supabase
    .from("material_download_jobs")
    .update({ status: "expired", updated_at: new Date().toISOString() })
    .in("id", ids)
    .eq("status", "ready");
  if (error) throw new Error(`Failed to expire material downloads: ${error.message}`);
}

async function updateJob(jobId: string, patch: Record<string, unknown>): Promise<void> {
  const { error } = await supabase
    .from("material_download_jobs")
    .update(patch)
    .eq("id", jobId)
    .neq("status", "expired");
  if (error) throw new Error(`Failed to update material download job ${jobId}: ${error.message}`);
}
