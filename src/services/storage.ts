import { createWriteStream, createReadStream } from "node:fs";
import { stat } from "node:fs/promises";
import { Readable } from "node:stream";
import type { ReadableStream as NodeReadableStream } from "node:stream/web";
import { pipeline } from "node:stream/promises";
import { performance } from "node:perf_hooks";

import { config } from "../config";
import { supabase } from "./supabase";

export type StorageDownloadResult = {
  bytes: number;
  downloadMs: number;
};

export type StorageUploadResult = {
  uploadMs: number;
};

export async function downloadStorageFile(options: {
  bucket: string;
  storagePath: string;
  destinationPath: string;
}): Promise<StorageDownloadResult> {
  const startedAt = performance.now();
  const { data, error } = await supabase.storage
    .from(options.bucket)
    .createSignedUrl(options.storagePath, 60);

  if (error || !data?.signedUrl) {
    throw new Error(`Failed to create signed download URL: ${error?.message ?? "missing signedUrl"}`);
  }

  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), config.limits.downloadTimeoutMs);

  try {
    const response = await fetch(data.signedUrl, { signal: controller.signal });

    if (!response.ok || !response.body) {
      throw new Error(`Storage download failed with status ${response.status}`);
    }

    await pipeline(
      Readable.fromWeb(response.body as unknown as NodeReadableStream<Uint8Array>),
      createWriteStream(options.destinationPath),
    );
    const fileStat = await stat(options.destinationPath);

    return {
      bytes: fileStat.size,
      downloadMs: Math.round(performance.now() - startedAt),
    };
  } catch (error) {
    if (error instanceof Error && error.name === "AbortError") {
      throw new Error(`Storage download timed out after ${config.limits.downloadTimeoutMs}ms`);
    }

    throw error;
  } finally {
    clearTimeout(timeout);
  }
}

export async function uploadStorageFile(options: {
  bucket: string;
  storagePath: string;
  sourcePath: string;
  contentType: string;
}): Promise<StorageUploadResult> {
  const startedAt = performance.now();
  const { error } = await supabase.storage.from(options.bucket).upload(
    options.storagePath,
    createReadStream(options.sourcePath),
    {
      contentType: options.contentType,
      upsert: true,
    },
  );

  if (error) {
    throw new Error(`Storage upload failed: ${error.message}`);
  }

  return {
    uploadMs: Math.round(performance.now() - startedAt),
  };
}
