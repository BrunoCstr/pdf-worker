import { spawn } from "node:child_process";
import { stat } from "node:fs/promises";
import { performance } from "node:perf_hooks";

import { config } from "../config";
import { JobCancelledError } from "../errors";

export const GHOSTSCRIPT_SETTING = "ghostscript-300dpi";

export type CompressPdfResult = {
  originalSize: number;
  compressedSize: number;
  reductionRatio: number;
  applied: boolean;
  setting: typeof GHOSTSCRIPT_SETTING;
  ghostscriptMs: number;
};

export async function compressPdfWithGhostscript(options: {
  inputPath: string;
  outputPath: string;
  timeoutMs?: number;
  signal?: AbortSignal;
}): Promise<CompressPdfResult> {
  const startedAt = performance.now();
  const originalStat = await stat(options.inputPath);

  await runGhostscript(options.inputPath, options.outputPath, options.timeoutMs, options.signal);

  const compressedStat = await stat(options.outputPath);
  const reductionRatio = (originalStat.size - compressedStat.size) / originalStat.size;

  return {
    originalSize: originalStat.size,
    compressedSize: compressedStat.size,
    reductionRatio,
    applied:
      compressedStat.size < originalStat.size &&
      reductionRatio >= config.limits.minCompressionReduction,
    setting: GHOSTSCRIPT_SETTING,
    ghostscriptMs: Math.round(performance.now() - startedAt),
  };
}

async function runGhostscript(
  inputPath: string,
  outputPath: string,
  timeoutMs: number = config.limits.ghostscriptTimeoutMs,
  signal?: AbortSignal,
): Promise<void> {
  const args = [
    "-sDEVICE=pdfwrite",
    "-dCompatibilityLevel=1.7",
    "-dNOPAUSE",
    "-dQUIET",
    "-dBATCH",
    "-dSAFER",
    ...(config.ghostscript.detectDuplicateImages ? ["-dDetectDuplicateImages=true"] : []),
    ...(config.ghostscript.numRenderingThreads
      ? [`-dNumRenderingThreads=${config.ghostscript.numRenderingThreads}`]
      : []),
    "-dCompressFonts=true",
    "-dSubsetFonts=true",
    "-dDownsampleColorImages=true",
    "-dDownsampleGrayImages=true",
    "-dDownsampleMonoImages=true",
    "-dColorImageDownsampleType=/Bicubic",
    "-dGrayImageDownsampleType=/Bicubic",
    "-dMonoImageDownsampleType=/Subsample",
    "-dColorImageResolution=300",
    "-dGrayImageResolution=300",
    "-dMonoImageResolution=300",
    "-dJPEGQ=92",
    `-sOutputFile=${outputPath}`,
    inputPath,
  ];

  await new Promise<void>((resolve, reject) => {
    const child = spawn(config.ghostscriptBinary, args, {
      stdio: ["ignore", "ignore", "pipe"],
    });

    let stderr = "";
    let timedOut = false;
    let settled = false;
    let keepKillTimer = false;
    let timeout: NodeJS.Timeout;
    let killTimer: NodeJS.Timeout | undefined;

    const cleanup = () => {
      clearTimeout(timeout);
      if (!keepKillTimer) {
        clearTimeout(killTimer);
      }
      signal?.removeEventListener("abort", abortGhostscript);
    };

    const rejectOnce = (error: Error) => {
      if (settled) {
        return;
      }

      settled = true;
      cleanup();
      reject(error);
    };

    const abortGhostscript = () => {
      const reason = signal?.reason instanceof Error ? signal.reason : new JobCancelledError();
      child.kill("SIGTERM");
      keepKillTimer = true;
      killTimer = setTimeout(() => child.kill("SIGKILL"), 5_000);
      rejectOnce(reason);
    };

    timeout = setTimeout(() => {
      timedOut = true;
      child.kill("SIGTERM");
      // If GS ignores SIGTERM (e.g. blocked on disk I/O), force-kill after 5s
      // to guarantee the promise always settles and the worker is never frozen.
      killTimer = setTimeout(() => child.kill("SIGKILL"), 5_000);
    }, timeoutMs);

    if (signal?.aborted) {
      abortGhostscript();
      return;
    }

    signal?.addEventListener("abort", abortGhostscript, { once: true });

    child.stderr.on("data", (chunk: Buffer) => {
      stderr += chunk.toString("utf8");

      if (stderr.length > 8_192) {
        stderr = stderr.slice(-8_192);
      }
    });

    child.on("error", (error) => {
      rejectOnce(error);
    });

    child.on("close", (code, signal) => {
      if (settled) {
        return;
      }

      settled = true;
      cleanup();

      if (timedOut) {
        reject(new Error(`Ghostscript timed out after ${timeoutMs}ms`));
        return;
      }

      if (code === 0) {
        resolve();
        return;
      }

      reject(
        new Error(
          `Ghostscript failed with code ${code ?? "unknown"} and signal ${signal ?? "none"}: ${stderr.trim()}`,
        ),
      );
    });
  });
}
