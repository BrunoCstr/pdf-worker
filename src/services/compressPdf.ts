import { spawn } from "node:child_process";
import { stat } from "node:fs/promises";
import { performance } from "node:perf_hooks";

import { config } from "../config";
import { JobCancelledError } from "../errors";

export const GHOSTSCRIPT_SETTING = "ghostscript-study";

export type CompressPdfResult = {
  originalSize: number;
  compressedSize: number;
  reductionRatio: number;
  applied: boolean;
  setting: typeof GHOSTSCRIPT_SETTING;
  ghostscriptMs: number;
};

export type GhostscriptProgress = {
  /** 1-based page index Ghostscript is currently writing. */
  currentPage: number;
  /** Total page count parsed from "Processing pages 1 through N.". Null until the line is seen. */
  totalPages: number | null;
};

export async function compressPdfWithGhostscript(options: {
  inputPath: string;
  outputPath: string;
  timeoutMs?: number;
  signal?: AbortSignal;
  onProgress?: (progress: GhostscriptProgress) => void;
}): Promise<CompressPdfResult> {
  const startedAt = performance.now();
  const originalStat = await stat(options.inputPath);

  await runGhostscript(
    options.inputPath,
    options.outputPath,
    options.timeoutMs,
    options.signal,
    options.onProgress,
  );

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

const PROCESSING_PAGES_RE = /Processing pages \d+ through (\d+)\./;
const PAGE_RE = /Page (\d+)/g;

async function runGhostscript(
  inputPath: string,
  outputPath: string,
  timeoutMs: number = config.limits.ghostscriptTimeoutMs,
  signal?: AbortSignal,
  onProgress?: (progress: GhostscriptProgress) => void,
): Promise<void> {
  const args = [
    "-sDEVICE=pdfwrite",
    "-dCompatibilityLevel=1.7",
    "-dNOPAUSE",
    "-dBATCH",
    "-dSAFER",
    // Redirect Ghostscript's stdout (where "Page N" lines normally go) to stderr
    // so we have a single stream to parse for progress AND for error context.
    "-sstdout=%stderr",
    ...(config.ghostscript.detectDuplicateImages ? ["-dDetectDuplicateImages=true"] : []),
    ...(config.ghostscript.numRenderingThreads
      ? [`-dNumRenderingThreads=${config.ghostscript.numRenderingThreads}`]
      : []),
    "-dCompressFonts=true",
    "-dSubsetFonts=true",
    // Auto filtering: GS chooses JPEG (DCT) for photographic content and
    // Flate (lossless ZIP-like) for diagrams/line-art. Critical for medical
    // study material where charts and illustrations must stay lossless.
    `-dAutoFilterColorImages=${config.ghostscript.autoFilterColorImages}`,
    `-dAutoFilterGrayImages=${config.ghostscript.autoFilterGrayImages}`,
    "-dDownsampleColorImages=true",
    "-dDownsampleGrayImages=true",
    "-dDownsampleMonoImages=true",
    `-dColorImageDownsampleType=/${config.ghostscript.colorImageDownsampleType}`,
    `-dGrayImageDownsampleType=/${config.ghostscript.grayImageDownsampleType}`,
    `-dMonoImageDownsampleType=/${config.ghostscript.monoImageDownsampleType}`,
    `-dColorImageResolution=${config.ghostscript.colorImageResolution}`,
    `-dGrayImageResolution=${config.ghostscript.grayImageResolution}`,
    `-dMonoImageResolution=${config.ghostscript.monoImageResolution}`,
    `-dJPEGQ=${config.ghostscript.jpegQuality}`,
    // Preserve original color space — important for ECG colors, MRI heatmaps,
    // histology stains, and any color-coded diagrams.
    `-sColorConversionStrategy=/${config.ghostscript.colorConversionStrategy}`,
    // Keep annotations (highlights, sticky notes) that students may have added.
    `-dPreserveAnnots=${config.ghostscript.preserveAnnots}`,
    `-sOutputFile=${outputPath}`,
    inputPath,
  ];

  await new Promise<void>((resolve, reject) => {
    const child = spawn(config.ghostscriptBinary, args, {
      stdio: ["ignore", "ignore", "pipe"],
    });

    let stderrTail = "";
    let stderrBuffer = "";
    let totalPages: number | null = null;
    let currentPage = 0;
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
      const text = chunk.toString("utf8");

      stderrTail += text;
      if (stderrTail.length > 8_192) {
        stderrTail = stderrTail.slice(-8_192);
      }

      if (!onProgress) {
        return;
      }

      stderrBuffer += text;

      // Cap the parse buffer to prevent unbounded growth on long runs.
      if (stderrBuffer.length > 16_384) {
        stderrBuffer = stderrBuffer.slice(-16_384);
      }

      let progressChanged = false;

      if (totalPages === null) {
        const match = PROCESSING_PAGES_RE.exec(stderrBuffer);
        if (match) {
          totalPages = Number.parseInt(match[1] ?? "", 10) || null;
          progressChanged = totalPages !== null;
        }
      }

      PAGE_RE.lastIndex = 0;
      let pageMatch: RegExpExecArray | null;
      let lastPage = currentPage;
      while ((pageMatch = PAGE_RE.exec(stderrBuffer)) !== null) {
        const pageNum = Number.parseInt(pageMatch[1] ?? "", 10);
        if (Number.isFinite(pageNum) && pageNum > lastPage) {
          lastPage = pageNum;
        }
      }

      if (lastPage > currentPage) {
        currentPage = lastPage;
        progressChanged = true;
      }

      if (progressChanged) {
        try {
          onProgress({ currentPage, totalPages });
        } catch {
          // Progress callbacks must never crash the GS pipeline.
        }
      }
    });

    child.on("error", (error) => {
      rejectOnce(error);
    });

    child.on("close", (code, sig) => {
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
          `Ghostscript failed with code ${code ?? "unknown"} and signal ${sig ?? "none"}: ${stderrTail.trim()}`,
        ),
      );
    });
  });
}
