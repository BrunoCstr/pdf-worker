import { spawn } from "node:child_process";
import { stat, writeFile } from "node:fs/promises";
import { performance } from "node:perf_hooks";

import { config } from "../config";
import { JobCancelledError } from "../errors";
import { createJobTempDir } from "../utils/tempFiles";

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

/**
 * Builds the Ghostscript argv used to compress a single PDF.
 *
 * Exported (via {@link smokeTestGhostscript}) so the same args can be exercised
 * at worker startup against a synthetic 1-page PDF — this catches malformed
 * flags (like the `-sColorConversionStrategy=/...` rangecheck) before any real
 * job hits production.
 */
function buildGhostscriptArgs(inputPath: string, outputPath: string): string[] {
  return [
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
    // histology stains, and any color-coded diagrams. Must use -d (not -s) so
    // GS parses the value as a PostScript /name literal; with -s, the leading
    // slash gets included in the string and triggers a rangecheck in
    // .putdeviceprops because the resulting "/LeaveColorUnchanged" doesn't
    // match any known strategy.
    `-dColorConversionStrategy=/${config.ghostscript.colorConversionStrategy}`,
    // Keep annotations (highlights, sticky notes) that students may have added.
    `-dPreserveAnnots=${config.ghostscript.preserveAnnots}`,
    `-sOutputFile=${outputPath}`,
    inputPath,
  ];
}

async function runGhostscript(
  inputPath: string,
  outputPath: string,
  timeoutMs: number = config.limits.ghostscriptTimeoutMs,
  signal?: AbortSignal,
  onProgress?: (progress: GhostscriptProgress) => void,
): Promise<void> {
  const args = buildGhostscriptArgs(inputPath, outputPath);

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

      // Strip filesystem paths so the args list is safe to attach to the
      // error message (avoids leaking tmp paths into logs/DB).
      const debugArgs = args
        .filter((arg) => arg !== inputPath && !arg.startsWith("-sOutputFile="))
        .join(" ");

      reject(
        new Error(
          `Ghostscript failed with code ${code ?? "unknown"} and signal ${sig ?? "none"}: ${stderrTail.trim()} [gs args: ${debugArgs}]`,
        ),
      );
    });
  });
}

/**
 * Builds a structurally valid 1-page blank PDF in memory.
 *
 * We compute the xref byte offsets dynamically so the document is always
 * well-formed regardless of how the body string is encoded by the runtime —
 * a hard-coded PDF with fixed offsets would silently drift on any change.
 */
function buildMinimalPdf(): Buffer {
  const objects = [
    "<< /Type /Catalog /Pages 2 0 R >>",
    "<< /Type /Pages /Kids [3 0 R] /Count 1 >>",
    "<< /Type /Page /Parent 2 0 R /MediaBox [0 0 595 842] /Resources << >> >>",
  ];

  let body = "%PDF-1.4\n%\u00e2\u00e3\u00cf\u00d3\n";
  const offsets: number[] = [];

  objects.forEach((obj, index) => {
    offsets.push(Buffer.byteLength(body, "latin1"));
    body += `${index + 1} 0 obj\n${obj}\nendobj\n`;
  });

  const xrefOffset = Buffer.byteLength(body, "latin1");
  body += `xref\n0 ${objects.length + 1}\n`;
  body += "0000000000 65535 f \n";
  for (const offset of offsets) {
    body += `${offset.toString().padStart(10, "0")} 00000 n \n`;
  }

  body += `trailer\n<< /Size ${objects.length + 1} /Root 1 0 R >>\n`;
  body += `startxref\n${xrefOffset}\n%%EOF\n`;

  return Buffer.from(body, "latin1");
}

export type SmokeTestResult = {
  durationMs: number;
  inputBytes: number;
  outputBytes: number;
};

/**
 * Smoke test: runs Ghostscript with the production argv against a synthetic
 * 1-page PDF. Fails fast if any flag is malformed for the installed GS version
 * (e.g. a future invalid value, deprecated parameter, or path issue), so
 * misconfiguration is caught at worker boot — never on a real user job.
 */
export async function smokeTestGhostscript(): Promise<SmokeTestResult> {
  const startedAt = performance.now();
  const temp = await createJobTempDir("smoke-test");

  try {
    const minimalPdf = buildMinimalPdf();
    await writeFile(temp.inputPath, minimalPdf);

    await runGhostscript(temp.inputPath, temp.outputPath, 30_000);

    const outputStat = await stat(temp.outputPath);

    return {
      durationMs: Math.round(performance.now() - startedAt),
      inputBytes: minimalPdf.byteLength,
      outputBytes: outputStat.size,
    };
  } finally {
    await temp.cleanup().catch(() => undefined);
  }
}
