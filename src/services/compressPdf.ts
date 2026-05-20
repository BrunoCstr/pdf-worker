import { spawn } from "node:child_process";
import { stat } from "node:fs/promises";
import { performance } from "node:perf_hooks";

import { config } from "../config";

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
}): Promise<CompressPdfResult> {
  const startedAt = performance.now();
  const originalStat = await stat(options.inputPath);

  await runGhostscript(options.inputPath, options.outputPath);

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

async function runGhostscript(inputPath: string, outputPath: string): Promise<void> {
  const args = [
    "-sDEVICE=pdfwrite",
    "-dCompatibilityLevel=1.7",
    "-dNOPAUSE",
    "-dQUIET",
    "-dBATCH",
    "-dSAFER",
    "-dDetectDuplicateImages=true",
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
    const timeout = setTimeout(() => {
      timedOut = true;
      child.kill("SIGTERM");
    }, config.limits.ghostscriptTimeoutMs);

    child.stderr.on("data", (chunk: Buffer) => {
      stderr += chunk.toString("utf8");

      if (stderr.length > 8_192) {
        stderr = stderr.slice(-8_192);
      }
    });

    child.on("error", (error) => {
      clearTimeout(timeout);
      reject(error);
    });

    child.on("close", (code, signal) => {
      clearTimeout(timeout);

      if (timedOut) {
        reject(new Error(`Ghostscript timed out after ${config.limits.ghostscriptTimeoutMs}ms`));
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
