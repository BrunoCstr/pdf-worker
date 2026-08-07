import { readFile, stat, writeFile } from "node:fs/promises";

import { degrees, PDFDocument, rgb, StandardFonts } from "pdf-lib";

function asciiSafe(value: string, max: number): string {
  const normalized = value.normalize("NFD").replace(/\p{M}/gu, "");
  const ascii = normalized.replace(/[^\x20-\x7E]/g, "?");
  return ascii.length > max ? `${ascii.slice(0, Math.max(0, max - 3))}...` : ascii;
}

export async function watermarkMaterialPdf(options: {
  inputPath: string;
  outputPath: string;
  displayName: string;
  email: string;
  phone: string | null;
  userId: string;
  downloadedAtLabel: string;
  materialId: string;
  jobId: string;
}): Promise<{ bytes: number; pageCount: number }> {
  const input = await readFile(options.inputPath);
  const document = await PDFDocument.load(input, { ignoreEncryption: true });
  const font = await document.embedFont(StandardFonts.Helvetica);

  const line1 = asciiSafe(
    `${options.displayName} | ${options.email} | ID ${options.userId}`,
    220,
  );
  const line2 = asciiSafe(
    `Tel: ${options.phone ?? "-"} | ${options.downloadedAtLabel}`,
    220,
  );
  const stamp = `${line1}  -  ${line2}`;

  const pages = document.getPages();
  for (const page of pages) {
    const { width, height } = page.getSize();
    const size = Math.min(11, Math.max(7, width / 55));
    page.drawText(stamp, {
      x: width * 0.06,
      y: height * 0.52,
      size,
      font,
      color: rgb(0.5, 0.5, 0.5),
      opacity: 0.16,
      rotate: degrees(-32),
    });
    page.drawText(stamp, {
      x: width * 0.28,
      y: height * 0.22,
      size,
      font,
      color: rgb(0.5, 0.5, 0.5),
      opacity: 0.12,
      rotate: degrees(-32),
    });
  }

  document.setKeywords([
    `userId:${options.userId}`,
    `ts:${new Date().toISOString()}`,
    `materialId:${options.materialId}`,
    `jobId:${options.jobId}`,
  ]);
  document.setProducer("Tropa do SOI");

  await writeFile(options.outputPath, await document.save());
  return { bytes: (await stat(options.outputPath)).size, pageCount: pages.length };
}
