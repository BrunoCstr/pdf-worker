import assert from "node:assert/strict";
import { mkdtemp, readFile, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import test from "node:test";

import { PDFDocument, StandardFonts } from "pdf-lib";

import { watermarkMaterialPdf } from "./watermarkPdf";

test("watermarks every page and preserves trace metadata", async () => {
  const tempDir = await mkdtemp(join(tmpdir(), "material-watermark-test-"));
  const inputPath = join(tempDir, "input.pdf");
  const outputPath = join(tempDir, "output.pdf");

  try {
    const source = await PDFDocument.create();
    const font = await source.embedFont(StandardFonts.Helvetica);
    for (let pageIndex = 0; pageIndex < 2; pageIndex += 1) {
      const page = source.addPage([595, 842]);
      page.drawText(`Pagina ${pageIndex + 1}`, { x: 50, y: 780, size: 18, font });
    }
    await writeFile(inputPath, await source.save());

    const result = await watermarkMaterialPdf({
      inputPath,
      outputPath,
      displayName: "Aluno Teste",
      email: "aluno@example.com",
      phone: "11999999999",
      userId: "11111111-1111-1111-1111-111111111111",
      downloadedAtLabel: "06/08/2026 15:00:00",
      materialId: "22222222-2222-2222-2222-222222222222",
      jobId: "33333333-3333-3333-3333-333333333333",
    });

    const output = await PDFDocument.load(await readFile(outputPath), { updateMetadata: false });
    assert.equal(result.pageCount, 2);
    assert.equal(output.getPageCount(), 2);
    assert.equal(output.getProducer(), "Tropa do SOI");
    assert.match(output.getKeywords() ?? "", /userId:11111111/);
    assert.match(output.getKeywords() ?? "", /materialId:22222222/);
    assert.ok(result.bytes > 0);
  } finally {
    await rm(tempDir, { recursive: true, force: true });
  }
});
