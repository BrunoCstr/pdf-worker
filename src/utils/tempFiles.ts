import { mkdtemp, rm } from "node:fs/promises";
import { join } from "node:path";
import { tmpdir } from "node:os";

export type JobTempDir = {
  dir: string;
  inputPath: string;
  outputPath: string;
  cleanup: () => Promise<void>;
};

export async function createJobTempDir(fileId: string): Promise<JobTempDir> {
  const dir = await mkdtemp(join(tmpdir(), `pdf-worker-${fileId}-`));

  return {
    dir,
    inputPath: join(dir, "input.pdf"),
    outputPath: join(dir, "optimized.pdf"),
    cleanup: async () => {
      await rm(dir, { recursive: true, force: true });
    },
  };
}
