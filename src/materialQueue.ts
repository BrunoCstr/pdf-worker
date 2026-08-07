import { createHmac, timingSafeEqual } from "node:crypto";

import { config } from "./config";

export type MaterialPdfWatermarkJob = {
  jobId: string;
  materialId: string;
  userId: string;
  enqueuedAt: string;
  signature?: string;
};

export function validateMaterialJobData(data: unknown): asserts data is MaterialPdfWatermarkJob {
  if (!data || typeof data !== "object") throw new Error("Material job payload must be an object");
  const payload = data as Partial<MaterialPdfWatermarkJob>;

  if (!payload.jobId || typeof payload.jobId !== "string") throw new Error("Missing material jobId");
  if (!payload.materialId || typeof payload.materialId !== "string") throw new Error("Missing materialId");
  if (!payload.userId || typeof payload.userId !== "string") throw new Error("Missing material userId");
  if (!payload.enqueuedAt || Number.isNaN(Date.parse(payload.enqueuedAt))) {
    throw new Error("Material enqueuedAt must be a valid ISO timestamp");
  }

  if (!config.workerApiSecret) return;
  if (!payload.signature) throw new Error("Material job signature is required");

  const expected = signMaterialPayload(payload as MaterialPdfWatermarkJob, config.workerApiSecret);
  const providedBuffer = Buffer.from(payload.signature, "hex");
  const expectedBuffer = Buffer.from(expected, "hex");
  if (providedBuffer.length !== expectedBuffer.length || !timingSafeEqual(providedBuffer, expectedBuffer)) {
    throw new Error("Invalid material job signature");
  }
}

function signMaterialPayload(payload: MaterialPdfWatermarkJob, secret: string): string {
  return createHmac("sha256", secret)
    .update(`${payload.jobId}|${payload.userId}|${payload.materialId}|${payload.enqueuedAt}`)
    .digest("hex");
}
