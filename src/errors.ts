export class JobCancelledError extends Error {
  constructor(message = "Job cancelled by user") {
    super(message);
    this.name = "JobCancelledError";
  }
}

export function isJobCancelledError(error: unknown): error is JobCancelledError {
  return error instanceof JobCancelledError || error instanceof Error && error.message === "Job cancelled by user";
}
