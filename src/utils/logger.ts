import pino from "pino";

// Pino's built-in error serializer only activates for the "err" key.
// Register it for every key we use across the codebase so Error objects
// are never logged as empty `{}`.
const errorSerializer = pino.stdSerializers.err;

export const logger = pino({
  level: process.env.LOG_LEVEL ?? "info",
  base: undefined,
  timestamp: pino.stdTimeFunctions.isoTime,
  serializers: {
    err: errorSerializer,
    error: errorSerializer,
    dbErr: errorSerializer,
    markError: errorSerializer,
    dlqError: errorSerializer,
    auditError: errorSerializer,
    validationError: errorSerializer,
  },
});
