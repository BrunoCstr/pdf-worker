FROM node:22-bookworm-slim AS build

# Coolify (e outros) podem injetar NODE_ENV=production no build e pular devDependencies.
ENV NODE_ENV=development

RUN apt-get update \
  && apt-get install -y --no-install-recommends ghostscript \
  && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY package*.json ./
RUN npm ci --include=dev

COPY tsconfig.json ./
COPY src ./src
RUN npm run build

FROM node:22-bookworm-slim AS runtime

ENV NODE_ENV=production

RUN apt-get update \
  && apt-get install -y --no-install-recommends ghostscript \
  && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY package*.json ./
RUN npm ci --omit=dev

COPY --from=build /app/dist ./dist

# Coolify lê HEALTHCHECK do Dockerfile. Não use HEALTHCHECK NONE — o Coolify ainda
# espera .State.Health e falha com "map has no entry for key Health".
HEALTHCHECK --interval=30s --timeout=15s --start-period=45s --retries=3 \
  CMD ["node", "dist/healthcheck.js"]

CMD ["node", "dist/worker.js"]