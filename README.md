# pdf-worker

Worker BullMQ em Node.js 22/TypeScript para dois fluxos do app `tropa-do-soi`:

- `drive-pdf-optimize`: otimiza PDFs do Drive com Ghostscript;
- `material-pdf-watermark`: aplica a marca d'água individual dos materiais e grava uma cópia temporária no bucket privado `lesson-material-downloads`.

## Arquitetura

```text
Next.js (Vercel)                    VPS (Hetzner/Coolify)
     │                                      │
     ├─ upload-sign                         │
     ├─ PUT → Supabase Storage              │
     ├─ upload-complete (insert pending)    │
     ├─ enqueue ── Redis publico/TLS ─────► │ pdf-worker
     │                                      ├─ download com timeout
     │                                      ├─ Ghostscript local
     │                                      ├─ upsert no mesmo storage_path
     │                                      ├─ update files + quota + audit
     │                                      └─ DLQ drive-pdf-failed
     └─ poll / Realtime ◄── Postgres ───────┘
```

No fluxo de materiais, a Vercel apenas valida o acesso e enfileira um job. O worker lê usuário e
material no Postgres, baixa o original de `lesson-materials`, aplica a marca d'água com `pdf-lib`,
grava o resultado em `lesson-material-downloads` e marca `material_download_jobs` como `ready`.
O navegador baixa por URL assinada diretamente do Storage. Um processo periódico remove objetos
cujo `expires_at` já passou.

O Redis precisa ser alcançável pela Vercel e pela VPS. Use Redis gerenciado com TLS, como Upstash ou Redis Cloud, ou exponha Redis com firewall e senha forte.

## Setup Local

```bash
npm install
cp .env.example .env
docker run --rm -p 6379:6379 redis:7
npm run dev
```

Também é necessário ter Ghostscript disponível localmente:

```bash
brew install ghostscript
```

## Variáveis

Veja `.env.example`. Valores importantes:

- `REDIS_URL` (ex.: `redis://localhost:6379` ou `rediss://...` do Upstash — mesma URL no Next e na VPS)
- `BULLMQ_QUEUE_NAME=drive-pdf-optimize`
- `BULLMQ_DLQ_NAME=drive-pdf-failed`
- `WORKER_CONCURRENCY=1`
- `MATERIAL_PDF_QUEUE_NAME=material-pdf-watermark`
- `MATERIAL_WORKER_CONCURRENCY=1`
- `SUPABASE_URL` e `SUPABASE_SECRET_KEY` (a mesma chave server-side do `tropa-do-soi`; nao use `NEXT_PUBLIC_SUPABASE_PUBLISHABLE_KEY` no worker)
- `SUPABASE_BUCKET=user-files`
- `SUPABASE_LESSON_MATERIALS_BUCKET=lesson-materials`
- `SUPABASE_MATERIAL_DOWNLOADS_BUCKET=lesson-material-downloads`
- `MATERIAL_WATERMARK_MAX_BYTES=52428800`
- `DRIVE_PDF_COMPRESS_MAX_BYTES=524288000`
- `DRIVE_MIN_COMPRESSION_REDUCTION=0.05`
- `DRIVE_PDF_OPTIMIZER_TIMEOUT_MS=300000`
- `WORKER_API_SECRET`, opcional, para validar HMAC do payload

## Migração no App Principal

Aplicar as migrations do banco usadas pelo `tropa-do-soi` antes de ligar o worker. Para materiais,
isso inclui `20260806150000_material_download_jobs.sql`, que cria o bucket temporário, a tabela de
jobs e as RPCs de contagem atômica.

Para o Drive, a estrutura esperada continua sendo:

```sql
alter table public.files
  add column if not exists processing_status text not null default 'ready'
    check (processing_status in ('pending', 'processing', 'ready', 'failed', 'skipped')),
  add column if not exists compression_setting text null,
  add column if not exists compression_message text null,
  add column if not exists processed_at timestamptz null;
```

Semântica:

- `pending`: arquivo inserido e job ainda na fila.
- `processing`: worker iniciou download/Ghostscript.
- `ready`: job terminou com sucesso, com ou sem compressão aplicada.
- `skipped`: worker não tentou otimizar, por exemplo PDF acima do limite.
- `failed`: retries esgotados; o original permanece no Storage.

O worker chama a RPC `increment_user_storage_used_bytes` com os argumentos `p_user_id` e `p_delta_bytes`. Se a RPC existente usa outros nomes, alinhe a assinatura no banco ou ajuste `src/services/filesDb.ts`.

## Producer no Next.js

Exemplo para o app principal, após o `upload-complete` inserir `files` com `processing_status = 'pending'`:

```typescript
import { createHmac } from "node:crypto";
import { Queue } from "bullmq";

type DrivePdfOptimizeJob = {
  fileId: string;
  userId: string;
  storagePath: string;
  mimeType: "application/pdf";
  originalSizeBytes: number;
  bucket: string;
  enqueuedAt: string;
  signature?: string;
};

import IORedis from "ioredis";

const queue = new Queue<DrivePdfOptimizeJob>("drive-pdf-optimize", {
  connection: new IORedis(process.env.REDIS_URL!, { maxRetriesPerRequest: null }),
});

function signPayload(job: DrivePdfOptimizeJob): string | undefined {
  if (!process.env.WORKER_API_SECRET) return undefined;

  return createHmac("sha256", process.env.WORKER_API_SECRET)
    .update(`${job.fileId}|${job.userId}|${job.storagePath}|${job.enqueuedAt}`)
    .digest("hex");
}

export async function enqueueDrivePdfOptimize(job: Omit<DrivePdfOptimizeJob, "signature">) {
  const payload = { ...job, signature: signPayload(job) };

  await queue.add("optimize", payload, {
    jobId: job.fileId,
    removeOnComplete: 100,
    removeOnFail: 50,
    attempts: 3,
    backoff: { type: "exponential", delay: 5000 },
  });
}
```

Com `USE_EXTERNAL_PDF_WORKER=true`, o `upload-complete` não deve rodar compressão inline. Ele deve incrementar a cota com o tamanho original, enfileirar o job antes de responder `201` e retornar `compression.status = "pending"`.

## Operação

Build e execução:

```bash
npm run build
npm start
```

Healthcheck (carrega `.env` via `--env-file`; no Docker/Coolify as variáveis vêm do ambiente do container):

```bash
npm run build
npm run healthcheck
```

Inspecionar DLQ:

```bash
node -e "const { Queue } = require('bullmq'); const IORedis = require('ioredis'); const conn = new IORedis(process.env.REDIS_URL, { maxRetriesPerRequest: null }); const q = new Queue(process.env.BULLMQ_DLQ_NAME || 'drive-pdf-failed', { connection: conn }); q.getJobs(['waiting','delayed','failed'], 0, 20).then(j => console.log(j.map(x => ({ id: x.id, data: x.data })))).finally(async () => { await q.close(); await conn.quit(); })"
```

## Deploy Coolify

1. Crie um app Docker apontando para este repositório.
2. Configure todas as variáveis de `.env.example`.
3. Comece com `WORKER_CONCURRENCY=1`.
4. Garanta que `gs` está disponível pela imagem Docker.
5. Use Redis público/TLS compartilhado com o app Vercel.
6. Monitore memória antes de subir concorrência para `2`, especialmente para PDFs acima de 100 MB.

**Variáveis no Coolify:** marque `NODE_ENV`, `REDIS_URL`, `SUPABASE_SECRET_KEY`, `WORKER_API_SECRET` e demais secrets como **Runtime only** (não "Available at Buildtime"). Com `NODE_ENV=production` no build, o `npm ci` pula `typescript` e o deploy falha com `tsc: not found`. O `Dockerfile` já força devDependencies no stage de build, mas evitar secrets no buildtime também é mais seguro.

**Health check no Coolify:** este worker não expõe HTTP. Use o health check do `Dockerfile` (`node dist/healthcheck.js` — Redis + Storage). **Não** use `HEALTHCHECK NONE` (o Coolify ainda tenta ler `.State.Health` e o deploy falha com `map has no entry for key "Health"`). Na UI do app, deixe o health check **desligado** se for tipo HTTP/porta; o rolling update usa o `HEALTHCHECK` da imagem. Se o deploy falhar após ~45s, confira os logs do container e rode localmente `npm run build && npm run healthcheck` com as mesmas variáveis — falhas comuns: `REDIS_URL` inacessível do VPS (use `rediss://` do Upstash, não `localhost`) ou `SUPABASE_SECRET_KEY` / `SUPABASE_BUCKET` incorretos.

## Rollback

Mantenha a feature flag `USE_EXTERNAL_PDF_WORKER=false` no `tropa-do-soi` para voltar à compressão inline legada. O worker pode continuar deployado; basta parar de enfileirar novos jobs. Jobs já pendentes podem ser drenados ou removidos manualmente no Redis.

## Testes Manuais

- PDF pequeno que reduz mais de 5%.
- PDF que não reduz o suficiente, esperando `ready` + `compression_message = 'not_smaller'`.
- PDF acima de `DRIVE_PDF_COMPRESS_MAX_BYTES`, esperando `skipped`.
- Falha de Ghostscript, esperando retry e depois DLQ.
- Timeout de download ou Ghostscript.
- Falha de Redis ou Supabase, confirmando logs estruturados e retries.
