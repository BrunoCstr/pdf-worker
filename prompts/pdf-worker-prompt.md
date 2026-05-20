# Projeto: pdf-worker

Crie um repositório Node.js 22 + TypeScript chamado `pdf-worker`: worker BullMQ na VPS (Coolify/Hetzner) que otimiza PDFs do **Drive de arquivos** do app principal (`tropa-do-soi` / Next.js na Vercel), integrado ao **Supabase Storage e Postgres** já existentes.

## Contexto do app principal (NÃO reinventar storage)

O Next.js já implementa:

1. `POST /api/drive/files/upload-sign` — valida MIME (PDF/DOCX), cota, gera `file_id` + `storage_path` + signed upload URL.
2. Browser faz `PUT` na `signed_url` (arquivo vai direto ao bucket).
3. `POST /api/drive/files/upload-complete` — hoje comprime PDF **inline** com qpdf/Ghostscript/pdf-lib e grava em `public.files`.

**Bucket:** `user-files` (env `DRIVE_STORAGE_BUCKET`, default `user-files`). Privado. MIME permitidos: `application/pdf`, DOCX.

**Path canônico (obrigatório respeitar):**

```
{userId}/{folderPrefix}/{fileId}.pdf
```

- `userId` = UUID do usuário (1º segmento — exigido pela policy RLS em `storage.objects`).
- `folderPrefix` = path materializado da pasta, ex. `raiz` ou `raiz/subpasta` (sem barra inicial).
- `fileId` = UUID gerado no `upload-sign`.
- **Não usar** pastas globais tipo `uploads/original/` ou `uploads/optimized/` — quebram RLS e o modelo de dados.

**Tabela `public.files` (campos relevantes):**

- `id`, `user_id`, `folder_id`, `name`, `original_name`, `mime_type`
- `size_bytes` (tamanho final cobrado na cota)
- `original_size_bytes` (tamanho do upload antes da otimização)
- `storage_path` (único, UNIQUE)
- `compression_ratio` (float ou null)
- `created_at`, `updated_at`

**Cota:** `user_storage_quota` via RPC `increment_user_storage_used_bytes`.

**Auditoria:** `file_audit_logs` com `action` + `metadata` JSON.

**Limites (alinhar com o app):**

- Upload máximo na API: 500 MB (`DRIVE_MAX_UPLOAD_BYTES`)
- Worker só processa PDFs até **500 MB** (`DRIVE_PDF_COMPRESS_MAX_BYTES`) ← *alterado de 250 MB no código legado*
- Redução mínima para aceitar resultado: 5% (`DRIVE_MIN_COMPRESSION_REDUCTION`)
- Timeout Ghostscript: **300s / 5 min** (`DRIVE_PDF_OPTIMIZER_TIMEOUT_MS`) ← *alterado de 120s — PDFs de 500 MB podem levar 3–4 min*

> **Paridade no app principal:** ao integrar o worker, atualizar `lib/drive/constants.ts` no repositório `tropa-do-soi` para os mesmos valores (500 MB / 300 s), ou remover o uso de `compressPdfBuffer` no `upload-complete` quando `USE_EXTERNAL_PDF_WORKER=true`, para não haver dois limites divergentes.

---

## Objetivo da migração arquitetural

Mover **apenas a etapa Ghostscript** (e futuramente qpdf opcional) do `upload-complete` da Vercel para este worker na VPS. O Storage continua com **um objeto por arquivo no mesmo `storage_path`**.

### Fluxo alvo (integrado ao drive existente)

```text
[Browser]
  → upload-sign (Next, service role)
  → PUT signed_url → Storage (PDF original no path final)
  → upload-complete (Next) — SEM compressão inline para PDF
      → insert `files` com status de processamento pendente (ver migração abaixo)
      → incrementa cota com `original_size_bytes` (tamanho do upload)
      → enfileira job BullMQ
      → responde 201 com file + compression.status = "pending"

[pdf-worker VPS]
  → consome job
  → download do `storage_path` (service role, stream para disco, com timeout próprio)
  → Ghostscript → arquivo local otimizado
  → se redução ≥ 5%: upload com upsert no **mesmo** `storage_path`
  → se não: mantém objeto original (não fazer upload)
  → atualiza `files`: size_bytes, compression_ratio, processing_status
  → se size_bytes diminuiu: ajusta cota (delta negativo na RPC)
  → `file_audit_logs`: action `compress` com metadata (incluindo queue_wait_ms e duration_ms)
  → remove arquivos temporários locais

[Browser]
  → polling em `GET /api/drive/files/{id}` (criar no app — hoje não existe) ou Realtime em `files.processing_status`
    até terminal: `ready` | `failed` | `skipped` (ver semântica abaixo)
```

> Não criar segundo objeto permanente em outra pasta. Não apagar o original antes do upsert bem-sucedido no mesmo path (o upsert substitui o conteúdo; rollback = não fazer upload se falhar).

**Staging opcional** somente se quiser dois objetos temporários (evitar janela sem arquivo):

```
{userId}/_staging/{fileId}.pdf → após sucesso, storage.move(staging → storage_path) e remove staging.
```

> Staging também deve começar com `{userId}/` por causa do RLS. **Recomendado para PDFs grandes** — para arquivos acima de 100 MB, considere tornar staging obrigatório para eliminar a janela de indisponibilidade durante o upsert.

---

## Migração SQL (especificar no README; app principal aplica)

Adicionar em `public.files`:

```sql
processing_status TEXT NOT NULL DEFAULT 'ready'
  CHECK (processing_status IN ('pending', 'processing', 'ready', 'failed', 'skipped')),
compression_setting TEXT NULL,
compression_message TEXT NULL,
processed_at TIMESTAMPTZ NULL
```

- PDFs novos após migração: `upload-complete` grava `pending` (não `ready`).
- DOCX: `processing_status = 'ready'` imediato (sem fila).
- Registros existentes antes da migração: `DEFAULT 'ready'` — comportamento legado preservado.

### Semântica de `processing_status` (regra única)

| Status | Quando usar |
|--------|-------------|
| `pending` | Insert no `upload-complete`; job ainda não consumido ou na fila |
| `processing` | Worker começou (download/GS em andamento) |
| `ready` | Job terminou com sucesso — **com ou sem** compressão aplicada. Se o GS não reduziu ≥5%, detalhe em `compression_message` (ex. `not_smaller`), não use `skipped` |
| `skipped` | Worker **não tentou** otimizar (ex. PDF > `DRIVE_PDF_COMPRESS_MAX_BYTES`) |
| `failed` | Erro após esgotar retries; objeto original no Storage permanece |

> **Não confundir:** `compression.status` na API de upload (`pending`, `applied`, `not_smaller`) é resposta transitória do `upload-complete`. O campo persistido é `files.processing_status`. Após o worker, a UI deve olhar só `processing_status` (+ `compression_message` para detalhe).

---

## Stack do worker

- Node.js 22, TypeScript
- BullMQ + ioredis
- `@supabase/supabase-js` (service role)
- Ghostscript via `child_process` (mesmos argumentos do app principal)
- Docker (Node 22 + gs)
- Logs estruturados (pino ou similar)
- Concorrência: `WORKER_CONCURRENCY` (default **1** em produção; **2** só após medir RAM)

> **Concorrência e RAM:** dois PDFs grandes (~500 MB) em paralelo podem causar OOM em VPS 4 GB (download + temp + GS). Começar com `WORKER_CONCURRENCY=1`, subir para 2 após monitorar memória no Coolify. PDFs >100 MB com concorrência >1 exigem cautela extra.

---

## Infraestrutura Redis (obrigatório para integração)

O **producer** BullMQ roda no **Next.js na Vercel**; o worker consome na VPS. O Redis **não pode** ficar apenas em rede privada da VPS — a Vercel precisa alcançá-lo.

- **Recomendado:** Redis gerenciado com TLS e host público (ex. [Upstash](https://upstash.com/), Redis Cloud).
- **Variáveis no app (`tropa-do-soi`):** `REDIS_HOST`, `REDIS_PORT`, `REDIS_PASSWORD` (e `rediss://` se TLS).
- **Variáveis no worker (VPS):** mesmas credenciais apontando para o **mesmo** Redis.
- Redis só na VPS + firewall = fila enfileirada **nunca** será consumida pelo worker se o Next não conseguir conectar.

Documentar no README do worker: diagrama Vercel → Redis ← VPS.

---

## Ghostscript (paridade com `lib/drive/compression.ts`)

Usar exatamente estes argumentos (`setting: ghostscript-300dpi`):

```bash
gs -sDEVICE=pdfwrite -dCompatibilityLevel=1.7 -dNOPAUSE -dQUIET -dBATCH -dSAFER \
  -dDetectDuplicateImages=true -dCompressFonts=true -dSubsetFonts=true \
  -dDownsampleColorImages=true -dDownsampleGrayImages=true -dDownsampleMonoImages=true \
  -dColorImageDownsampleType=/Bicubic -dGrayImageDownsampleType=/Bicubic \
  -dMonoImageDownsampleType=/Subsample \
  -dColorImageResolution=300 -dGrayImageResolution=300 -dMonoImageResolution=300 \
  -dJPEGQ=92 -sOutputFile={outputPath} {inputPath}
```

**Regras de aceite (igual ao app):**

- `compressedSize < originalSize`
- `reductionRatio >= 0.05` → `applied`, grava `compression_ratio`
- senão → não substituir objeto no Storage; `processing_status = 'ready'`, `compression_message = 'not_smaller'` (ou similar)
- PDF > 500 MB → `processing_status = 'skipped'`, mensagem explicativa, sem rodar `gs`

---

## Payload do job BullMQ

Nome da fila: `drive-pdf-optimize` (configurável por env).

```typescript
type DrivePdfOptimizeJob = {
  fileId: string           // UUID = files.id
  userId: string           // UUID
  storagePath: string      // path completo no bucket (ex: userId/raiz/uuid.pdf)
  mimeType: "application/pdf"
  originalSizeBytes: number
  bucket: string           // default "user-files"
  enqueuedAt: string       // ISO — usado para calcular queue_wait_ms no worker
  signature?: string     // HMAC opcional (se WORKER_API_SECRET configurado)
}
```

- Não enviar binário no Redis — só metadados.
- **Idempotência:** `jobId = fileId` (evita duplicar processamento).
- **Retries:** 3 com backoff exponencial; em falha final → `processing_status = 'failed'` + audit log + mover para dead letter queue `drive-pdf-failed` para inspeção manual.

---

## Variáveis de ambiente (`.env.example`)

```env
NODE_ENV=production

# Redis (BullMQ)
REDIS_HOST=
REDIS_PORT=6379
REDIS_PASSWORD=
BULLMQ_QUEUE_NAME=drive-pdf-optimize
BULLMQ_DLQ_NAME=drive-pdf-failed
WORKER_CONCURRENCY=1

# Supabase (service role — nunca expor no browser)
SUPABASE_URL=
SUPABASE_SERVICE_ROLE_KEY=
SUPABASE_BUCKET=user-files

# Limites (espelhar app principal)
DRIVE_PDF_COMPRESS_MAX_BYTES=524288000    # 500 MB
DRIVE_MIN_COMPRESSION_REDUCTION=0.05
DRIVE_PDF_OPTIMIZER_TIMEOUT_MS=300000    # 5 min
DRIVE_DOWNLOAD_TIMEOUT_MS=120000         # 2 min — timeout separado para download do Storage

GHOSTSCRIPT_BINARY=gs

# Segurança (recomendado em produção)
WORKER_API_SECRET=    # ver seção abaixo
```

### `WORKER_API_SECRET` (contrato opcional)

- **MVP:** Redis com senha forte + firewall (só IPs Vercel/upstream e VPS) pode bastar sem HMAC.
- **Produção:** incluir no payload do job um campo `signature` (HMAC-SHA256 de `fileId|userId|storagePath|enqueuedAt` com o secret). O worker rejeita jobs sem assinatura válida.
- O Next calcula a assinatura em `enqueueDrivePdfOptimize`; o worker valida antes de processar.
- Não expor `WORKER_API_SECRET` no browser — apenas env server-side na Vercel.

---

## Estrutura do repositório

```
pdf-worker/
├── src/
│   ├── worker.ts              # BullMQ Worker
│   ├── queue.ts               # Queue + DLQ + tipos do job
│   ├── jobs/
│   │   └── optimizeDrivePdf.ts
│   ├── services/
│   │   ├── supabase.ts        # client service role
│   │   ├── storage.ts         # download stream (com timeout), upload upsert, move/remove staging
│   │   ├── compressPdf.ts     # Ghostscript + métricas
│   │   └── filesDb.ts         # update files, quota delta, audit
│   ├── utils/
│   │   ├── logger.ts
│   │   └── tempFiles.ts       # mkdtemp + cleanup garantido
│   └── types/
├── Dockerfile
├── package.json
├── tsconfig.json
├── .env.example
└── README.md
```

---

## Comportamento do worker (detalhado)

1. Calcular `queue_wait_ms = Date.now() - Date.parse(job.data.enqueuedAt)` e logar.
2. Marcar `files.processing_status = 'processing'`, `updated_at = now()`.
3. Download via `storage.download(storagePath)` → gravar em temp com stream, respeitando `DRIVE_DOWNLOAD_TIMEOUT_MS`.
4. Executar Ghostscript input → output em outro temp, respeitando `DRIVE_PDF_OPTIMIZER_TIMEOUT_MS`.
5. Comparar tamanhos e aplicar regra dos 5%.
6. **Se applied:**
   - `storage.upload(storagePath, optimized, { upsert: true, contentType: 'application/pdf' })`
   - Atualizar `size_bytes`, `compression_ratio`, `compression_setting = 'ghostscript-300dpi'`
   - Se `size_bytes < original_size_bytes`: chamar RPC `increment_user_storage_used_bytes(userId, deltaNegativo)`
7. **Se não aplicou compressão** (redução <5%): manter Storage; `processing_status = 'ready'`, `compression_message = 'not_smaller'` (ou similar), `compression_ratio = null`.
8. **Se PDF > limite:** `processing_status = 'skipped'`, `compression_message` com motivo, sem alterar Storage.
9. **Se applied:** após upsert bem-sucedido, `processing_status = 'ready'`, `processed_at = now()`, demais campos de compressão preenchidos.
10. Audit: `{ action: 'compress', metadata: { applied, setting, original_size, compressed_size, duration_ms, queue_wait_ms } }`
11. `finally`: apagar diretório temp local.

> **Rollback:** falha antes do upsert → não alterar objeto no Storage; status `failed`; job movido para DLQ `drive-pdf-failed`; original permanece utilizável.

---

## Métricas a logar por job

| Campo | Descrição |
|---|---|
| `queue_wait_ms` | Tempo entre `enqueuedAt` e início do processamento |
| `download_ms` | Tempo de download do Storage |
| `ghostscript_ms` | Tempo de execução do GS |
| `upload_ms` | Tempo de upsert (se aplicado) |
| `duration_ms` | Tempo total do job |
| `applied` | Boolean — compressão foi aceita |
| `original_size` | Bytes antes |
| `compressed_size` | Bytes depois (ou mesmo valor se skipped) |

---

## Dockerfile

```dockerfile
FROM node:22-bookworm-slim
RUN apt-get update && apt-get install -y ghostscript && rm -rf /var/lib/apt/lists/*
WORKDIR /app
COPY package*.json ./
RUN npm ci
COPY . .
RUN npm run build
CMD ["node", "dist/worker.js"]
```

- Healthcheck opcional: script que verifica Redis + Supabase.

---

## README (obrigatório)

- Setup local (Redis Docker, `.env`, `npm run dev`)
- Deploy Coolify (env vars, volume não necessário, rede privada Redis)
- Como o app Next enfileira jobs (ver abaixo)
- Migração SQL que o app principal deve aplicar antes de ligar o worker
- Rollback / feature flag: `USE_EXTERNAL_PDF_WORKER=true` (com flag `false`, o app mantém compressão inline legada)
- Como inspecionar jobs na DLQ `drive-pdf-failed`
- Ajuste de `WORKER_CONCURRENCY` conforme recursos da VPS (começar em 1)
- Redis acessível pela Vercel (Upstash ou equivalente)

---

## Integração no Next.js (documentar exemplos no README)

### Producer (após refatorar `upload-complete`)

```typescript
// lib/drive/pdfOptimizeQueue.ts (no app principal — exemplo para o README)
import { Queue } from "bullmq"

const queue = new Queue("drive-pdf-optimize", {
  connection: { host: process.env.REDIS_HOST!, port: Number(process.env.REDIS_PORT) },
})

export async function enqueueDrivePdfOptimize(job: DrivePdfOptimizeJob) {
  await queue.add("optimize", job, {
    jobId: job.fileId,
    removeOnComplete: 100,
    removeOnFail: 50,
    attempts: 3,
    backoff: { type: "exponential", delay: 5000 },
  })
}
```

### Mudança em `upload-complete` (descrição, não implementar aqui)

- Com `USE_EXTERNAL_PDF_WORKER=true`: remover `compressPdfBuffer` para PDFs.
- Inserir `files` com `processing_status: 'pending'`, `original_size_bytes: file_size`, `size_bytes: file_size` (provisório).
- `incrementQuota` com tamanho original.
- **Enfileirar antes de responder 201:** chamar `enqueueDrivePdfOptimize` e só então retornar sucesso. Se `queue.add` falhar → não deixar órfão: ou não inserir `files`, ou marcar `failed` + remover objeto do Storage + retornar 503.
- Payload: `{ fileId, userId, storagePath, mimeType, originalSizeBytes, bucket, enqueuedAt: new Date().toISOString() }` (+ `signature` se usar HMAC).
- Resposta inclui `compression: { status: 'pending', applied: false }` e `file.processing_status: 'pending'`.

### Nova rota no app: `GET /api/drive/files/[id]`

- **Hoje:** `app/api/drive/files/[id]/route.ts` só tem PATCH e DELETE; não existe GET leve para polling.
- **Criar GET** que retorna `serializeFile` + `processing_status`, `compression_message`, `processed_at` — **sem** gerar signed URL (evitar custo a cada poll).
- Alternativa pior: reutilizar `GET .../preview` a cada poll (gera URL assinada desnecessariamente).

### UI (`components/FileManager/api.ts`)

- Fase `optimizing` continua; pode durar vários minutos (worker assíncrono).
- Após `upload-complete`, polling `GET /api/drive/files/:id` a cada 2–3 s até `processing_status` ∈ `ready` | `failed` | `skipped` (timeout sugerido: 8–10 min).
- Em `failed`: toast com opção de recarregar a pasta; arquivo original continua utilizável.
- Em `ready` com `compression_message = 'not_smaller'`: tratar como sucesso (arquivo disponível).
- **Preview/download durante `pending`/`processing`:** o PDF original já está no `storage_path` — preview pode funcionar; opcionalmente exibir “Otimização em andamento…” na UI.
- Opcional: Supabase Realtime `postgres_changes` em `files` filtrado por `id` (habilitar Realtime na tabela na migração).

---

## Arquitetura

```
Next.js (Vercel)                    VPS (Hetzner/Coolify)
     │                                      │
     ├─ upload-sign ────────────────────────┤
     ├─ PUT → Supabase Storage (path final)│
     ├─ upload-complete (insert pending)    │
     ├─ enqueue ──Redis (Vercel+Upstash)───►│ pdf-worker (concurrency=1–2)
     │                                      ├─ download com timeout próprio
     │                                      ├─ Ghostscript (local temp, 5 min timeout)
     │                                      ├─ upsert mesmo storage_path
     │                                      ├─ update files + quota + audit
     │                                      └─ DLQ drive-pdf-failed se 3 retries falharem
     ├─ poll / Realtime ◄── Postgres ───────┘
     └─ preview/download (original já no path durante pending)
```

---

## Checklist de integração no `tropa-do-soi` (após worker pronto)

1. Redis público/TLS + env na Vercel e na VPS.
2. Migration SQL (`processing_status`, etc.) + Realtime opcional.
3. Atualizar `lib/drive/constants.ts` (500 MB / 300 s) ou desativar compressão inline via flag.
4. `lib/drive/pdfOptimizeQueue.ts` + deps `bullmq` / `ioredis`.
5. Refatorar `upload-complete` + feature flag `USE_EXTERNAL_PDF_WORKER`.
6. `GET /api/drive/files/[id]` + estender `serializeFile` / `FILE_PUBLIC_COLUMNS`.
7. Polling ou Realtime em `components/FileManager/api.ts`.
8. Deploy com flag `false` → validar → flag `true` em staging → produção.
9. Monitorar DLQ e memória da VPS.

---

## O que NÃO fazer

- Pastas `uploads/original/` ou `uploads/optimized/` fora do prefixo `{userId}/`.
- Dois paths permanentes por arquivo sem atualizar `files.storage_path`.
- Apagar o PDF do Storage antes de confirmar upsert bem-sucedido.
- Comprimir no browser.
- Usar anon key no worker (sempre service role).
- Enfileirar job antes do PUT do cliente terminar (só enfileirar no `upload-complete`).
- Responder 201 com `pending` se o enqueue no Redis falhou (arquivo órfão na fila).
- Usar `processing_status = 'skipped'` quando o PDF está pronto mas só não encolheu (use `ready` + `compression_message`).

---

## Qualidade

- TypeScript strict
- Tratamento de erros por etapa com contexto no log
- Testes manuais documentados: PDF pequeno, PDF grande >250 MB, PDF ~500 MB, PDF que não encolhe, falha de rede, falha do Ghostscript, timeout de download
- Código pronto para produção e manutenção
