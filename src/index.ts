/**
 * GenTeam channel plugin for OpenClaw.
 *
 * Lets a self-hosted OpenClaw gateway connect to GenTeam as an Agent runtime:
 * GenTeam becomes a channel inside the gateway, the same model as Slack /
 * Discord. The gateway authenticates with a long-lived bot secret, opens an
 * authenticated WebSocket, receives `turn.start` frames (one per inbound
 * GenTeam message), runs them through its own agent, and writes the reply back
 * into GenTeam under that agent's identity via the backend HTTP API.
 *
 * The gateway NEVER opens any other chat-backend socket directly — every
 * visible message rides the backend `agent_tools/message-send` write-back
 * under the agent's identity.
 *
 * What the plugin wires into the gateway agent (parity with the Local Computer /
 * Cloud Sandbox runtimes):
 *   - Per-agent system prompt: the backend renders the full per-agent system
 *     prompt (identity, visible channels, runtime contract, tool guidance) and
 *     ships it on `turn.start` as `system_prompt_text`. The plugin applies it to
 *     THIS turn's agent run via `ctx.GroupSystemPrompt` (read by get-reply into
 *     the cache-static system-prompt slot — additive, never clobbers the
 *     operator's own prompt). See `dispatchTurnToAgent`.
 *   - The `de` tool surface: the same agent_tools verbs the local/sandbox `de`
 *     CLI exposes are registered as model-callable tools via `api.registerTool`
 *     (`de_*`, the same verb names the local/sandbox `de` CLI uses). Each tool
 *     POSTs `{verb, ...}` to `/agent_tools/<verb>` with
 *     the per-agent write-back token. See `buildGenteamTools`.
 *   - Stop / abort: a `turn.abort` frame cancels the in-flight run via a
 *     per-turn AbortController threaded into the dispatcher.
 *
 * Config in openclaw.json (channels.genteam.accounts.<id>):
 *   endpoint  - GenTeam backend base URL (e.g. https://www.genspark.ai)
 *   channelId - non-secret routing id (occ_…)
 *   appToken  - app/connection token (oca_…), cross-checked at auth-exchange
 *   botToken  - bot secret (ocb_…), the Authorization: Bearer at auth-exchange
 *
 * Contract (the backend HTTP API this plugin targets):
 *   (1) auth-exchange:  POST {endpoint}/api/digital-employee/openclaw/channel/auth
 *   (2) channel WS:     {ws_base}/api/digital-employee/openclaw/channel/ws?token=<ws_token>
 *   (3) write-back:     POST {endpoint}/api/digital-employee/agent_tools/<verb>
 */
import { readFileSync, realpathSync, createWriteStream, mkdirSync, lstatSync, mkdtempSync } from 'fs'
import { rm } from 'fs/promises'
import { resolve, dirname, isAbsolute, relative, join } from 'path'
import { tmpdir } from 'os'
import { Readable, Transform } from 'stream'
import { pipeline } from 'stream/promises'
import { createHash, randomBytes, randomUUID } from 'crypto'
import { execSync } from 'child_process'
import { fileURLToPath } from 'url'
import { createRequire } from 'module'
// TypeBox `Type` for the registerTool parameter schemas. esbuild `--bundle`
// INLINES typebox into dist/index.js (only `ws` and the `openclaw` peer stay
// external), so there is NO runtime `typebox` dependency for the gateway to
// resolve. This is deliberate: a bare runtime `import 'typebox'` regressed in
// the wild — the gateway installs plugin deps with `--omit=dev` and resolves
// modules from the loaded extension path, so a typebox that is dev-only (or
// installed to a different path) fails the WHOLE module load with "Cannot find
// module 'typebox'". Bundling sidesteps both failure modes. typebox v1's
// `Type.Object(...)` serializes to plain JSON Schema, which the gateway's own
// typebox re-normalizes and validates, so a bundled copy causes no
// cross-version skew.
import { Type } from 'typebox'

// Shared GenTeam attachment session+SAS direct-upload core — the SAME module
// the Local Computer daemon uses, kept in a sibling source dir outside this
// package. It is a relative source import, so (like typebox) esbuild `--bundle`
// INLINES it into dist/index.js; there is no runtime dependency on any sibling
// package.
import {
  uploadAttachmentsViaSession,
  combineSignals,
  GENTEAM_ATTACHMENT_MAX_COUNT,
  GENTEAM_DIRECT_UPLOAD_MAX_BYTES,
  type AgentToolPost,
} from '../shared/attachment-upload.ts'

// Prefer the `ws` npm package over Node.js built-in WebSocket.
// OpenClaw's gateway calls undici.setGlobalDispatcher() which corrupts the
// built-in WebSocket (also undici-based) handshake, causing "Received network
// error or non-101 status code" on every connect. The `ws` package uses Node's
// native `http` module and is unaffected.
//
// Resolution is tried in order of robustness: (1) from THIS module's location
// (the plugin's own / hoisted node_modules — works regardless of how the
// gateway was launched), then (2) from the resolved `openclaw` binary root.
// Only if BOTH miss do we fall back to the built-in WebSocket — and we WARN,
// because that path is known-broken under the gateway's undici dispatcher and a
// silent fallback would dead-end every connect with a cryptic error.
// `WebSocket` is a runtime global only on Node >= 21; a bare value reference
// throws ReferenceError on the Node 18-20 floor before the `ws` resolution
// below runs. Guard it with `typeof` (the one safe way to read a possibly
// undeclared global) — this last-resort fallback is the known-broken path
// anyway, so leaving it undefined when both `ws` resolution AND the global miss
// only defers to the explicit warn below.
let WsWebSocket: typeof WebSocket =
  typeof WebSocket !== 'undefined' ? WebSocket : (undefined as unknown as typeof WebSocket)
let _wsResolved = false
for (const resolveFrom of [
  () => import.meta.url,
  () => execSync('readlink -f $(which openclaw)', { encoding: 'utf-8' }).trim(),
]) {
  try {
    WsWebSocket = createRequire(resolveFrom())('ws')
    _wsResolved = true
    break
  } catch {
    // try the next resolution strategy
  }
}
if (!_wsResolved) {
  // eslint-disable-next-line no-console
  console.warn(
    '[genteam] could not resolve the `ws` package; falling back to the built-in ' +
      'WebSocket, which the gateway undici dispatcher may corrupt (connects may fail). ' +
      'Install `ws` in the gateway environment.',
  )
}

// ---------------------------------------------------------------------------
// Version — read from package.json, sent to the backend on auth + ready
// ---------------------------------------------------------------------------

const __pluginDir = dirname(fileURLToPath(import.meta.url))

const PLUGIN_VERSION: string = (() => {
  try {
    const pkg = JSON.parse(readFileSync(resolve(__pluginDir, '..', 'package.json'), 'utf-8'))
    return pkg.version || '0.0.0'
  } catch {
    return '0.0.0'
  }
})()

const AGENT_TOOLS_PREFIX = '/api/digital-employee/agent_tools'

// The 1 GiB byte cap + 10-attachment count cap live in the shared
// attachment-upload core (GENTEAM_DIRECT_UPLOAD_MAX_BYTES /
// GENTEAM_ATTACHMENT_MAX_COUNT) as the single source of truth, drift-guarded
// against the backend. Attachments go via that shared session+SAS core, so the
// body never buffers in the gateway and the old 99 MiB multipart ceiling is
// gone. Only the count is referenced here (the tool-schema maxItems bound).
// Bound for a tool's agent_tools HTTP call. The per-turn lease is the real
// bound; this just keeps
// a single stalled fetch from hanging a turn.
const TOOL_HTTP_TIMEOUT_MS = 30_000
const TOOL_ATTACHMENT_HTTP_TIMEOUT_MS = 300_000

// Default per-attachment cap for `de_attachment_view` byte materialization.
// Mirrors the 1 GiB upload ceiling so a download never exceeds what GenTeam
// itself accepts; an operator can lower it per-account via
// `attachmentDownloadMaxBytes`. The body is streamed straight to disk (never
// buffered), so this guards disk abuse and a lying ``X-DE-Attachment-Size``
// header — not gateway memory.
const GENTEAM_ATTACHMENT_DOWNLOAD_MAX_BYTES = GENTEAM_DIRECT_UPLOAD_MAX_BYTES

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

interface GenteamAccountConfig {
  accountId: string
  endpoint: string
  channelId: string
  appToken: string
  botToken: string
  attachmentRoots: string[]
  // Where `de_attachment_view` writes downloaded attachment bytes. Optional;
  // defaults to a private, unpredictably-named per-process subdir of the system
  // temp dir. Set it to a directory the agent's own file tools can read when the
  // gateway runs the agent workspace-rooted.
  attachmentDownloadDir?: string
  // Optional per-attachment download cap (bytes); defaults to 1 GiB. Lets an
  // operator restrict how large a single materialized download may be.
  attachmentDownloadMaxBytes?: number
}

interface AuthResult {
  wsToken: string
  wsUrl: string
  wsTokenTtl: number
  agentToken: string
  agentId: string
  runtimeId: string
  serverId: string
  agentHandle: string
  agentDisplayName: string
}

// turn.start frame the backend sends over the channel WS. `reply_target` is
// EXPLICIT — the plugin never regexes the envelope for it. `system_prompt_text`
// carries the backend-rendered per-agent system prompt (applied via
// ctx.GroupSystemPrompt); `system_prompt_hash`/`version` let the plugin notice
// identity/template changes for debug logging.
interface TurnStartFrame {
  type: 'turn.start'
  turn_id: string
  agent_id: string
  runtime_id: string
  envelope: string
  reply_target: string
  parent_message?: string
  deadline_ms?: number
  model?: string
  system_prompt_text?: string
  system_prompt_hash?: string
  system_prompt_version?: number
}

// Gateway context passed from startAccount.
interface GatewayCtx {
  cfg: any
  accountId: string
  abortSignal: AbortSignal
  log: any
  channelRuntime: any
  setStatus?: (patch: Record<string, any>) => void
}

// The turn currently being dispatched for an account. The backend delivers
// turns FIFO-serially per agent, so at most one is in flight per account — the
// `de` tools read `replyTarget` (the reply-to-current-conversation default for
// de_message_send) and `abort` cancels the run on a `turn.abort` frame.
interface ActiveTurn {
  turnId: string
  replyTarget: string
  parentMessage?: string
  abort: AbortController
  // Count of successful visible sends (de_message_send / -attachment) this turn.
  // Used only to observe the no-reply case (a turn that posts nothing); NOT a
  // delivery fallback.
  sentCount: number
}

// Auth state shared with the WS connection + write-back + the de tools.
// Re-resolved on every (re)connect so a rotated agent_token is picked up.
interface ConnectionState {
  cfg: GenteamAccountConfig
  auth: AuthResult
  log: any
  // Per-turn dispatch state keyed by turn_id (FIFO-serial, usually 0..1 live).
  turns: Map<string, ActiveTurn>
  // turn_ids whose turn.abort arrived BEFORE the turn registered in `turns`
  // (fire-and-forget dispatch). dispatchTurnToAgent drains this right after it
  // registers, so a Stop racing turn registration is not dropped.
  pendingAborts: Set<string>
  // Last system_prompt_hash applied for this connection — debug-only, so a
  // hash change (agent identity / template version bump) is observable.
  lastPromptHash?: string
}

// ---------------------------------------------------------------------------
// Per-account connection registry — populated by runGenteamMonitor, consumed
// by the de tools (resolve the live agent_token + endpoint at call time) and
// the proactive outbound adapter. Replaces the old single-account global so a
// multi-account gateway resolves the correct connection per `agentAccountId`.
// ---------------------------------------------------------------------------

const connectionsByAccount = new Map<string, ConnectionState>()

// ---------------------------------------------------------------------------
// Config resolution
// ---------------------------------------------------------------------------

function normalizeAttachmentRoots(value: unknown): string[] {
  if (!Array.isArray(value)) return []
  const roots = value
    .filter((item): item is string => typeof item === 'string' && item.trim().length > 0)
    .map((item) => resolve(item))
  return roots
}

function isPathInsideRoot(filePath: string, root: string): boolean {
  const rel = relative(root, filePath)
  return rel === '' || (!!rel && !rel.startsWith('..') && !isAbsolute(rel))
}

function normalizeAllowedAttachmentPath(rawPath: string, roots: string[]): string | undefined {
  if (!rawPath || rawPath.includes('\0') || !isAbsolute(rawPath) || roots.length === 0) return undefined
  let resolvedPath: string
  try {
    resolvedPath = realpathSync(rawPath)
  } catch {
    return undefined
  }
  for (const root of roots) {
    try {
      const resolvedRoot = realpathSync(root)
      if (isPathInsideRoot(resolvedPath, resolvedRoot)) return resolvedPath
    } catch {
      // Missing/misconfigured roots do not grant access.
    }
  }
  return undefined
}

function formatAttachmentRoots(roots: string[]): string {
  return roots.length > 0 ? roots.join(', ') : 'none configured'
}

// Collapse a backend-supplied attachment filename to a safe basename so a
// hostile ``X-DE-Attachment-Filename`` (e.g. ``../../etc/passwd`` or one with
// path separators / NUL) can never escape the download dir. Mirrors the
// sandbox CLI shim's ``tr -c 'A-Za-z0-9._-' '_'`` + basename behaviour, and
// guards the pure-dot cases so we never target ``.`` / ``..``. Length-bounded
// so a pathological 64 KiB filename cannot blow the path limit.
function sanitizeAttachmentBasename(name: string): string {
  const base = (name || '').split(/[/\\]/).pop() || ''
  const cleaned = base.replace(/[^A-Za-z0-9._-]/g, '_')
  if (!cleaned || cleaned === '.' || cleaned === '..') return 'file'
  return cleaned.slice(0, 200)
}

// Where downloaded attachment bytes land. An explicit configured dir wins;
// otherwise we lazily create ONE private, unpredictably-named per-process dir
// under the OS temp dir. `mkdtempSync` creates it mode 0700, and the random
// suffix defeats a co-tenant pre-planting a symlink at a guessable
// `/tmp/genteam-attachments` (the prior fixed name made that a real attack on a
// shared host). Cached so repeated downloads reuse the same private dir.
let _defaultDownloadDir: string | undefined
function resolveAttachmentDownloadDir(cfg: GenteamAccountConfig): string {
  if (cfg.attachmentDownloadDir) return cfg.attachmentDownloadDir
  if (!_defaultDownloadDir) {
    _defaultDownloadDir = mkdtempSync(join(tmpdir(), 'genteam-attachments-'))
  }
  return _defaultDownloadDir
}

function resolveAccount(cfg: any, accountId?: string | null): GenteamAccountConfig {
  const id = accountId ?? 'default'
  const acc = cfg?.channels?.genteam?.accounts?.[id] ?? {}
  const endpoint = String(acc.endpoint ?? '').replace(/\/$/, '')
  const channelId = String(acc.channelId ?? '')
  const appToken = String(acc.appToken ?? '')
  const botToken = String(acc.botToken ?? '')
  const attachmentRoots = normalizeAttachmentRoots(acc.attachmentRoots)
  const attachmentDownloadDir =
    typeof acc.attachmentDownloadDir === 'string' && acc.attachmentDownloadDir.trim().length > 0
      ? resolve(acc.attachmentDownloadDir)
      : undefined
  const attachmentDownloadMaxBytes =
    typeof acc.attachmentDownloadMaxBytes === 'number' &&
    Number.isFinite(acc.attachmentDownloadMaxBytes) &&
    acc.attachmentDownloadMaxBytes > 0
      ? Math.floor(acc.attachmentDownloadMaxBytes)
      : undefined

  const missing: string[] = []
  if (!endpoint) missing.push('endpoint')
  if (!channelId) missing.push('channelId')
  if (!appToken) missing.push('appToken')
  if (!botToken) missing.push('botToken')
  if (missing.length > 0) {
    throw new Error(
      `[genteam] account "${id}" is missing required config: ${missing.join(', ')}`,
    )
  }

  return {
    accountId: id,
    endpoint,
    channelId,
    appToken,
    botToken,
    attachmentRoots,
    attachmentDownloadDir,
    attachmentDownloadMaxBytes,
  }
}

function listAccountIds(cfg: any): string[] {
  return Object.keys(cfg?.channels?.genteam?.accounts ?? {})
}

// ---------------------------------------------------------------------------
// (1) Auth-exchange — botToken → ws_token + agent_token + routing
// ---------------------------------------------------------------------------

async function authExchange(cfg: GenteamAccountConfig): Promise<AuthResult> {
  const url = `${cfg.endpoint}/api/digital-employee/openclaw/channel/auth`
  const res = await fetch(url, {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
      Authorization: `Bearer ${cfg.botToken}`,
    },
    body: JSON.stringify({
      channel_id: cfg.channelId,
      app_token: cfg.appToken,
      plugin_version: PLUGIN_VERSION,
    }),
  })

  let json: any
  try {
    json = await res.json()
  } catch {
    throw new Error(`[genteam] auth-exchange returned non-JSON (HTTP ${res.status})`)
  }

  if (json?.status !== 0) {
    const detail = json?.error || json?.message || JSON.stringify(json)
    throw new Error(`[genteam] auth-exchange failed (HTTP ${res.status}): ${detail}`)
  }

  const d = json.data ?? {}
  if (!d.ws_token || !d.ws_url || !d.agent_token) {
    throw new Error('[genteam] auth-exchange response missing ws_token/ws_url/agent_token')
  }

  return {
    wsToken: d.ws_token,
    wsUrl: d.ws_url,
    wsTokenTtl: typeof d.ws_token_ttl === 'number' ? d.ws_token_ttl : 60,
    agentToken: d.agent_token,
    agentId: d.agent_id ?? '',
    runtimeId: d.runtime_id ?? '',
    serverId: d.server_id ?? '',
    agentHandle: d.agent_handle ?? '',
    agentDisplayName: d.agent_display_name ?? '',
  }
}

// ---------------------------------------------------------------------------
// agent_tools write-back — POST {verb, ...} with the per-agent token.
// ---------------------------------------------------------------------------

interface AgentToolCall {
  status: number
  ok: boolean
  // Parsed JSON body when the response was JSON, else the raw text.
  json: any
  text: string
}

// One authenticated JSON POST to `/agent_tools/<verb>`. The body ALWAYS carries
// `verb` — the agent_tools request requires a `verb` field, so a body
// without it is rejected with 422 before the endpoint logic runs (this was the
// write-back bug). `signal` lets a turn.abort cancel an in-flight tool call.
async function callAgentTool(
  state: ConnectionState,
  verb: string,
  body: Record<string, any>,
  signal?: AbortSignal,
  timeoutMs: number = TOOL_HTTP_TIMEOUT_MS,
): Promise<AgentToolCall> {
  const { cfg, auth } = state
  const url = `${cfg.endpoint}${AGENT_TOOLS_PREFIX}/${verb}`
  const res = await fetch(url, {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
      Authorization: `Bearer ${auth.agentToken}`,
    },
    body: JSON.stringify({ verb, ...body }),
    // Combine, don't XOR: a caller-supplied turn-abort signal must NOT disarm
    // the per-request stall timeout (the attachment path always passes one).
    signal: combineSignals(signal, timeoutMs),
  })
  const text = await res.text()
  let json: any = null
  try {
    json = JSON.parse(text)
  } catch {
    // non-JSON body — keep the raw text
  }
  // The agent_tools envelope uses {status:0} on success; a non-zero status is a
  // backend-side rejection (permission, idempotency, validation).
  const envelopeOk = json == null || json.status === undefined || json.status === 0
  return { status: res.status, ok: res.ok && envelopeOk, json, text }
}

// Result of materializing an inbound attachment onto the gateway filesystem.
// `source` is a nested {attachment_ref, source} object to match the sandbox /
// local `de attachment-view` result shape exactly (cross-runtime parity).
interface MaterializedAttachment {
  local_path: string
  filename: string
  mime_type: string
  size: number
  sha256: string
  source: { attachment_ref: string; source: string }
}

type DownloadResult =
  | { ok: true; data: MaterializedAttachment }
  | { ok: false; status: number; detail: string; json: any }

// Download an attachment's raw bytes (backend binary mode, `metadata_only:false`)
// and STREAM them straight to a file under the account's download dir — the body
// is never buffered in gateway memory, so a large attachment cannot OOM the
// gateway. The backend has already enforced agent-token auth + channel
// membership + ref/chat validation and stripped the secure-media URL/token; the
// plugin only ever sees the bytes and a small set of safe `X-DE-Attachment-*`
// headers. On top of that we: cap the size (the header can lie or be absent),
// reject a symlinked download dir, write to an UNPREDICTABLE filename with
// O_EXCL + mode 0600 (so a co-tenant cannot pre-plant a symlink at the target to
// redirect/leak the bytes), and clean up any partial on failure.
async function downloadAttachmentToDisk(
  state: ConnectionState,
  body: Record<string, any>,
  signal: AbortSignal | undefined,
): Promise<DownloadResult> {
  const { cfg, auth } = state
  const maxBytes = cfg.attachmentDownloadMaxBytes ?? GENTEAM_ATTACHMENT_DOWNLOAD_MAX_BYTES
  const url = `${cfg.endpoint}${AGENT_TOOLS_PREFIX}/attachment-view`
  const res = await fetch(url, {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
      Authorization: `Bearer ${auth.agentToken}`,
    },
    // Force binary mode regardless of any caller-supplied flag.
    body: JSON.stringify({ ...body, verb: 'attachment-view', metadata_only: false }),
    signal: combineSignals(signal, TOOL_ATTACHMENT_HTTP_TIMEOUT_MS),
  })
  if (!res.ok || res.status >= 400) {
    const text = await res.text().catch(() => '')
    let json: any = null
    try {
      json = JSON.parse(text)
    } catch {
      /* non-JSON error body */
    }
    return { ok: false, status: res.status, detail: text.slice(0, 600), json }
  }
  if (!res.body) {
    return { ok: false, status: res.status, detail: 'empty attachment response body', json: null }
  }

  const h = res.headers
  const filename = h.get('x-de-attachment-filename') || 'file'
  const mime = h.get('x-de-attachment-mime') || ''
  const sizeHdr = h.get('x-de-attachment-size') || ''
  const declaredSize = /^\d+$/.test(sizeHdr) ? Number(sizeHdr) : null
  const refHeader = h.get('x-de-attachment-ref') || String(body.attachment_ref ?? '')
  const source = h.get('x-de-attachment-source') || 'secure_media'

  // Reject an oversized attachment up front when the backend declared a size,
  // so we don't even open the file. (The mid-stream guard below is the real
  // enforcement — the header is advisory and can lie.) Cancel the body so an
  // undrained response stream doesn't pin the connection (resource leak).
  if (declaredSize != null && declaredSize > maxBytes) {
    await res.body.cancel().catch(() => {})
    return {
      ok: false,
      status: 413,
      detail: `attachment is too large to download (${declaredSize} bytes > ${maxBytes} max)`,
      json: null,
    }
  }

  const dir = resolveAttachmentDownloadDir(cfg)
  try {
    mkdirSync(dir, { recursive: true, mode: 0o700 })
  } catch (e: any) {
    // Cancel the still-open body so a bailout before streaming doesn't pin the
    // connection (same leak class as the oversized early-return above).
    await res.body.cancel().catch(() => {})
    return { ok: false, status: 0, detail: `cannot create download dir ${dir}: ${String(e?.message ?? e)}`, json: null }
  }
  // Refuse to write into a symlinked dir — a co-tenant who pre-planted the
  // (configured) dir as a symlink would otherwise redirect every download.
  try {
    if (lstatSync(dir).isSymbolicLink()) {
      await res.body.cancel().catch(() => {})
      return { ok: false, status: 0, detail: `download dir is a symlink, refusing: ${dir}`, json: null }
    }
  } catch {
    /* stat race — fall through; the O_EXCL write below is the real guard */
  }
  const safeName = sanitizeAttachmentBasename(filename)
  const refPart = (refHeader || 'ref').replace(/[^A-Za-z0-9._-]/g, '_')
  // A random component makes the target unpredictable (defeats a pre-planted
  // symlink at a guessable name) AND prevents same-ref re-downloads from
  // overwriting a file the agent may still be reading.
  const finalPath = join(dir, `${refPart}_${randomBytes(6).toString('hex')}_${safeName}`)

  const hasher = createHash('sha256')
  let written = 0
  let overflow = false
  // A pass-through that counts + hashes bytes and trips the cap mid-stream.
  // Erroring here makes `pipeline` tear down the whole chain (source fetch
  // stream + file write stream) and reject, so we then unlink the partial.
  const limiter = new Transform({
    transform(chunk: Buffer, _enc, cb) {
      written += chunk.length
      if (written > maxBytes) {
        overflow = true
        cb(new Error('attachment exceeds the maximum download size'))
        return
      }
      hasher.update(chunk)
      cb(null, chunk)
    },
  })

  try {
    // `flags: 'wx'` (O_EXCL) refuses to follow/clobber an existing symlink or
    // file at the target; `mode: 0o600` keeps the confidential bytes private.
    await pipeline(
      Readable.fromWeb(res.body as any),
      limiter,
      createWriteStream(finalPath, { flags: 'wx', mode: 0o600 }),
    )
  } catch (e: any) {
    await rm(finalPath, { force: true }).catch(() => {})
    if (overflow) {
      return {
        ok: false,
        status: 413,
        detail: `attachment exceeds the maximum download size (${maxBytes} bytes)`,
        json: null,
      }
    }
    // Aborted turn, network/TLS error, or disk failure mid-stream.
    return { ok: false, status: 0, detail: String(e?.message ?? e), json: null }
  }

  return {
    ok: true,
    data: {
      local_path: finalPath,
      filename,
      mime_type: mime,
      size: written,
      sha256: hasher.digest('hex'),
      source: { attachment_ref: refHeader, source },
    },
  }
}

// Visible-reply write-back used by the dispatch deliver fallback + the proactive
// outbound adapter. `verb: "message-send"` is mandatory.
async function sendGenteamMessage(
  state: ConnectionState,
  target: string,
  content: string,
  parentMessage?: string,
): Promise<void> {
  const { log } = state
  const body: Record<string, any> = { target, content }
  if (parentMessage) body.parent_message = parentMessage
  const res = await callAgentTool(state, 'message-send', body)
  if (!res.ok) {
    const detail = res.json?.error || res.json?.message || res.text.slice(0, 300)
    const msg = `[genteam] message-send failed (HTTP ${res.status}): ${detail}`
    log?.error?.(msg)
    throw new Error(msg)
  }
}

// ---------------------------------------------------------------------------
// `de` tool surface — the agent_tools verbs exposed as model-callable tools
// named `de_<verb>` (one per `de` CLI verb), so the gateway agent uses the SAME
// verbs the local/sandbox `de` CLI exposes and the SAME ones the shared system
// prompt teaches — no separate naming / translation layer. The agent's VISIBLE
// reply is `de_message_send` (`target` defaults to the current conversation);
// the read/task/attachment/reaction tools are the rest of the surface. The
// verb→field mapping matches the local/sandbox `de` CLI verbs and the
// agent_tools request schema.
//
// `--await-reply` blocking ask and standalone `reply-wait` are deferred —
// the gateway agent cannot block-poll a turn the way the CLI does.
// ---------------------------------------------------------------------------

function toolText(text: string, isError = false): any {
  return { content: [{ type: 'text', text }], ...(isError ? { isError: true } : {}) }
}

// Render an agent_tools response as the tool result text the model reads.
function toolResultFromCall(res: AgentToolCall): any {
  if (!res.ok) {
    const detail =
      (res.json && (res.json.error || res.json.message)) || res.text.slice(0, 600) || `HTTP ${res.status}`
    return toolText(`error (HTTP ${res.status}): ${detail}`, true)
  }
  // Prefer the parsed JSON (compact) so the model gets structured data; fall
  // back to the raw text for non-JSON success bodies.
  const out = res.json != null ? JSON.stringify(res.json) : res.text
  return toolText(out || '{"status":0}')
}

interface DeToolDef {
  // Tool name the model calls (and the manifest `contracts.tools` entry).
  name: string
  // agent_tools verb.
  verb: string
  description: string
  parameters: any
  // Map validated tool params → the agent_tools JSON body (verb is added by
  // callAgentTool). `turn` supplies the reply-to-current-conversation default.
  buildBody: (params: any, turn: ActiveTurn | undefined) => Record<string, any>
}

// JSON verbs (plain POST of the agent_tools request fields). attachment-view is
// included in metadata mode (the model cannot consume raw bytes; metadata +
// ref is the useful surface). Binary download / multipart upload diverge and
// are handled by dedicated tools below.
const DE_TOOL_DEFS: DeToolDef[] = [
  {
    name: 'de_server_info',
    verb: 'server-info',
    description:
      'Get the current GenTeam server context: server name, the channels/DMs this agent can see, and members. Use this to discover where you can post and who is present.',
    parameters: Type.Object({}),
    buildBody: () => ({}),
  },
  {
    name: 'de_channel_members',
    verb: 'channel-members',
    description:
      'List the members (humans and agents) of a GenTeam channel or DM. `target` is a channel like "#all" or a dm target.',
    parameters: Type.Object({
      target: Type.String({ description: 'Channel/DM target, e.g. "#all" or "dm:@alice".' }),
    }),
    buildBody: (p) => ({ target: p.target }),
  },
  {
    name: 'de_channel_files',
    verb: 'channel-files',
    description: 'List files shared in a GenTeam channel. Paginate with `file_cursor`.',
    parameters: Type.Object({
      target: Type.String({ description: 'Channel target, e.g. "#all".' }),
      file_cursor: Type.Optional(Type.String({ description: 'Pagination cursor from a prior call.' })),
    }),
    buildBody: (p) => ({ target: p.target, ...(p.file_cursor ? { file_cursor: p.file_cursor } : {}) }),
  },
  {
    name: 'de_share_project',
    verb: 'share-project',
    description:
      'Share a Genspark project you created (via gsk create_task) with the current channel. Without `add`, lists who it is shared with; with `add` ("current" = every human member of the current channel, or comma-separated @handles) it grants those humans read access. Read-only, humans only — it never shares to an agent.',
    parameters: Type.Object({
      project_id: Type.String({ description: 'The id of the project to share.' }),
      add: Type.Optional(
        Type.String({
          description:
            '"current" to share with all human members of the current channel, or comma-separated @handles. Omit to just list current shares.',
        }),
      ),
    }),
    buildBody: (p) => ({
      project_id: p.project_id,
      ...(p.add ? { op: 'set', add: p.add } : { op: 'get' }),
    }),
  },
  {
    name: 'de_message_read',
    verb: 'message-read',
    description:
      'Read message history for a channel/DM/thread target. The inbound turn ships no history by design — call this to see prior messages before replying. Use `before_message` / `around_message` (a comet_message_id, e.g. from a `de_message_search` hit) to load the page older than, or the window around, a specific message.',
    parameters: Type.Object({
      target: Type.String({ description: 'Channel/DM/thread target to read.' }),
      limit: Type.Optional(Type.Integer({ minimum: 1, maximum: 200, description: 'Max messages (default backend cap).' })),
      before_message: Type.Optional(Type.String({ description: 'comet_message_id: load the page of messages older than this one.' })),
      around_message: Type.Optional(Type.String({ description: 'comet_message_id: load the window around this message (the anchor row itself is excluded). Mutually exclusive with before_message.' })),
      before_cursor: Type.Optional(Type.String({ description: 'Pagination cursor for older messages.' })),
    }),
    buildBody: (p) => ({
      target: p.target,
      ...(p.limit != null ? { limit: p.limit } : {}),
      ...(p.before_message ? { before_message: p.before_message } : {}),
      ...(p.around_message ? { around_message: p.around_message } : {}),
      ...(p.before_cursor ? { before_cursor: p.before_cursor } : {}),
    }),
  },
  {
    name: 'de_message_search',
    verb: 'message-search',
    description:
      'Full-text search of visible message history by keyword — use it to recover something discussed earlier that is no longer in context. Without `target` it searches every channel you are a member of; with `target` just that one. Returns relevance-ranked hits (a small default; pass `limit` up to 30 for more), each carrying a `target` + `comet_message_id`; chain `de_message_read` with `around_message`=<comet_message_id> to load the surrounding context.',
    parameters: Type.Object({
      query: Type.String({ description: 'Free-text keyword(s) to search for.' }),
      target: Type.Optional(Type.String({ description: 'Restrict to one channel/DM/thread target; omit to search all your channels.' })),
      limit: Type.Optional(Type.Integer({ minimum: 1, maximum: 30, description: 'Max hits (default 8, capped at 30).' })),
    }),
    buildBody: (p) => ({
      query: p.query,
      ...(p.target ? { target: p.target } : {}),
      ...(p.limit != null ? { limit: p.limit } : {}),
    }),
  },
  {
    name: 'de_thread_read',
    verb: 'thread-read',
    description: 'Read messages in a GenTeam thread. `thread` is a thread target (channel:shortId).',
    parameters: Type.Object({
      thread: Type.String({ description: 'Thread target, e.g. "#all:1a2b3c4d".' }),
      limit: Type.Optional(Type.Integer({ minimum: 1, maximum: 200 })),
    }),
    buildBody: (p) => ({ thread: p.thread, ...(p.limit != null ? { limit: p.limit } : {}) }),
  },
  {
    name: 'de_thread_unfollow',
    verb: 'thread-unfollow',
    description: 'Stop following a GenTeam thread so its replies no longer wake this agent.',
    parameters: Type.Object({
      thread: Type.String({ description: 'Thread target to unfollow.' }),
    }),
    buildBody: (p) => ({ thread: p.thread }),
  },
  {
    name: 'de_attachment_list',
    verb: 'attachment-list',
    description: 'List attachments on a message or in a channel.',
    parameters: Type.Object({
      target: Type.Optional(Type.String({ description: 'Channel target to list attachments in.' })),
      message_id: Type.Optional(Type.String({ description: 'A specific comet_message_id to list attachments for.' })),
      limit: Type.Optional(Type.Integer({ minimum: 1, maximum: 200 })),
    }),
    buildBody: (p) => ({
      ...(p.target ? { target: p.target } : {}),
      ...(p.message_id ? { message_id: p.message_id } : {}),
      ...(p.limit != null ? { limit: p.limit } : {}),
    }),
  },
  {
    name: 'de_task_list',
    verb: 'task-list',
    description: 'List GenTeam tasks, optionally filtered by channel target, status, or assignee.',
    parameters: Type.Object({
      target: Type.Optional(Type.String({ description: 'Channel target to scope tasks to.' })),
      status: Type.Optional(Type.String({ description: 'todo | in_progress | in_review | done | closed.' })),
      assignee: Type.Optional(Type.String({ description: 'Assignee handle filter.' })),
    }),
    buildBody: (p) => ({
      ...(p.target ? { target: p.target } : {}),
      ...(p.status ? { status: p.status } : {}),
      ...(p.assignee ? { assignee: p.assignee } : {}),
    }),
  },
  {
    name: 'de_task_claim',
    verb: 'task-claim',
    description:
      'Claim a GenTeam task by number (within a channel) or by task_id; or pass `message_id` (+ `target`) to convert a top-level message into a task and claim it in one step. Only the assignee should work a claimed task.',
    parameters: Type.Object({
      task_number: Type.Optional(Type.Integer({ minimum: 1, description: 'Channel-local task number, e.g. 3.' })),
      task_id: Type.Optional(Type.String({ description: 'Global task id.' })),
      message_id: Type.Optional(Type.String({ description: 'comet_message_id of a top-level message to convert to a task and claim.' })),
      target: Type.Optional(Type.String({ description: 'Channel target (required with task_number or message_id).' })),
    }),
    buildBody: (p) => ({
      ...(p.task_number != null ? { task_number: p.task_number } : {}),
      ...(p.task_id ? { task_id: p.task_id } : {}),
      ...(p.message_id ? { message_id: p.message_id } : {}),
      ...(p.target ? { target: p.target } : {}),
    }),
  },
  {
    name: 'de_task_unclaim',
    verb: 'task-unclaim',
    description: 'Release a GenTeam task you previously claimed.',
    parameters: Type.Object({
      task_number: Type.Optional(Type.Integer({ minimum: 1 })),
      task_id: Type.Optional(Type.String()),
      target: Type.Optional(Type.String({ description: 'Channel target (required with task_number).' })),
    }),
    buildBody: (p) => ({
      ...(p.task_number != null ? { task_number: p.task_number } : {}),
      ...(p.task_id ? { task_id: p.task_id } : {}),
      ...(p.target ? { target: p.target } : {}),
    }),
  },
  {
    name: 'de_task_update',
    verb: 'task-update',
    description: 'Update a GenTeam task: change status, assignee, or record a reason. Status is one of todo | in_progress | in_review | done | closed.',
    parameters: Type.Object({
      task_number: Type.Optional(Type.Integer({ minimum: 1 })),
      task_id: Type.Optional(Type.String()),
      target: Type.Optional(Type.String({ description: 'Channel target (required with task_number).' })),
      status: Type.Optional(Type.String()),
      assignee: Type.Optional(Type.String()),
      reason: Type.Optional(Type.String()),
    }),
    buildBody: (p) => ({
      ...(p.task_number != null ? { task_number: p.task_number } : {}),
      ...(p.task_id ? { task_id: p.task_id } : {}),
      ...(p.target ? { target: p.target } : {}),
      ...(p.status ? { status: p.status } : {}),
      ...(p.assignee ? { assignee: p.assignee } : {}),
      ...(p.reason ? { reason: p.reason } : {}),
    }),
  },
  {
    name: 'de_task_read',
    verb: 'task-read',
    description: 'Read a GenTeam task (its metadata, status, assignee, and origin message).',
    parameters: Type.Object({
      task_number: Type.Optional(Type.Integer({ minimum: 1 })),
      task_id: Type.Optional(Type.String()),
      target: Type.Optional(Type.String({ description: 'Channel target (required with task_number).' })),
    }),
    buildBody: (p) => ({
      ...(p.task_number != null ? { task_number: p.task_number } : {}),
      ...(p.task_id ? { task_id: p.task_id } : {}),
      ...(p.target ? { target: p.target } : {}),
    }),
  },
  {
    name: 'de_reaction_add',
    verb: 'reaction-add',
    description: 'Add an emoji reaction to a GenTeam message.',
    parameters: Type.Object({
      message_id: Type.String({ description: 'comet_message_id of the message to react to.' }),
      emoji: Type.String({ description: 'The emoji character(s), e.g. "👍".' }),
      target: Type.Optional(Type.String({ description: 'Channel target the message is in.' })),
    }),
    buildBody: (p) => ({ message_id: p.message_id, emoji: p.emoji, ...(p.target ? { target: p.target } : {}) }),
  },
  {
    name: 'de_reaction_remove',
    verb: 'reaction-remove',
    description: 'Remove an emoji reaction you previously added to a GenTeam message.',
    parameters: Type.Object({
      message_id: Type.String({ description: 'comet_message_id of the message.' }),
      emoji: Type.String({ description: 'The emoji character(s) to remove.' }),
      target: Type.Optional(Type.String({ description: 'Channel target the message is in.' })),
    }),
    buildBody: (p) => ({ message_id: p.message_id, emoji: p.emoji, ...(p.target ? { target: p.target } : {}) }),
  },
  {
    name: 'de_message_send',
    verb: 'message-send',
    description:
      'Send a visible GenTeam message. THIS IS HOW YOU REPLY — your assistant text is never shown to anyone, so you MUST call this tool to say anything visible. `target` defaults to the current conversation when omitted; pass another channel/DM/thread for a proactive/cross-target send. Open a new thread with `parent_message`. Message bodies are capped at 8000 characters — split longer replies into multiple calls: set `progress: true` on every non-final chunk, number the chunks (e.g. "(part 2/5)") so no two are identical, and finish with exactly one ordinary final call without it; if the reply includes files, make the `de_message_send_attachment` call the single final send (caption via its `content`).',
    parameters: Type.Object({
      content: Type.String({ description: 'The message body (visible to humans and agents).' }),
      target: Type.Optional(Type.String({ description: 'Where to post; defaults to the current conversation.' })),
      parent_message: Type.Optional(Type.String({ description: 'Full comet_message_id to open/post into a thread.' })),
      progress: Type.Optional(
        Type.Boolean({
          description:
            'Set true only for a non-final progress update. The default is a final reply and immediately ends the busy indicator.',
        }),
      ),
      post_to_channel: Type.Optional(
        Type.Boolean({
          description:
            "Set true to reply to the task's ORIGIN channel instead of its thread. Only use after a send was rejected with `task_reply_requires_thread` and you deliberately want the origin channel.",
        }),
      ),
    }),
    buildBody: (p, turn) => ({
      content: p.content,
      target: p.target || turn?.replyTarget,
      // Client-minted command identity (#42717 stage 5): one per send call,
      // so the backend can collapse ad-hoc retries onto one manifest row.
      operation_id: randomUUID(),
      ...(p.parent_message ? { parent_message: p.parent_message } : {}),
      ...(p.progress ? { progress: true } : {}),
      ...(p.post_to_channel ? { post_to_channel: true } : {}),
    }),
  },
]

// Build the per-turn `de` tool list for a tool factory invocation. Scoped to
// genteam turns; resolves the live ConnectionState by account at execute time
// (the token may have rotated since the factory ran). Fails closed when the
// account/connection cannot be resolved — `agentAccountId` is runtime metadata,
// not a security boundary, so its presence is never treated as authorization.
function buildGenteamTools(toolCtx: any): any[] {
  // Attach ONLY to genteam-channel turns — require an EXACT match so the
  // surface never leaks onto another channel's agent. A missing / empty
  // messageChannel (tool enumeration or a non-channel run) yields no tools too:
  // these tools are meaningless off a genteam turn and the live connection is
  // the source of truth at execute time. (Regression guard: a bare
  // `channel && channel !== 'genteam'` would let an absent channel through.)
  const channel = toolCtx?.messageChannel
  if (channel !== 'genteam') return []
  const accountId: string = toolCtx?.agentAccountId ?? 'default'

  function currentTurn(state: ConnectionState): ActiveTurn | undefined {
    // FIFO-serial per agent → at most one live turn. Pick the most recent.
    let latest: ActiveTurn | undefined
    for (const t of state.turns.values()) latest = t
    return latest
  }

  const jsonTools = DE_TOOL_DEFS.map((def) => ({
    name: def.name,
    label: def.name,
    description: def.description,
    parameters: def.parameters,
    execute: async (
      _toolCallId: string,
      params: any,
      signal: AbortSignal | undefined,
    ): Promise<any> => {
      const state = connectionsByAccount.get(accountId)
      if (!state) {
        // eslint-disable-next-line no-console
        console.warn(
          `[genteam] de tool "${def.name}" found no live connection for account "${accountId}"; known accounts=[${[...connectionsByAccount.keys()].join(',')}]`,
        )
        return toolText(
          `error: the GenTeam connection for account "${accountId}" is not active right now; retry shortly.`,
          true,
        )
      }
      const turn = currentTurn(state)
      const body = def.buildBody(params ?? {}, turn)
      if (def.verb === 'message-send' && !body.target) {
        return toolText('error: no target — pass `target` (no current conversation to default to).', true)
      }
      try {
        const res = await callAgentTool(state, def.verb, body, signal)
        if (def.verb === 'message-send' && res.ok && turn) turn.sentCount += 1
        return toolResultFromCall(res)
      } catch (e: any) {
        return toolText(`error: ${String(e?.message ?? e)}`, true)
      }
    },
  }))

  // Inbound attachment read — by default DOWNLOADS the bytes to a local file on
  // the gateway and returns its `local_path` (the agent then Reads that path;
  // for images, Read it directly), reaching parity with the sandbox/local `de
  // attachment-view`. The body is streamed straight to disk by
  // `downloadAttachmentToDisk` (never buffered), the filename is sanitized to a
  // safe basename, and a size cap guards against OOM/disk abuse. Passing
  // `metadata_only: true` keeps the cheap pointer-only JSON path (filename /
  // mime / size, no byte egress).
  const attachmentView = {
    name: 'de_attachment_view',
    label: 'de_attachment_view',
    description:
      'Download an inbound attachment to a local file on this gateway and return its `local_path` (Read that path to inspect it; for image attachments, Read it directly), plus filename, mime type, size, and sha256. Identify it by `attachment_ref` (e.g. "<message_id>:0") or by `message_id` + `attachment_index`. Pass `metadata_only: true` to fetch only the filename/mime/size without downloading the bytes.',
    parameters: Type.Object({
      attachment_ref: Type.Optional(Type.String({ description: 'Canonical "<message_id>:<index>" ref.' })),
      message_id: Type.Optional(Type.String({ description: 'Message id (with attachment_index).' })),
      attachment_index: Type.Optional(Type.Integer({ minimum: 0, maximum: 64 })),
      metadata_only: Type.Optional(
        Type.Boolean({ description: 'Return only filename/mime/size without downloading the bytes.' }),
      ),
    }),
    execute: async (
      _toolCallId: string,
      params: any,
      signal: AbortSignal | undefined,
    ): Promise<any> => {
      const state = connectionsByAccount.get(accountId)
      if (!state) {
        return toolText(`error: the GenTeam connection for account "${accountId}" is not active right now.`, true)
      }
      const ids: Record<string, any> = {}
      if (params?.attachment_ref) ids.attachment_ref = String(params.attachment_ref)
      if (params?.message_id) ids.message_id = String(params.message_id)
      if (params?.attachment_index != null) ids.attachment_index = params.attachment_index
      if (!ids.attachment_ref && !ids.message_id) {
        return toolText('error: pass `attachment_ref` (e.g. "<message_id>:0") or `message_id`.', true)
      }
      try {
        // Cheap pointer-only mode: the plain JSON agent_tools call.
        if (params?.metadata_only === true) {
          const res = await callAgentTool(state, 'attachment-view', { metadata_only: true, ...ids }, signal)
          return toolResultFromCall(res)
        }
        // Default: stream the bytes to disk and hand back the local path.
        const r = await downloadAttachmentToDisk(state, ids, signal)
        if (r.ok) return toolText(JSON.stringify({ ok: true, ...r.data }))
        if (r.json) return toolResultFromCall({ status: r.status, ok: false, json: r.json, text: r.detail })
        return toolText(`error: ${r.detail}`, true)
      } catch (e: any) {
        return toolText(`error: ${String(e?.message ?? e)}`, true)
      }
    },
  }

  // Local-file attachment send — reads file paths from the gateway agent's own
  // filesystem and uploads them via the shared 1 GiB session+SAS direct-upload
  // core (per-file init → streamed PUT straight to Azure Blob → ONE batch
  // finalize), the SAME path the Local Computer daemon uses. The legacy
  // multipart endpoint is no longer used.
  const sendAttachment = {
    name: 'de_message_send_attachment',
    label: 'de_message_send_attachment',
    description:
      'Send one or more local files as a GenTeam message attachment (up to 10, 1 GiB each). `paths` must be absolute paths under configured attachment roots; if no roots are configured, local uploads are disabled. Optional `content` is the caption. `target` defaults to the current conversation.',
    parameters: Type.Object({
      paths: Type.Array(Type.String(), { minItems: 1, maxItems: GENTEAM_ATTACHMENT_MAX_COUNT, description: 'Local file paths to upload.' }),
      content: Type.Optional(Type.String({ description: 'Optional caption.' })),
      target: Type.Optional(Type.String({ description: 'Where to post; defaults to the current conversation.' })),
      parent_message: Type.Optional(Type.String({ description: 'Open/post into a thread.' })),
    }),
    execute: async (
      _toolCallId: string,
      params: any,
      signal: AbortSignal | undefined,
    ): Promise<any> => {
      const state = connectionsByAccount.get(accountId)
      if (!state) {
        return toolText(`error: the GenTeam connection for account "${accountId}" is not active right now.`, true)
      }
      const turn = currentTurn(state)
      const target: string = params?.target || turn?.replyTarget || ''
      if (!target) return toolText('error: no target — pass `target`.', true)
      const rawPaths: string[] = Array.isArray(params?.paths) ? params.paths : []
      if (rawPaths.length === 0) return toolText('error: `paths` must list at least one file.', true)
      // Fail fast on the count cap before the (per-path) allow-root resolution
      // so an over-count batch returns the count error regardless of which
      // paths it contains — matching the daemon, where the shared core checks
      // the count before touching the filesystem. (The shared core re-checks.)
      if (rawPaths.length > GENTEAM_ATTACHMENT_MAX_COUNT) {
        return toolText(`error: too many attachments (max ${GENTEAM_ATTACHMENT_MAX_COUNT}).`, true)
      }

      // Resolve every path against the configured attachment roots BEFORE the
      // upload — the agent may only read files under an allow-listed root.
      const safePaths: string[] = []
      for (const p of rawPaths) {
        const safePath = normalizeAllowedAttachmentPath(String(p), state.cfg.attachmentRoots)
        if (!safePath) {
          return toolText(
            `error: attachment path is outside the allowed roots (${formatAttachmentRoots(state.cfg.attachmentRoots)}): ${p}`,
            true,
          )
        }
        safePaths.push(safePath)
      }

      // SAME shared session+SAS core the Local Computer daemon uses: per-file
      // init → streamed PUT straight to Azure Blob → ONE batch finalize. So a
      // 1 GiB file never
      // buffers in the gateway's memory or crosses the backend, and all files
      // land in ONE secure_media message rather than the legacy 99 MiB
      // multipart path. Transport is this plugin's agent-token-authed POST to
      // the configured endpoint; the SAS PUT goes direct to storage inside the
      // shared core.
      const post: AgentToolPost = async (verb, payload, sig) => {
        const r = await callAgentTool(state, verb, payload, sig, TOOL_ATTACHMENT_HTTP_TIMEOUT_MS)
        return { status: r.status, body: r.text }
      }
      try {
        const result = await uploadAttachmentsViaSession({
          files: safePaths,
          target,
          text: params?.content != null ? String(params.content) : null,
          parentMessage: params?.parent_message != null ? String(params.parent_message) : null,
          post,
          signal,
          putTimeoutMs: TOOL_ATTACHMENT_HTTP_TIMEOUT_MS,
        })
        if (result.ok) {
          if (turn) turn.sentCount += 1
          let json: any = null
          try {
            json = JSON.parse(result.body)
          } catch {
            /* non-JSON 2xx body — passed through as text */
          }
          return toolResultFromCall({ status: result.status, ok: true, json, text: result.body })
        }
        // A backend HTTP rejection carries the server's typed envelope — render
        // it through the same `error (HTTP <status>): <detail>` formatter the
        // other de tools use, for a consistent model-facing error shape. A
        // client-side failure (path / size / network / hashing) has no server
        // body, so surface its typed message directly.
        if (result.serverBody !== undefined) {
          let json: any = null
          try {
            json = JSON.parse(result.serverBody)
          } catch {
            /* non-JSON backend body */
          }
          return toolResultFromCall({
            status: result.httpStatus ?? 0,
            ok: false,
            json,
            text: result.serverBody,
          })
        }
        return toolText(`error: ${result.message}`, true)
      } catch (e: any) {
        // Defensive: the shared core returns a discriminated result rather than
        // throwing, but a truly unexpected throw (e.g. an fs race) must still
        // surface as a clean tool error, not a rejected execute() promise.
        return toolText(`error: ${String(e?.message ?? e)}`, true)
      }
    },
  }

  return [...jsonTools, attachmentView, sendAttachment]
}

// All de tool names — also declared in openclaw.plugin.json `contracts.tools`
// (the registry rejects any registerTool name not in the manifest).
// `de_attachment_view` (byte-materializing) and `de_message_send_attachment`
// are dedicated tools, not plain JSON verbs, so they're listed explicitly.
const DE_TOOL_NAMES: string[] = [
  ...DE_TOOL_DEFS.map((d) => d.name),
  'de_attachment_view',
  'de_message_send_attachment',
]

// ---------------------------------------------------------------------------
// WS close-code classification — the channel WIRE contract.
//   4401 token_revoked        → fatal (re-auth required)
//   4404 connection_deleted   → fatal (connection deleted on the server)
//   4426 protocol / version   → reconnectable (re-auth may produce a fresh ws_token)
//   1013 try_again_later      → reconnectable
//   1xxx transport / restart  → reconnectable
// Reason-aware so a future backend that distinguishes reasons keeps working.
// ---------------------------------------------------------------------------

const CLOSE_TOKEN_REVOKED = 4401
const CLOSE_CONNECTION_DELETED = 4404
const CLOSE_PROTOCOL = 4426
const CLOSE_TRY_AGAIN_LATER = 1013

interface CloseDisposition {
  fatal: boolean
  reason: string
}

function classifyClose(code: number | undefined, rawReason: string): CloseDisposition {
  const reason = (rawReason ?? '').trim()
  switch (code) {
    case CLOSE_TOKEN_REVOKED:
      return {
        fatal: true,
        reason: reason || 'token revoked — credentials were regenerated or the connection was invalidated',
      }
    case CLOSE_CONNECTION_DELETED:
      return {
        fatal: true,
        reason: reason || 'connection deleted on the server',
      }
    case CLOSE_PROTOCOL:
      // Protocol / handshake mismatch — a fresh auth-exchange may resolve it
      // (e.g. a stale one-time ws_token), so keep it reconnectable.
      return { fatal: false, reason: reason || 'protocol error' }
    case CLOSE_TRY_AGAIN_LATER:
      return { fatal: false, reason: reason || 'server asked to retry later' }
    default:
      // 1xxx transport / restart signals, and any unmapped code — reconnect.
      return { fatal: false, reason: reason || (code !== undefined ? `WS close code ${code}` : '') }
  }
}

// ---------------------------------------------------------------------------
// Reconnect backoff — exponential with jitter (daemon model: 1s base, 30s cap)
// ---------------------------------------------------------------------------

const BACKOFF_BASE_MS = 1_000
const BACKOFF_MAX_MS = 30_000

function getReconnectDelay(attempt: number): number {
  const exp = Math.min(BACKOFF_MAX_MS, BACKOFF_BASE_MS * 2 ** attempt)
  const jitter = Math.floor(Math.random() * BACKOFF_BASE_MS)
  return Math.min(BACKOFF_MAX_MS, exp + jitter)
}

// ---------------------------------------------------------------------------
// Channel WS — connect, ready handshake, ping/pong watchdog, turn dispatch
// ---------------------------------------------------------------------------

const PING_INTERVAL_MS = 30_000
const WATCHDOG_TIMEOUT_MS = 90_000
// Deadline from WS construction through ready.ack. The inbound watchdog only
// arms on the first inbound frame / ready.ack, so without this a backend that
// accepts the socket but never sends ready.ack (or never closes) would leave
// the connect promise hanging forever — close the socket and
// finish non-fatal so the monitor's backoff retries.
const CONNECT_DEADLINE_MS = 25_000

interface ConnectWsResult {
  /** true when the close was fatal and the monitor must stop reconnecting. */
  fatal: boolean
  /** whether the socket reached the connected (ready) state at least once. */
  connectedOnce: boolean
}

/**
 * Open one channel WS connection and pump it until close. Resolves with the
 * close disposition so the monitor loop can decide reconnect-vs-stop.
 */
function connectGenteamWs(opts: {
  state: ConnectionState
  gatewayCtx: GatewayCtx
  onConnected: () => void
  onEvent: () => void
}): Promise<ConnectWsResult> {
  const { state, gatewayCtx } = opts
  const { cfg, auth, log } = state
  const { abortSignal } = gatewayCtx

  return new Promise<ConnectWsResult>((resolvePromise) => {
    const fullUrl = `${auth.wsUrl}?token=${encodeURIComponent(auth.wsToken)}`
    log?.info?.(`[genteam] WS connecting to ${auth.wsUrl}`)

    const ws = new WsWebSocket(fullUrl) as unknown as WebSocket
    let connectedOnce = false
    let settled = false
    let pingTimer: ReturnType<typeof setInterval> | null = null
    let watchdogTimer: ReturnType<typeof setTimeout> | null = null
    let connectDeadlineTimer: ReturnType<typeof setTimeout> | null = null

    function clearTimers(): void {
      if (pingTimer) {
        clearInterval(pingTimer)
        pingTimer = null
      }
      if (watchdogTimer) {
        clearTimeout(watchdogTimer)
        watchdogTimer = null
      }
      if (connectDeadlineTimer) {
        clearTimeout(connectDeadlineTimer)
        connectDeadlineTimer = null
      }
    }

    // Arm the connect/handshake deadline at construction. If neither ready.ack
    // nor a close arrives within the bound (backend accepted the socket but
    // stalled), close it to force the close handler, which finishes the promise
    // {fatal:false} so the monitor reconnects with backoff. Cleared on ready.ack
    // (handshake complete) and in clearTimers (on settle).
    connectDeadlineTimer = setTimeout(() => {
      if (settled || connectedOnce) return
      log?.warn?.('[genteam] WS handshake stalled before ready.ack; closing to force reconnect')
      try {
        ws.close()
      } catch {
        /* ignore */
      }
    }, CONNECT_DEADLINE_MS)

    function resetWatchdog(): void {
      if (watchdogTimer) clearTimeout(watchdogTimer)
      watchdogTimer = setTimeout(() => {
        log?.warn?.('[genteam] WS inbound timeout; closing to force reconnect')
        try {
          ws.close()
        } catch {
          /* ignore */
        }
      }, WATCHDOG_TIMEOUT_MS)
    }

    function finish(result: ConnectWsResult): void {
      if (settled) return
      settled = true
      clearTimers()
      resolvePromise(result)
    }

    function onAbort(): void {
      try {
        ws.close()
      } catch {
        /* ignore */
      }
    }
    abortSignal.addEventListener('abort', onAbort, { once: true })

    ws.addEventListener('open', () => {
      log?.info?.('[genteam] WS open; sending ready')
      try {
        ws.send(
          JSON.stringify({
            type: 'ready',
            plugin_version: PLUGIN_VERSION,
            channel_id: cfg.channelId,
            // The gateway agent has the de tool surface + applied system prompt,
            // so it can read/send/manage — advertise the richer capability set.
            capabilities: ['text', 'tools'],
          }),
        )
      } catch (e) {
        log?.error?.(`[genteam] failed to send ready: ${e}`)
      }
    })

    ws.addEventListener('message', (event: MessageEvent) => {
      const text = typeof event.data === 'string' ? event.data : String(event.data)
      let frame: any
      try {
        frame = JSON.parse(text)
      } catch {
        log?.warn?.(`[genteam] WS unparseable frame: ${text.slice(0, 120)}`)
        return
      }

      // Any inbound frame proves the link is live.
      resetWatchdog()
      opts.onEvent()

      const ftype = frame?.type
      if (ftype === 'ready.ack') {
        connectedOnce = true
        // Handshake complete — the inbound watchdog (reset above) now guards
        // liveness, so disarm the connect deadline.
        if (connectDeadlineTimer) {
          clearTimeout(connectDeadlineTimer)
          connectDeadlineTimer = null
        }
        log?.info?.(
          `[genteam] ready.ack connection_id=${frame.connection_id ?? '?'} ` +
            `agent=${auth.agentHandle || auth.agentId}`,
        )
        opts.onConnected()
        // Start application ping once the handshake completes.
        if (pingTimer) clearInterval(pingTimer)
        pingTimer = setInterval(() => {
          if (ws.readyState === WsWebSocket.OPEN) {
            try {
              ws.send(JSON.stringify({ type: 'ping', ts: Date.now() }))
            } catch {
              /* dropped while closing */
            }
          }
        }, PING_INTERVAL_MS)
        resetWatchdog()
        return
      }

      if (ftype === 'ping') {
        // Backend-initiated heartbeat — respond pong (echo ts when present).
        const pong: Record<string, any> = { type: 'pong' }
        if (typeof frame.ts === 'number') pong.ts = frame.ts
        if (ws.readyState === WsWebSocket.OPEN) {
          try {
            ws.send(JSON.stringify(pong))
          } catch {
            /* ignore */
          }
        }
        return
      }

      if (ftype === 'pong') {
        // Liveness only — watchdog already reset above.
        return
      }

      if (ftype === 'turn.start') {
        dispatchTurnToAgent(state, gatewayCtx, frame as TurnStartFrame, ws).catch((e) => {
          log?.error?.(`[genteam] turn dispatch error: ${e}`)
        })
        return
      }

      if (ftype === 'turn.abort') {
        // Stop request for an in-flight turn. Cancel the run via its
        // AbortController; dispatchTurnToAgent emits turn.error so the backend
        // inbox/runtime terminalizes through the normal completion path.
        const turnId = typeof frame.turn_id === 'string' ? frame.turn_id : ''
        const active = turnId ? state.turns.get(turnId) : undefined
        if (active) {
          log?.info?.(`[genteam] turn.abort received for ${turnId}; aborting run`)
          try {
            active.abort.abort()
          } catch {
            /* ignore */
          }
        } else if (turnId) {
          // The abort arrived before the turn registered (turn.start dispatch
          // hasn't reached state.turns yet). Record it so dispatchTurnToAgent
          // aborts immediately once it registers, instead of dropping the Stop
          // on the floor. Bounded so a stream of stale aborts can't grow it.
          log?.info?.(`[genteam] turn.abort for not-yet-registered turn ${turnId}; queuing`)
          if (state.pendingAborts.size >= 64) state.pendingAborts.clear()
          state.pendingAborts.add(turnId)
        } else {
          log?.info?.('[genteam] turn.abort with no turn_id — ignoring')
        }
        return
      }

      log?.info?.(`[genteam] WS ignoring frame type=${ftype}`)
    })

    ws.addEventListener('error', (event: Event) => {
      log?.error?.(`[genteam] WS error: ${(event as any).message || 'unknown'}`)
    })

    ws.addEventListener('close', (event: CloseEvent) => {
      abortSignal.removeEventListener('abort', onAbort)
      const disposition = classifyClose(event.code, event.reason || '')
      const reasonTail = disposition.reason ? ` reason="${disposition.reason}"` : ''
      if (disposition.fatal) {
        log?.error?.(`[genteam] WS closed FATAL: code=${event.code}${reasonTail}. Will not reconnect.`)
      } else {
        log?.info?.(`[genteam] WS closed: code=${event.code}${reasonTail}`)
      }
      finish({ fatal: disposition.fatal, connectedOnce })
    })
  })
}

// ---------------------------------------------------------------------------
// Turn dispatch — build MsgContext, apply the system prompt, run the agent
// (which replies via the de_message_send tool), ack.
// ---------------------------------------------------------------------------

async function dispatchTurnToAgent(
  state: ConnectionState,
  gatewayCtx: GatewayCtx,
  turn: TurnStartFrame,
  ws: WebSocket,
): Promise<void> {
  const { log } = state
  const { channelRuntime, accountId } = gatewayCtx

  const replyTarget = turn.reply_target
  const parentMessage = turn.parent_message

  // Completion frames MUST carry agent_id + runtime_id: the backend's
  // the backend drops any turn.done/turn.error that lacks them
  // (it re-validates the bound agent + runtime before finalizing the inbox).
  function emitTurnDone(): void {
    if (ws.readyState !== WsWebSocket.OPEN) return
    try {
      ws.send(
        JSON.stringify({
          type: 'turn.done',
          turn_id: turn.turn_id,
          agent_id: turn.agent_id,
          runtime_id: turn.runtime_id,
        }),
      )
    } catch {
      /* ignore */
    }
  }
  function emitTurnError(error: string): void {
    if (ws.readyState !== WsWebSocket.OPEN) return
    try {
      ws.send(
        JSON.stringify({
          type: 'turn.error',
          turn_id: turn.turn_id,
          agent_id: turn.agent_id,
          runtime_id: turn.runtime_id,
          error: error.slice(0, 500),
        }),
      )
    } catch {
      /* ignore */
    }
  }

  if (!turn.turn_id || !replyTarget) {
    log?.error?.('[genteam] turn.start missing turn_id or reply_target; ignoring')
    emitTurnError('malformed turn.start: missing turn_id or reply_target')
    return
  }

  if (!channelRuntime) {
    log?.warn?.('[genteam] channelRuntime unavailable; cannot run agent')
    try {
      await sendGenteamMessage(
        state,
        replyTarget,
        "Sorry, I'm unable to process your message right now (channelRuntime unavailable). Please try again later.",
        parentMessage,
      )
    } catch (e) {
      log?.error?.(`[genteam] channelRuntime-unavailable fallback send failed: ${e}`)
    }
    emitTurnError('channelRuntime unavailable')
    return
  }

  const agentId = turn.agent_id || state.auth.agentId || 'main'
  const sessionKey = `agent:${agentId}:genteam:${replyTarget}`

  // Register the turn so the de tools can resolve the reply-to-current target
  // and a turn.abort can cancel the run.
  const controller = new AbortController()
  const active: ActiveTurn = {
    turnId: turn.turn_id,
    replyTarget,
    parentMessage,
    abort: controller,
    sentCount: 0,
  }
  state.turns.set(turn.turn_id, active)
  // If a turn.abort raced ahead of this registration, honor it now (the WS
  // handler queued it because state.turns didn't have the turn yet).
  if (state.pendingAborts.delete(turn.turn_id)) {
    log?.info?.(`[genteam] applying queued turn.abort for ${turn.turn_id}`)
    controller.abort()
  }

  // Debug-only: notice an agent-identity / template-version change.
  if (turn.system_prompt_hash && turn.system_prompt_hash !== state.lastPromptHash) {
    state.lastPromptHash = turn.system_prompt_hash
    log?.debug?.(
      `[genteam] system prompt applied hash=${turn.system_prompt_hash} ` +
        `version=${turn.system_prompt_version ?? '?'}`,
    )
  }

  // Build the inbound MsgContext. `reply_target` is the explicit From/route —
  // the agent's replies fan back to the exact same GenTeam target.
  // GroupSystemPrompt carries the backend-rendered per-agent system prompt; the
  // OpenClaw prompt builder appends it to the agent's system prompt (cache-
  // static slot), additive over the operator's own prompt.
  const msgCtx: Record<string, any> = {
    Body: turn.envelope ?? '',
    From: replyTarget,
    To: accountId,
    Provider: 'genteam',
    Surface: 'genteam',
    OriginatingChannel: 'genteam',
    OriginatingTo: replyTarget,
    AccountId: accountId || 'default',
    SessionKey: sessionKey,
    ChatType: 'group',
    Timestamp: Date.now(),
    CommandAuthorized: true,
    ExplicitDeliverRoute: true,
  }
  if (turn.system_prompt_text) {
    msgCtx.GroupSystemPrompt = turn.system_prompt_text
  }

  const finalCtx = channelRuntime.reply.finalizeInboundContext(msgCtx)

  // Record inbound session metadata for tracking (best-effort).
  try {
    const storePath = channelRuntime.session.resolveStorePath(gatewayCtx.cfg?.session?.store, {
      agentId,
    })
    await channelRuntime.session.recordInboundSession({
      storePath,
      sessionKey,
      ctx: finalCtx,
      onRecordError: (err: any) => {
        log?.warn?.(`[genteam] recordInboundSession meta error: ${err}`)
      },
    })
  } catch (e) {
    log?.warn?.(`[genteam] recordInboundSession failed: ${e}`)
  }

  log?.info?.(
    `[genteam] dispatching turn ${turn.turn_id} agent=${agentId} ` +
      `target=${replyTarget} envelope_len=${(turn.envelope || '').length} ` +
      `system_prompt=${turn.system_prompt_text ? 'applied' : 'none'}`,
  )

  // Reply model = exact parity with the local/sandbox `de` runtimes (and with
  // the system prompt we apply): the agent's VISIBLE reply is the `de_message_send`
  // tool, NOT its assistant text. The shared runtime contract states the agent's
  // free text is runtime noise that is never shown — so the OpenClaw-native
  // reply blocks delivered here are intentionally NOT posted to GenTeam (doing
  // so would contradict the contract and double-post the tool reply). We still
  // run the dispatch (that is how the agent executes + calls the de_* tools);
  // `deliver` just observes.
  try {
    await channelRuntime.reply.dispatchReplyWithBufferedBlockDispatcher({
      ctx: finalCtx,
      cfg: gatewayCtx.cfg,
      replyOptions: {
        abortSignal: controller.signal,
        // The agent replies via the de_message_send tool; suppress core's own
        // default tool-progress text so nothing extra leaks toward the channel.
        suppressDefaultToolProgressMessages: true,
        // Honor the backend's per-turn lease as the run timeout.
        ...(turn.deadline_ms ? { timeoutOverrideSeconds: Math.ceil(turn.deadline_ms / 1000) } : {}),
      },
      dispatcherOptions: {
        deliver: async (_payload: any, info: any) => {
          // Native reply blocks are NOT posted — the visible reply went through
          // the de_message_send tool. Observe only (debug).
          log?.debug?.(`[genteam] ignoring native ${info?.kind ?? 'block'} (reply rides de_message_send)`)
        },
        onError: (err: any, info: any) => {
          log?.error?.(`[genteam] dispatch deliver error (${info?.kind}): ${err}`)
        },
      },
    })

    // The dispatch may RESOLVE normally even though a turn.abort fired (the run
    // finished just as Stop arrived, or the backend ignored the signal). Treat
    // an aborted turn as aborted regardless of how dispatch settled, so Stop
    // never finalizes as a misleading turn.done.
    if (controller.signal.aborted) {
      log?.info?.(`[genteam] turn ${turn.turn_id} aborted (dispatch resolved post-abort)`)
      emitTurnError('aborted')
      return
    }
    // Parity with local/sandbox: the agent's visible reply is de_message_send,
    // not its native text. If a turn completes without any send tool firing, no
    // message reached the channel — log it (the backend's no-send handling still
    // finalizes the turn, but the silent case must be observable, not invisible).
    if (active.sentCount === 0) {
      log?.warn?.(
        `[S] de_openclaw_turn_no_reply turn=${turn.turn_id} agent=${agentId} ` +
          `target=${replyTarget} — dispatch completed with no de_message_send`,
      )
    }
    log?.info?.(`[genteam] turn ${turn.turn_id} dispatch completed (sends=${active.sentCount})`)
    emitTurnDone()
  } catch (e) {
    if (controller.signal.aborted) {
      log?.info?.(`[genteam] turn ${turn.turn_id} aborted`)
      emitTurnError('aborted')
      return
    }
    log?.error?.(`[genteam] turn ${turn.turn_id} dispatch failed: ${e}`)
    // Best-effort error notification into the channel (don't mask the error).
    try {
      await sendGenteamMessage(state, replyTarget, '[error] Agent processing failed', parentMessage)
    } catch (fallbackErr) {
      log?.error?.(`[genteam] fallback error notification failed: ${fallbackErr}`)
    }
    emitTurnError(String(e))
  } finally {
    state.turns.delete(turn.turn_id)
  }
}

// ---------------------------------------------------------------------------
// Monitor — auth + WS connect loop with reconnect/backoff
// ---------------------------------------------------------------------------

async function runGenteamMonitor(cfg: GenteamAccountConfig, gatewayCtx: GatewayCtx): Promise<void> {
  const { abortSignal, log, setStatus } = gatewayCtx

  function reportConnected(): void {
    setStatus?.({ connected: true, lastEventAt: Date.now(), pluginVersion: PLUGIN_VERSION })
  }
  function reportDisconnected(): void {
    setStatus?.({ connected: false })
  }
  function reportEvent(): void {
    setStatus?.({ lastEventAt: Date.now() })
  }

  let reconnectAttempt = 0

  while (!abortSignal.aborted) {
    let connectedOnce = false
    try {
      // Fresh auth-exchange per (re)connect: ws_token is one-time (TTL 60) and
      // the agent_token may have rotated.
      const auth = await authExchange(cfg)
      const state: ConnectionState = {
        cfg,
        auth,
        log,
        turns: new Map(),
        pendingAborts: new Set(),
      }
      connectionsByAccount.set(cfg.accountId, state)

      log?.info?.(
        `[genteam] authenticated: agent=${auth.agentHandle || auth.agentId} ` +
          `server=${auth.serverId} ws_url=${auth.wsUrl}`,
      )

      const result = await connectGenteamWs({
        state,
        gatewayCtx,
        onConnected: () => {
          reconnectAttempt = 0
          reportConnected()
        },
        onEvent: reportEvent,
      })
      connectedOnce = result.connectedOnce
      reportDisconnected()
      // Drop the live binding for this account on disconnect so the de tools
      // fail closed (and a stale token is never reused) until the next connect.
      if (connectionsByAccount.get(cfg.accountId) === state) {
        connectionsByAccount.delete(cfg.accountId)
      }

      if (result.fatal) {
        log?.error?.('[genteam] fatal close — stopping monitor (re-create the connection in GenTeam)')
        break
      }
    } catch (e) {
      log?.error?.(`[genteam] monitor error: ${e}`)
      reportDisconnected()
    }

    if (abortSignal.aborted) break

    // Reset backoff if we had a healthy connection this round.
    if (connectedOnce) reconnectAttempt = 0
    const delay = getReconnectDelay(reconnectAttempt)
    log?.info?.(`[genteam] reconnecting in ${delay}ms (attempt ${reconnectAttempt + 1})`)
    await new Promise((r) => setTimeout(r, delay))
    reconnectAttempt++
  }

  connectionsByAccount.delete(cfg.accountId)
  reportDisconnected()
  log?.info?.('[genteam] monitor stopped')
}

// ---------------------------------------------------------------------------
// ChannelPlugin definition
// ---------------------------------------------------------------------------

const meta = {
  id: 'genteam' as const,
  label: 'GenTeam',
  selectionLabel: 'GenTeam',
  docsPath: '/channels/genteam',
  docsLabel: 'genteam',
  blurb: 'Connect this gateway to GenTeam as an Agent runtime.',
  order: 70,
}

// Outbound adapter — lets the agent proactively send a GenTeam message
// (`ctx.to` is a reply_target). Replies driven by a turn are delivered inside
// dispatchTurnToAgent; this path is for agent-initiated sends. Resolves the
// connection by account (defaults to the sole connection when unambiguous).
const outbound = {
  deliveryMode: 'direct' as const,
  async sendText(ctx: any) {
    const accountId = ctx?.accountId ?? ctx?.AccountId
    let state: ConnectionState | undefined
    if (accountId) {
      state = connectionsByAccount.get(accountId)
    } else if (connectionsByAccount.size === 1) {
      state = connectionsByAccount.values().next().value
    }
    if (!state) {
      return {
        channel: 'genteam',
        messageId: '',
        error: 'genteam channel not connected',
      }
    }
    try {
      await sendGenteamMessage(state, ctx.to, ctx.text, ctx.replyToId ?? undefined)
      return { channel: 'genteam', messageId: '' }
    } catch (e: any) {
      return { channel: 'genteam', messageId: '', error: String(e?.message ?? e) }
    }
  },
}

const genteamPlugin = {
  id: 'genteam' as const,
  meta,
  capabilities: {
    chatTypes: ['direct', 'group'] as const,
    polls: false,
    threads: true,
    media: true,
    reactions: true,
    edit: false,
    reply: true,
  },
  config: {
    listAccountIds: (cfg: any) => listAccountIds(cfg),
    resolveAccount: (cfg: any, accountId?: string | null) => resolveAccount(cfg, accountId),
  },
  gateway: {
    startAccount: async (ctx: GatewayCtx) => {
      const pluginCfg = resolveAccount(ctx.cfg, ctx.accountId)
      ctx.log?.info?.(`[genteam] startAccount: ${pluginCfg.accountId} endpoint=${pluginCfg.endpoint}`)
      await runGenteamMonitor(pluginCfg, {
        cfg: ctx.cfg,
        accountId: ctx.accountId,
        abortSignal: ctx.abortSignal,
        log: ctx.log,
        channelRuntime: ctx.channelRuntime,
        setStatus: ctx.setStatus,
      })
    },
  },
  outbound,
}

const plugin = {
  id: 'genteam',
  name: 'GenTeam',
  description: 'Connect a self-hosted OpenClaw gateway to GenTeam as an Agent runtime.',
  configSchema: { type: 'object' as const, properties: {} },
  register(api: any) {
    api.registerChannel({ plugin: genteamPlugin })
    // The de tool surface: model-callable GenTeam tools for the gateway agent.
    // Factory form — rebuilt per turn so each tool closes over the turn's
    // account/conversation. Non-optional → default-on once the plugin is
    // enabled (no operator `tools.allow` entry needed).
    api.registerTool(buildGenteamTools)
    api.logger.info(`[genteam] Plugin registered (${DE_TOOL_NAMES.length} de tools)`)
  },
}

export default plugin

// Named exports for unit tests (tests/plugin.test.ts). OpenClaw loads the
// default export; these expose the testable internals without a separate build.
export {
  buildGenteamTools,
  DE_TOOL_NAMES,
  DE_TOOL_DEFS,
  callAgentTool,
  sendGenteamMessage,
  dispatchTurnToAgent,
  connectionsByAccount,
  resolveAccount,
  listAccountIds,
  classifyClose,
  plugin,
}
export type { ConnectionState, ActiveTurn, TurnStartFrame, GenteamAccountConfig, AuthResult }
