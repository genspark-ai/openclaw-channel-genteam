// Unit tests for the GenTeam OpenClaw channel plugin.
//
// These exercise the plugin's wiring against fakes — no live gateway / LLM:
//   - register() wires both a channel AND the de tool surface.
//   - buildGenteamTools() is scoped to genteam turns and exposes the de_* verbs.
//   - tool execute() POSTs `{verb, ...}` with the per-agent bearer and
//     defaults de_message_send's target to the current turn's reply_target.
//   - dispatchTurnToAgent() applies the backend system prompt via
//     ctx.GroupSystemPrompt; the agent replies via the de_message_send
//     tool, so native reply blocks are NOT posted (parity with local/sandbox —
//     no auto-deliver), and the right completion frame is emitted.
//
// Run with `npm test` (node --import tsx --test).
import assert from 'node:assert/strict'
import { afterEach, beforeEach, test } from 'node:test'
import {
  existsSync,
  mkdtempSync,
  readFileSync,
  readdirSync,
  rmSync,
  symlinkSync,
  writeFileSync,
} from 'node:fs'
import { dirname, join } from 'node:path'
import { tmpdir } from 'node:os'

import {
  buildGenteamTools,
  DE_TOOL_NAMES,
  dispatchTurnToAgent,
  connectionsByAccount,
  plugin,
  type ConnectionState,
  type TurnStartFrame,
} from '../src/index.ts'

// ---------------------------------------------------------------------------
// Fakes
// ---------------------------------------------------------------------------

const NOOP_LOG = {
  info() {},
  warn() {},
  error() {},
  debug() {},
}

function fakeConnection(overrides: Partial<ConnectionState> = {}): ConnectionState {
  return {
    cfg: {
      accountId: 'default',
      endpoint: 'https://example.test',
      channelId: 'occ_test',
      appToken: 'oca_test',
      botToken: 'ocb_test',
      attachmentRoots: [process.cwd()],
    },
    auth: {
      wsToken: 'wt',
      wsUrl: 'wss://example.test/ws',
      wsTokenTtl: 60,
      agentToken: 'de-agent-stub-XYZ',
      agentId: 'agent-1',
      runtimeId: 'rt-1',
      serverId: 'srv-1',
      agentHandle: 'helper',
      agentDisplayName: 'Helper',
    },
    log: NOOP_LOG,
    turns: new Map(),
    pendingAborts: new Set(),
    ...overrides,
  }
}

interface CapturedFetch {
  url: string
  method: string
  headers: Record<string, string>
  body: any
}

// Install a fake global.fetch that records calls and returns a canned envelope.
function installFakeFetch(
  responder: (
    url: string,
    init: any,
  ) => {
    status?: number
    json?: any
    text?: string
    // Binary/streamed response support: when either is present the fake
    // returns a REAL `Response` so the code under test gets a working
    // `.headers.get()` + `.body` web stream (the attachment-download path).
    headers?: Record<string, string>
    bodyBytes?: Uint8Array
  },
): { calls: CapturedFetch[]; restore: () => void } {
  const calls: CapturedFetch[] = []
  const original = globalThis.fetch
  globalThis.fetch = (async (url: any, init: any) => {
    const headers = (init?.headers ?? {}) as Record<string, string>
    let parsed: any
    try {
      parsed = init?.body ? JSON.parse(init.body) : undefined
    } catch {
      parsed = init?.body
    }
    calls.push({ url: String(url), method: init?.method ?? 'GET', headers, body: parsed })
    const r = responder(String(url), init)
    if (r.bodyBytes !== undefined || r.headers !== undefined) {
      const status = r.status ?? 200
      const bytes = r.bodyBytes ?? new Uint8Array(0)
      return new Response(bytes as any, { status, headers: r.headers ?? {} }) as any
    }
    const text = r.text ?? JSON.stringify(r.json ?? { status: 0 })
    return {
      ok: (r.status ?? 200) < 400,
      status: r.status ?? 200,
      async text() {
        return text
      },
    } as any
  }) as any
  return { calls, restore: () => void (globalThis.fetch = original) }
}

afterEach(() => {
  connectionsByAccount.clear()
})

// ---------------------------------------------------------------------------
// register() — channel + tools
// ---------------------------------------------------------------------------

test('register wires both a channel and the de tool surface', () => {
  let channelRegistered = false
  let toolFactory: any = null
  const api = {
    registerChannel(arg: any) {
      channelRegistered = true
      assert.equal(arg.plugin.id, 'genteam')
    },
    registerTool(factory: any) {
      toolFactory = factory
    },
    logger: { info() {} },
  }
  plugin.register(api)
  assert.equal(channelRegistered, true)
  assert.equal(typeof toolFactory, 'function', 'registerTool must receive the factory')
  assert.equal(DE_TOOL_NAMES.length, 17)
})

// ---------------------------------------------------------------------------
// buildGenteamTools — scoping + names
// ---------------------------------------------------------------------------

test('buildGenteamTools exposes every declared de tool for a genteam turn', () => {
  const tools = buildGenteamTools({ messageChannel: 'genteam', agentAccountId: 'default' })
  const names = tools.map((t: any) => t.name).sort()
  assert.deepEqual(names, [...DE_TOOL_NAMES].sort())
  for (const t of tools) {
    assert.equal(typeof t.execute, 'function')
    assert.ok(t.parameters, 'every tool must carry a TypeBox parameter schema')
  }
})

test('buildGenteamTools returns nothing for a non-genteam channel turn', () => {
  assert.deepEqual(buildGenteamTools({ messageChannel: 'slack', agentAccountId: 'default' }), [])
})

test('buildGenteamTools returns nothing when messageChannel is missing/empty (no leak)', () => {
  // Regression: an absent or empty messageChannel must NOT register the
  // tools (exact-match guard).
  assert.deepEqual(buildGenteamTools({ agentAccountId: 'default' }), [])
  assert.deepEqual(buildGenteamTools({ messageChannel: '', agentAccountId: 'default' }), [])
  assert.deepEqual(buildGenteamTools({}), [])
  assert.deepEqual(buildGenteamTools(undefined), [])
})

// ---------------------------------------------------------------------------
// tool execute — verb in body + bearer + target default
// ---------------------------------------------------------------------------

test('a read tool POSTs verb + bearer to the right agent_tools endpoint', async () => {
  const state = fakeConnection()
  connectionsByAccount.set('default', state)
  const { calls, restore } = installFakeFetch(() => ({ json: { status: 0, messages: [] } }))
  try {
    const tools = buildGenteamTools({ messageChannel: 'genteam', agentAccountId: 'default' })
    const read = tools.find((t: any) => t.name === 'de_message_read')
    const res = await read.execute('call-1', { target: '#all', limit: 5 }, undefined)
    assert.equal(calls.length, 1)
    assert.equal(calls[0].url, 'https://example.test/api/digital-employee/agent_tools/message-read')
    assert.equal(calls[0].method, 'POST')
    assert.equal(calls[0].headers.Authorization, 'Bearer de-agent-stub-XYZ')
    // verb is ALWAYS present in the body.
    assert.equal(calls[0].body.verb, 'message-read')
    assert.equal(calls[0].body.target, '#all')
    assert.equal(calls[0].body.limit, 5)
    assert.equal(res.isError, undefined)
  } finally {
    restore()
  }
})

test('de_message_send (the reply tool) defaults target + forwards post_to_channel + counts the send', async () => {
  const state = fakeConnection()
  const turn = { turnId: 'turn-1', replyTarget: '#all', abort: new AbortController(), sentCount: 0 }
  state.turns.set('turn-1', turn)
  connectionsByAccount.set('default', state)
  const { calls, restore } = installFakeFetch(() => ({ json: { status: 0, comet_message_id: 'm1' } }))
  try {
    const tools = buildGenteamTools({ messageChannel: 'genteam', agentAccountId: 'default' })
    const send = tools.find((t: any) => t.name === 'de_message_send')
    // post_to_channel escape hatch is forwarded.
    await send.execute('call-2', { content: 'hello team', post_to_channel: true }, undefined)
    assert.equal(calls[0].url, 'https://example.test/api/digital-employee/agent_tools/message-send')
    assert.equal(calls[0].body.verb, 'message-send')
    assert.equal(calls[0].body.content, 'hello team')
    assert.equal(calls[0].body.target, '#all', 'target defaults to the current turn reply_target')
    assert.equal(calls[0].body.post_to_channel, true, 'post_to_channel forwarded')
    assert.equal(turn.sentCount, 1, 'a successful send is counted')
  } finally {
    restore()
  }
})

test('de_task_claim forwards message_id (create-and-claim from a message)', async () => {
  const state = fakeConnection()
  connectionsByAccount.set('default', state)
  const { calls, restore } = installFakeFetch(() => ({ json: { status: 0 } }))
  try {
    const tools = buildGenteamTools({ messageChannel: 'genteam', agentAccountId: 'default' })
    const claim = tools.find((t: any) => t.name === 'de_task_claim')
    await claim.execute('c', { message_id: 'm9', target: '#all' }, undefined)
    assert.equal(calls[0].body.verb, 'task-claim')
    assert.equal(calls[0].body.message_id, 'm9')
    assert.equal(calls[0].body.target, '#all')
  } finally {
    restore()
  }
})

test('de_attachment_view metadata_only:true uses the cheap JSON path (no byte download)', async () => {
  const state = fakeConnection()
  connectionsByAccount.set('default', state)
  const { calls, restore } = installFakeFetch(() => ({
    json: { ok: true, attachment: { attachment_ref: 'm1:0' }, metadata_only: true },
  }))
  try {
    const tools = buildGenteamTools({ messageChannel: 'genteam', agentAccountId: 'default' })
    const view = tools.find((t: any) => t.name === 'de_attachment_view')
    const out = await view.execute('c', { attachment_ref: 'm1:0', metadata_only: true }, undefined)
    assert.equal(calls[0].body.verb, 'attachment-view')
    assert.equal(calls[0].body.metadata_only, true)
    assert.equal(calls[0].body.attachment_ref, 'm1:0')
    // No file is written on the metadata path; the JSON envelope is returned.
    assert.ok(out.content[0].text.includes('"metadata_only":true'))
  } finally {
    restore()
  }
})

// Helper: a connection whose downloads land in a fresh temp dir we can inspect.
function downloadConnection(downloadDir?: string): ConnectionState {
  const base = fakeConnection()
  return fakeConnection({ cfg: { ...base.cfg, attachmentDownloadDir: downloadDir } })
}

test('de_attachment_view downloads bytes to a local file and returns local_path', async () => {
  const dir = mkdtempSync(join(tmpdir(), 'genteam-dl-'))
  const state = downloadConnection(dir)
  connectionsByAccount.set('default', state)
  const bytes = Buffer.from('the real image bytes  ÿ', 'binary')
  const { calls, restore } = installFakeFetch(() => ({
    status: 200,
    headers: {
      'x-de-attachment-filename': 'IMG_5410.jpg',
      'x-de-attachment-mime': 'image/jpeg',
      'x-de-attachment-size': String(bytes.length),
      'x-de-attachment-ref': '36091:0',
      'x-de-attachment-source': 'secure_media',
    },
    bodyBytes: new Uint8Array(bytes),
  }))
  try {
    const tools = buildGenteamTools({ messageChannel: 'genteam', agentAccountId: 'default' })
    const view = tools.find((t: any) => t.name === 'de_attachment_view')
    const out = await view.execute('c', { attachment_ref: '36091:0' }, undefined)
    // The backend is called in BINARY mode (metadata_only:false), not metadata.
    assert.equal(calls[0].body.verb, 'attachment-view')
    assert.equal(calls[0].body.metadata_only, false)
    const res = JSON.parse(out.content[0].text)
    assert.equal(res.ok, true)
    assert.equal(res.filename, 'IMG_5410.jpg')
    assert.equal(res.mime_type, 'image/jpeg')
    assert.equal(res.size, bytes.length)
    // `source` is a nested object for parity with the sandbox/local CLI shape.
    assert.equal(res.source.attachment_ref, '36091:0')
    assert.equal(res.source.source, 'secure_media')
    assert.match(res.sha256, /^[0-9a-f]{64}$/)
    // The file lands inside the configured download dir and holds the bytes.
    assert.equal(dirname(res.local_path), dir)
    assert.ok(existsSync(res.local_path))
    assert.deepEqual(readFileSync(res.local_path), bytes)
  } finally {
    restore()
    rmSync(dir, { recursive: true, force: true })
  }
})

test('de_attachment_view sanitizes a hostile filename (no path traversal escape)', async () => {
  const dir = mkdtempSync(join(tmpdir(), 'genteam-dl-'))
  const state = downloadConnection(dir)
  connectionsByAccount.set('default', state)
  const bytes = Buffer.from('payload')
  const { restore } = installFakeFetch(() => ({
    status: 200,
    headers: {
      // Hostile: path separators + traversal must NOT escape the download dir.
      'x-de-attachment-filename': '../../../../etc/passwd',
      'x-de-attachment-mime': 'text/plain',
      'x-de-attachment-ref': '36091:0',
    },
    bodyBytes: new Uint8Array(bytes),
  }))
  try {
    const tools = buildGenteamTools({ messageChannel: 'genteam', agentAccountId: 'default' })
    const view = tools.find((t: any) => t.name === 'de_attachment_view')
    const out = await view.execute('c', { attachment_ref: '36091:0' }, undefined)
    const res = JSON.parse(out.content[0].text)
    assert.equal(res.ok, true)
    // Written strictly inside the download dir; no traversal, no path parts.
    assert.equal(dirname(res.local_path), dir)
    assert.ok(!res.local_path.includes('..'))
    assert.ok(!res.local_path.includes('/etc/passwd'))
    assert.ok(existsSync(res.local_path))
  } finally {
    restore()
    rmSync(dir, { recursive: true, force: true })
  }
})

test('de_attachment_view rejects an oversized attachment by declared size (no write)', async () => {
  const dir = mkdtempSync(join(tmpdir(), 'genteam-dl-'))
  const state = downloadConnection(dir)
  connectionsByAccount.set('default', state)
  const { restore } = installFakeFetch(() => ({
    status: 200,
    headers: {
      'x-de-attachment-filename': 'huge.bin',
      // 1 TiB — far above the 1 GiB cap regardless of its exact value.
      'x-de-attachment-size': String(1024 ** 4),
      'x-de-attachment-ref': '36091:0',
    },
    bodyBytes: new Uint8Array([1, 2, 3]),
  }))
  try {
    const tools = buildGenteamTools({ messageChannel: 'genteam', agentAccountId: 'default' })
    const view = tools.find((t: any) => t.name === 'de_attachment_view')
    const out = await view.execute('c', { attachment_ref: '36091:0' }, undefined)
    assert.equal(out.isError, true)
    assert.match(out.content[0].text, /too large/i)
    // Nothing was materialized — the dir stays empty (size check precedes write).
    assert.deepEqual(readdirSync(dir), [])
  } finally {
    restore()
    rmSync(dir, { recursive: true, force: true })
  }
})

test('de_attachment_view defaults the download dir to the system temp dir when unconfigured', async () => {
  const state = downloadConnection(undefined)
  connectionsByAccount.set('default', state)
  const bytes = Buffer.from('x')
  const { restore } = installFakeFetch(() => ({
    status: 200,
    headers: { 'x-de-attachment-filename': 'a.txt', 'x-de-attachment-ref': 'm7:1' },
    bodyBytes: new Uint8Array(bytes),
  }))
  try {
    const tools = buildGenteamTools({ messageChannel: 'genteam', agentAccountId: 'default' })
    const view = tools.find((t: any) => t.name === 'de_attachment_view')
    const out = await view.execute('c', { attachment_ref: 'm7:1' }, undefined)
    const res = JSON.parse(out.content[0].text)
    assert.equal(res.ok, true)
    // Default dir is a private per-process mkdtemp dir under the system temp dir.
    assert.ok(dirname(res.local_path).startsWith(join(tmpdir(), 'genteam-attachments-')))
    rmSync(res.local_path, { force: true })
  } finally {
    restore()
  }
})

test('de_attachment_view requires an attachment_ref or message_id', async () => {
  const state = downloadConnection()
  connectionsByAccount.set('default', state)
  const { calls, restore } = installFakeFetch(() => ({ json: { status: 0 } }))
  try {
    const tools = buildGenteamTools({ messageChannel: 'genteam', agentAccountId: 'default' })
    const view = tools.find((t: any) => t.name === 'de_attachment_view')
    const out = await view.execute('c', {}, undefined)
    assert.equal(out.isError, true)
    assert.equal(calls.length, 0, 'no backend call without an identifier')
  } finally {
    restore()
  }
})

// Acceptance criterion #5: a backend 403/404 (non-member / wrong agent / bad
// ref) must surface as a tool error and write NOTHING to disk — the plugin must
// not let a forbidden download succeed.
test('de_attachment_view surfaces a backend 403 and writes no file', async () => {
  const dir = mkdtempSync(join(tmpdir(), 'genteam-dl-'))
  const state = downloadConnection(dir)
  connectionsByAccount.set('default', state)
  const { restore } = installFakeFetch(() => ({
    status: 403,
    headers: { 'content-type': 'application/json' },
    bodyBytes: new TextEncoder().encode(JSON.stringify({ status: 1, error: 'not_a_member' })),
  }))
  try {
    const tools = buildGenteamTools({ messageChannel: 'genteam', agentAccountId: 'default' })
    const view = tools.find((t: any) => t.name === 'de_attachment_view')
    const out = await view.execute('c', { attachment_ref: 'm1:0' }, undefined)
    assert.equal(out.isError, true)
    assert.match(out.content[0].text, /HTTP 403/)
    assert.match(out.content[0].text, /not_a_member/)
    assert.deepEqual(readdirSync(dir), [], 'a forbidden download must not materialize a file')
  } finally {
    restore()
    rmSync(dir, { recursive: true, force: true })
  }
})

// Acceptance criterion #6: the mid-stream cap is the REAL enforcement (the
// declared-size header is advisory). Drive a body past a low per-account cap
// with NO size header so the limiter Transform trips, and assert the partial is
// cleaned up.
test('de_attachment_view trips the mid-stream cap and removes the partial file', async () => {
  const dir = mkdtempSync(join(tmpdir(), 'genteam-dl-'))
  const base = downloadConnection(dir)
  const state = fakeConnection({ cfg: { ...base.cfg, attachmentDownloadMaxBytes: 4 } })
  connectionsByAccount.set('default', state)
  const { restore } = installFakeFetch(() => ({
    status: 200,
    // No x-de-attachment-size header → the up-front check can't fire; only the
    // mid-stream limiter can catch this.
    headers: { 'x-de-attachment-filename': 'big.bin', 'x-de-attachment-ref': 'm1:0' },
    bodyBytes: new Uint8Array(Buffer.alloc(64, 1)),
  }))
  try {
    const tools = buildGenteamTools({ messageChannel: 'genteam', agentAccountId: 'default' })
    const view = tools.find((t: any) => t.name === 'de_attachment_view')
    const out = await view.execute('c', { attachment_ref: 'm1:0' }, undefined)
    assert.equal(out.isError, true)
    assert.match(out.content[0].text, /exceeds the maximum download size/)
    assert.deepEqual(readdirSync(dir), [], 'the partial file must be unlinked on overflow')
  } finally {
    restore()
    rmSync(dir, { recursive: true, force: true })
  }
})

// A turn.abort mid-download must surface as an error and leave no partial file.
test('de_attachment_view cleans up and errors when the turn is aborted', async () => {
  const dir = mkdtempSync(join(tmpdir(), 'genteam-dl-'))
  const state = downloadConnection(dir)
  connectionsByAccount.set('default', state)
  const aborted = new AbortController()
  aborted.abort()
  const { restore } = installFakeFetch((_url, init) => {
    // Mirror real fetch: reject when handed an already-aborted signal.
    if (init?.signal?.aborted) throw new Error('The operation was aborted')
    return { status: 200, headers: { 'x-de-attachment-ref': 'm1:0' }, bodyBytes: new Uint8Array([1]) }
  })
  try {
    const tools = buildGenteamTools({ messageChannel: 'genteam', agentAccountId: 'default' })
    const view = tools.find((t: any) => t.name === 'de_attachment_view')
    const out = await view.execute('c', { attachment_ref: 'm1:0' }, aborted.signal)
    assert.equal(out.isError, true)
    assert.deepEqual(readdirSync(dir), [], 'no partial file after an aborted download')
  } finally {
    restore()
    rmSync(dir, { recursive: true, force: true })
  }
})

// Acceptance criterion #4: the materialized result must never carry a secure
// media URL, SAS/blob token, or cookie — even if the backend response grew
// extra headers. The tool only echoes the documented X-DE-Attachment-* fields.
test('de_attachment_view never leaks secret-shaped headers into the result', async () => {
  const dir = mkdtempSync(join(tmpdir(), 'genteam-dl-'))
  const state = downloadConnection(dir)
  connectionsByAccount.set('default', state)
  const { restore } = installFakeFetch(() => ({
    status: 200,
    headers: {
      'x-de-attachment-filename': 'a.txt',
      'x-de-attachment-ref': 'm1:0',
      // Decoy secrets the tool must ignore.
      'x-de-secure-media-url': 'https://acct.blob.core.windows.net/c/x?sig=SECRETSAS',
      'x-ms-signature': 'SECRETSAS',
      'set-cookie': 'session_id=SECRETCOOKIE',
    },
    bodyBytes: new Uint8Array([120]),
  }))
  try {
    const tools = buildGenteamTools({ messageChannel: 'genteam', agentAccountId: 'default' })
    const view = tools.find((t: any) => t.name === 'de_attachment_view')
    const out = await view.execute('c', { attachment_ref: 'm1:0' }, undefined)
    const text = out.content[0].text
    assert.ok(!text.includes('blob.core.windows.net'))
    assert.ok(!text.includes('SECRETSAS'))
    assert.ok(!text.includes('session_id'))
    // Only the documented MaterializedAttachment keys are present.
    const res = JSON.parse(text)
    assert.deepEqual(
      Object.keys(res).sort(),
      ['filename', 'local_path', 'mime_type', 'ok', 'sha256', 'size', 'source'],
    )
    rmSync(res.local_path, { force: true })
  } finally {
    restore()
    rmSync(dir, { recursive: true, force: true })
  }
})

// A symlink pre-planted at the (configured) download dir must be refused, not
// followed — defends the materialization path the way the upload path already
// defends its roots.
test('de_attachment_view refuses a symlinked download dir', async () => {
  const realParent = mkdtempSync(join(tmpdir(), 'genteam-dl-real-'))
  const linkParent = mkdtempSync(join(tmpdir(), 'genteam-dl-link-'))
  const linkDir = join(linkParent, 'dl')
  symlinkSync(realParent, linkDir)
  const state = downloadConnection(linkDir)
  connectionsByAccount.set('default', state)
  const { restore } = installFakeFetch(() => ({
    status: 200,
    headers: { 'x-de-attachment-filename': 'a.txt', 'x-de-attachment-ref': 'm1:0' },
    bodyBytes: new Uint8Array([1, 2, 3]),
  }))
  try {
    const tools = buildGenteamTools({ messageChannel: 'genteam', agentAccountId: 'default' })
    const view = tools.find((t: any) => t.name === 'de_attachment_view')
    const out = await view.execute('c', { attachment_ref: 'm1:0' }, undefined)
    assert.equal(out.isError, true)
    assert.match(out.content[0].text, /symlink/)
    assert.deepEqual(readdirSync(realParent), [], 'nothing written through the symlink')
  } finally {
    restore()
    rmSync(linkParent, { recursive: true, force: true })
    rmSync(realParent, { recursive: true, force: true })
  }
})

test('message-send-attachment only reads files under configured attachment roots', async () => {
  const root = mkdtempSync(join(tmpdir(), 'genteam-attach-root-'))
  const outside = mkdtempSync(join(tmpdir(), 'genteam-attach-outside-'))
  const allowedPath = join(root, 'ok.txt')
  const blockedPath = join(outside, 'secret.txt')
  const symlinkPath = join(root, 'secret-link.txt')
  writeFileSync(allowedPath, 'hello')
  writeFileSync(blockedPath, 'secret')
  symlinkSync(blockedPath, symlinkPath)

  const state = fakeConnection({
    cfg: {
      ...fakeConnection().cfg,
      attachmentRoots: [root],
    },
  })
  const turn = { turnId: 'turn-1', replyTarget: '#all', abort: new AbortController(), sentCount: 0 }
  state.turns.set('turn-1', turn)
  connectionsByAccount.set('default', state)
  const { calls, restore } = installFakeFetch((url: string) => {
    // Route the SHARED session+SAS flow: init mints a SAS, the direct PUT
    // lands the bytes, finalize sends the message.
    if (url.endsWith('/attachment-session-init')) {
      return {
        json: {
          ok: true,
          upload_url: 'https://blob.test/sas',
          upload_session: 'sess',
          content_type: 'text/plain',
        },
      }
    }
    if (url.endsWith('/attachment-session-finalize')) {
      return { json: { ok: true, comet_message_id: 'm1', attachments: [{ attachment_ref: 'm1:0' }] } }
    }
    return { status: 201, text: '' } // direct SAS PUT
  })
  try {
    const tools = buildGenteamTools({ messageChannel: 'genteam', agentAccountId: 'default' })
    const sendAttachment = tools.find((t: any) => t.name === 'de_message_send_attachment')

    const blocked = await sendAttachment.execute('c', { paths: [blockedPath] }, undefined)
    assert.equal(blocked.isError, true)
    assert.match(blocked.content[0].text, /outside the allowed roots/)
    assert.equal(calls.length, 0, 'blocked paths are rejected before any upload request')

    const symlinked = await sendAttachment.execute('c', { paths: [symlinkPath] }, undefined)
    assert.equal(symlinked.isError, true)
    assert.match(symlinked.content[0].text, /outside the allowed roots/)
    assert.equal(calls.length, 0, 'symlink escapes are rejected before any upload request')

    // Allowed path → the SHARED session+SAS flow (init → direct PUT →
    // finalize), NOT the legacy 99 MiB multipart endpoint.
    const allowed = await sendAttachment.execute('c', { paths: [allowedPath] }, undefined)
    assert.equal(allowed.isError, undefined)
    assert.ok(
      calls.some((c) => c.url.endsWith('/agent_tools/attachment-session-init')),
      'session-init must be called',
    )
    assert.ok(
      calls.some((c) => c.url === 'https://blob.test/sas' && c.method === 'PUT'),
      'the body must PUT direct to the SAS URL',
    )
    assert.ok(
      calls.some((c) => c.url.endsWith('/agent_tools/attachment-session-finalize')),
      'session-finalize must be called',
    )
    assert.ok(
      !calls.some((c) => c.url.endsWith('/message-send-attachment')),
      'the legacy multipart endpoint must not be used',
    )
    // The PUT must declare the Azure single-PutBlob headers with a real
    // Content-Length (== the 5 bytes of 'hello') and the backend-resolved type —
    // not chunked, not a wrong/zero length.
    const put = calls.find((c) => c.method === 'PUT' && c.url === 'https://blob.test/sas')
    assert.ok(put, 'a direct SAS PUT must have been issued')
    assert.equal(put!.headers['x-ms-blob-type'], 'BlockBlob')
    assert.equal(put!.headers['Content-Type'], 'text/plain')
    assert.equal(put!.headers['Content-Length'], '5')
    assert.equal(turn.sentCount, 1)
  } finally {
    restore()
    connectionsByAccount.clear()
    rmSync(root, { recursive: true, force: true })
    rmSync(outside, { recursive: true, force: true })
  }
})

test('de_message_send_attachment sends MULTIPLE files as ONE batch via the shared session flow', async () => {
  const root = mkdtempSync(join(tmpdir(), 'genteam-attach-multi-'))
  const a = join(root, 'a.txt')
  const b = join(root, 'b.txt')
  const c = join(root, 'c.txt')
  writeFileSync(a, 'AAA')
  writeFileSync(b, 'BBB')
  writeFileSync(c, 'CCC')

  const state = fakeConnection({
    cfg: { ...fakeConnection().cfg, attachmentRoots: [root] },
  })
  const turn = { turnId: 'turn-1', replyTarget: '#all', abort: new AbortController(), sentCount: 0 }
  state.turns.set('turn-1', turn)
  connectionsByAccount.set('default', state)
  let initCount = 0
  let finalizeBody: any = null
  const { calls, restore } = installFakeFetch((url: string, init: any) => {
    if (url.endsWith('/attachment-session-init')) {
      initCount += 1
      return {
        json: {
          ok: true,
          upload_url: `https://blob.test/sas-${initCount}`,
          upload_session: `sess-${initCount}`,
          content_type: 'text/plain',
        },
      }
    }
    if (url.endsWith('/attachment-session-finalize')) {
      try {
        finalizeBody = JSON.parse(init.body)
      } catch {
        /* ignore */
      }
      return { json: { ok: true, comet_message_id: 'm1', attachments: [{ attachment_ref: 'm1:0' }] } }
    }
    return { status: 201, text: '' }
  })
  try {
    const tools = buildGenteamTools({ messageChannel: 'genteam', agentAccountId: 'default' })
    const sendAttachment = tools.find((t: any) => t.name === 'de_message_send_attachment')
    const res = await sendAttachment.execute('c', { paths: [a, b, c], content: 'three files' }, undefined)
    assert.equal(res.isError, undefined)
    // 3 inits + 3 direct PUTs + exactly 1 finalize (NOT 3 messages).
    assert.equal(initCount, 3)
    const puts = calls.filter((c) => c.method === 'PUT')
    assert.equal(puts.length, 3)
    // Every PUT declares the BlockBlob headers with each file's real 3-byte
    // length (no chunked transfer, no shared/zero length).
    for (const p of puts) {
      assert.equal(p.headers['x-ms-blob-type'], 'BlockBlob')
      assert.equal(p.headers['Content-Length'], '3')
    }
    assert.equal(calls.filter((c) => c.url.endsWith('/attachment-session-finalize')).length, 1)
    assert.ok(finalizeBody && Array.isArray(finalizeBody.files) && finalizeBody.files.length === 3)
    assert.deepEqual(
      finalizeBody.files.map((f: any) => f.upload_session),
      ['sess-1', 'sess-2', 'sess-3'],
    )
    assert.equal(finalizeBody.text, 'three files')
    assert.equal(turn.sentCount, 1)
  } finally {
    restore()
    connectionsByAccount.clear()
    rmSync(root, { recursive: true, force: true })
  }
})

test('de_message_send_attachment surfaces a backend finalize rejection as an error tool result', async () => {
  const root = mkdtempSync(join(tmpdir(), 'genteam-attach-rej-'))
  const f = join(root, 'f.txt')
  writeFileSync(f, 'hi')
  const state = fakeConnection({ cfg: { ...fakeConnection().cfg, attachmentRoots: [root] } })
  const turn = { turnId: 'turn-1', replyTarget: '#all', abort: new AbortController(), sentCount: 0 }
  state.turns.set('turn-1', turn)
  connectionsByAccount.set('default', state)
  const { restore } = installFakeFetch((url: string) => {
    if (url.endsWith('/attachment-session-init')) {
      return {
        json: { ok: true, upload_url: 'https://blob.test/sas', upload_session: 'sess', content_type: 'text/plain' },
      }
    }
    if (url.endsWith('/attachment-session-finalize')) {
      // Backend rejects finalize (e.g. replay / membership).
      return { status: 409, json: { ok: false, code: 'UPLOAD_SESSION_ALREADY_CONSUMED' } }
    }
    return { status: 201, text: '' }
  })
  try {
    const tools = buildGenteamTools({ messageChannel: 'genteam', agentAccountId: 'default' })
    const sendAttachment = tools.find((t: any) => t.name === 'de_message_send_attachment')
    const res = await sendAttachment.execute('c', { paths: [f] }, undefined)
    // Rendered through the shared `error (HTTP <status>): <detail>` formatter,
    // carrying the backend's typed envelope.
    assert.equal(res.isError, true)
    assert.match(res.content[0].text, /error \(HTTP 409\)/)
    assert.match(res.content[0].text, /UPLOAD_SESSION_ALREADY_CONSUMED/)
    // A failed send must NOT count toward the turn's sent total.
    assert.equal(turn.sentCount, 0)
  } finally {
    restore()
    connectionsByAccount.clear()
    rmSync(root, { recursive: true, force: true })
  }
})

test('de_message_send_attachment surfaces a 200-but-ok:false finalize as an error result', async () => {
  // A finalize that returns HTTP 200 with `ok:false` is a LOGICAL backend
  // failure (distinct from a 4xx); it must still render as an error tool result
  // carrying the server body, and must NOT count as a send.
  const root = mkdtempSync(join(tmpdir(), 'genteam-attach-okfalse-'))
  const f = join(root, 'f.txt')
  writeFileSync(f, 'hi')
  const state = fakeConnection({ cfg: { ...fakeConnection().cfg, attachmentRoots: [root] } })
  const turn = { turnId: 'turn-1', replyTarget: '#all', abort: new AbortController(), sentCount: 0 }
  state.turns.set('turn-1', turn)
  connectionsByAccount.set('default', state)
  const { restore } = installFakeFetch((url: string) => {
    if (url.endsWith('/attachment-session-init')) {
      return {
        json: { ok: true, upload_url: 'https://blob.test/sas', upload_session: 'sess', content_type: 'text/plain' },
      }
    }
    if (url.endsWith('/attachment-session-finalize')) {
      return { status: 200, json: { ok: false, code: 'FINALIZE_LOGICAL_FAIL' } }
    }
    return { status: 201, text: '' }
  })
  try {
    const tools = buildGenteamTools({ messageChannel: 'genteam', agentAccountId: 'default' })
    const sendAttachment = tools.find((t: any) => t.name === 'de_message_send_attachment')
    const res = await sendAttachment.execute('c', { paths: [f] }, undefined)
    assert.equal(res.isError, true)
    assert.match(res.content[0].text, /error \(HTTP 200\)/)
    assert.match(res.content[0].text, /FINALIZE_LOGICAL_FAIL/)
    assert.equal(turn.sentCount, 0)
  } finally {
    restore()
    connectionsByAccount.clear()
    rmSync(root, { recursive: true, force: true })
  }
})

test('de_message_send_attachment renders CLIENT-SIDE failures as plain `error:` results (no HTTP prefix), uncounted', async () => {
  // The shared core distinguishes a backend HTTP rejection (echoed as
  // `error (HTTP <status>)`) from a client-side failure (network / malformed
  // init / validation), which the plugin surfaces as `error: <message>` with NO
  // `(HTTP …)` prefix and no server body. Only the backend branch was tested;
  // this pins the client-side branch (index.ts L835) the plugin relies on.
  const root = mkdtempSync(join(tmpdir(), 'genteam-attach-client-'))
  const f = join(root, 'f.txt')
  writeFileSync(f, 'hi')
  const state = fakeConnection({ cfg: { ...fakeConnection().cfg, attachmentRoots: [root] } })
  const turn = { turnId: 'turn-1', replyTarget: '#all', abort: new AbortController(), sentCount: 0 }
  state.turns.set('turn-1', turn)
  connectionsByAccount.set('default', state)
  const tools = buildGenteamTools({ messageChannel: 'genteam', agentAccountId: 'default' })
  const sendAttachment = tools.find((t: any) => t.name === 'de_message_send_attachment')
  try {
    // (a) transport failure on session-init → DE_NETWORK_ERROR
    {
      const { calls, restore } = installFakeFetch((url: string) => {
        if (url.endsWith('/attachment-session-init')) throw new Error('connect ECONNREFUSED')
        return { status: 201, text: '' }
      })
      try {
        const res = await sendAttachment.execute('c', { paths: [f] }, undefined)
        assert.equal(res.isError, true)
        assert.match(res.content[0].text, /^error: /)
        assert.doesNotMatch(res.content[0].text, /\(HTTP/)
        assert.match(res.content[0].text, /network failure/)
        assert.ok(!calls.some((c) => c.url.endsWith('/attachment-session-finalize')), 'finalize must not run')
      } finally {
        restore()
      }
    }
    // (b) 2xx init missing upload_url → DE_SESSION_INIT_MALFORMED (typed, not echoed)
    {
      const { restore } = installFakeFetch((url: string) =>
        url.endsWith('/attachment-session-init') ? { json: { ok: true } } : { status: 201, text: '' },
      )
      try {
        const res = await sendAttachment.execute('c', { paths: [f] }, undefined)
        assert.equal(res.isError, true)
        assert.match(res.content[0].text, /^error: /)
        assert.doesNotMatch(res.content[0].text, /\(HTTP/)
        assert.match(res.content[0].text, /upload_url|upload_session|not JSON/)
      } finally {
        restore()
      }
    }
    // (c) >10 files → DE_TOO_MANY_ATTACHMENTS, rejected before ANY request
    {
      const many: string[] = []
      for (let i = 0; i < 11; i++) {
        const p = join(root, `m${i}.txt`)
        writeFileSync(p, 'x')
        many.push(p)
      }
      const { calls, restore } = installFakeFetch(() => ({ status: 201, text: '' }))
      try {
        const res = await sendAttachment.execute('c', { paths: many }, undefined)
        assert.equal(res.isError, true)
        assert.match(res.content[0].text, /too many attachments/)
        assert.doesNotMatch(res.content[0].text, /\(HTTP/)
        assert.equal(calls.length, 0, 'the count cap is enforced before any upload request')
      } finally {
        restore()
      }
    }
    assert.equal(turn.sentCount, 0, 'no client-side failure counts as a send')
  } finally {
    connectionsByAccount.clear()
    rmSync(root, { recursive: true, force: true })
  }
})

test('de_message_send_attachment threads the caller turn-abort signal into the upload request', async () => {
  // The combineSignals change must keep the caller's turn-abort signal wired
  // into the request (not XOR'd away by the per-request timeout). Drive it with
  // a pre-aborted signal and a fetch that rejects when it sees an aborted
  // signal: if the signal did NOT propagate, the request would proceed and
  // finalize would run. (Every other attachment test passes `undefined`.)
  const root = mkdtempSync(join(tmpdir(), 'genteam-attach-signal-'))
  const f = join(root, 'f.txt')
  writeFileSync(f, 'hi')
  const state = fakeConnection({ cfg: { ...fakeConnection().cfg, attachmentRoots: [root] } })
  const turn = { turnId: 'turn-1', replyTarget: '#all', abort: new AbortController(), sentCount: 0 }
  state.turns.set('turn-1', turn)
  connectionsByAccount.set('default', state)
  const { calls, restore } = installFakeFetch((_url: string, init: any) => {
    // Faithful stand-in for fetch honouring an aborted signal.
    if (init?.signal?.aborted) throw new Error('The operation was aborted')
    return { status: 201, text: '' }
  })
  try {
    const aborted = new AbortController()
    aborted.abort()
    const tools = buildGenteamTools({ messageChannel: 'genteam', agentAccountId: 'default' })
    const sendAttachment = tools.find((t: any) => t.name === 'de_message_send_attachment')
    const res = await sendAttachment.execute('c', { paths: [f] }, aborted.signal)
    assert.equal(res.isError, true, 'a pre-aborted caller signal must abort the upload')
    assert.match(res.content[0].text, /^error: /)
    assert.ok(
      !calls.some((c) => c.url.endsWith('/attachment-session-finalize')),
      'finalize must not run once the caller signal has aborted the request',
    )
    assert.equal(turn.sentCount, 0)
  } finally {
    restore()
    connectionsByAccount.clear()
    rmSync(root, { recursive: true, force: true })
  }
})

test('message-send-attachment is disabled when no attachment roots are configured', async () => {
  const root = mkdtempSync(join(tmpdir(), 'genteam-attach-disabled-'))
  const path = join(root, 'file.txt')
  writeFileSync(path, 'hello')

  const state = fakeConnection({
    cfg: {
      ...fakeConnection().cfg,
      attachmentRoots: [],
    },
  })
  const turn = { turnId: 'turn-1', replyTarget: '#all', abort: new AbortController(), sentCount: 0 }
  state.turns.set('turn-1', turn)
  connectionsByAccount.set('default', state)
  const { calls, restore } = installFakeFetch(() => ({ json: { status: 0 } }))
  try {
    const tools = buildGenteamTools({ messageChannel: 'genteam', agentAccountId: 'default' })
    const sendAttachment = tools.find((t: any) => t.name === 'de_message_send_attachment')
    const res = await sendAttachment.execute('c', { paths: [path] }, undefined)
    assert.equal(res.isError, true)
    assert.match(res.content[0].text, /none configured/)
    assert.equal(calls.length, 0)
  } finally {
    restore()
    connectionsByAccount.clear()
    rmSync(root, { recursive: true, force: true })
  }
})

test('a tool fails closed when the account has no live connection', async () => {
  // connectionsByAccount intentionally empty.
  const tools = buildGenteamTools({ messageChannel: 'genteam', agentAccountId: 'default' })
  const info = tools.find((t: any) => t.name === 'de_server_info')
  const res = await info.execute('c', {}, undefined)
  assert.equal(res.isError, true)
  assert.match(res.content[0].text, /not active/)
})

test('a backend rejection surfaces as an error tool result', async () => {
  const state = fakeConnection()
  connectionsByAccount.set('default', state)
  const { restore } = installFakeFetch(() => ({ status: 403, json: { status: 1, error: 'tool_forbidden' } }))
  try {
    const tools = buildGenteamTools({ messageChannel: 'genteam', agentAccountId: 'default' })
    const members = tools.find((t: any) => t.name === 'de_channel_members')
    const res = await members.execute('c', { target: '#all' }, undefined)
    assert.equal(res.isError, true)
    assert.match(res.content[0].text, /tool_forbidden/)
  } finally {
    restore()
  }
})

// ---------------------------------------------------------------------------
// dispatchTurnToAgent — GroupSystemPrompt + reply model + ack
// ---------------------------------------------------------------------------

// A fake channelRuntime that captures the finalized ctx and drives the deliver
// callback with the supplied reply blocks.
function fakeChannelRuntime(opts: { deliverBlocks?: any[]; capture: { ctx?: any } }) {
  return {
    reply: {
      finalizeInboundContext(ctx: any) {
        opts.capture.ctx = ctx
        return ctx
      },
      async dispatchReplyWithBufferedBlockDispatcher(params: any) {
        opts.capture.ctx = params.ctx
        for (const payload of opts.deliverBlocks ?? []) {
          await params.dispatcherOptions.deliver(payload, { kind: 'final' })
        }
        return {}
      },
    },
    session: {
      resolveStorePath() {
        return '/tmp/store'
      },
      async recordInboundSession() {},
    },
  }
}

function fakeWs(sentFrames: any[]) {
  return {
    readyState: 1, // OPEN
    send(data: string) {
      sentFrames.push(JSON.parse(data))
    },
  } as any
}

function baseFrame(overrides: Partial<TurnStartFrame> = {}): TurnStartFrame {
  return {
    type: 'turn.start',
    turn_id: 'turn-1',
    agent_id: 'agent-1',
    runtime_id: 'rt-1',
    envelope: '[target=#all] @alice: hello',
    reply_target: '#all',
    ...overrides,
  }
}

test('dispatchTurnToAgent applies system_prompt_text via ctx.GroupSystemPrompt', async () => {
  const state = fakeConnection()
  connectionsByAccount.set('default', state)
  const capture: { ctx?: any } = {}
  const channelRuntime = fakeChannelRuntime({ deliverBlocks: [], capture })
  const sent: any[] = []
  const { restore } = installFakeFetch(() => ({ json: { status: 0 } }))
  try {
    await dispatchTurnToAgent(
      state,
      { cfg: {}, accountId: 'default', abortSignal: new AbortController().signal, log: NOOP_LOG, channelRuntime } as any,
      baseFrame({ system_prompt_text: 'YOU ARE HELPER. Reply via tools.', system_prompt_hash: 'h1', system_prompt_version: 3 }),
      fakeWs(sent),
    )
    assert.equal(capture.ctx.GroupSystemPrompt, 'YOU ARE HELPER. Reply via tools.', 'system_prompt_text → ctx.GroupSystemPrompt')
    assert.equal(capture.ctx.Provider, 'genteam')
    assert.equal(capture.ctx.ChatType, 'group')
  } finally {
    restore()
  }
})

test('no GroupSystemPrompt key when the frame carries no system_prompt_text', async () => {
  const state = fakeConnection()
  connectionsByAccount.set('default', state)
  const capture: { ctx?: any } = {}
  const channelRuntime = fakeChannelRuntime({ deliverBlocks: [], capture })
  const { restore } = installFakeFetch(() => ({ json: { status: 0 } }))
  try {
    await dispatchTurnToAgent(
      state,
      { cfg: {}, accountId: 'default', abortSignal: new AbortController().signal, log: NOOP_LOG, channelRuntime } as any,
      baseFrame(),
      fakeWs([]),
    )
    assert.equal('GroupSystemPrompt' in capture.ctx, false)
  } finally {
    restore()
  }
})

test('native reply blocks are NOT auto-posted; the agent replies via de_message_send', async () => {
  // Parity with local/sandbox: the agent's free assistant text is runtime noise
  // (the shared contract says it is never shown). The plugin does NOT post the
  // native reply blocks delivered during dispatch — the visible reply rides the
  // de_message_send tool. So a turn whose dispatch only emits native blocks (no
  // tool send) writes NOTHING back, and still emits turn.done.
  const state = fakeConnection()
  connectionsByAccount.set('default', state)
  const capture: { ctx?: any } = {}
  const channelRuntime = fakeChannelRuntime({ deliverBlocks: [{ text: 'native final answer' }], capture })
  const sent: any[] = []
  const { calls, restore } = installFakeFetch(() => ({ json: { status: 0 } }))
  try {
    await dispatchTurnToAgent(
      state,
      { cfg: {}, accountId: 'default', abortSignal: new AbortController().signal, log: NOOP_LOG, channelRuntime } as any,
      baseFrame(),
      fakeWs(sent),
    )
    // No write-back from the deliver path (the native block is dropped).
    assert.equal(
      calls.filter((c) => c.url.endsWith('/agent_tools/message-send')).length,
      0,
      'native reply blocks must NOT be auto-posted',
    )
    const done = sent.find((f) => f.type === 'turn.done')
    assert.ok(done, 'turn.done emitted')
    assert.equal(done.agent_id, 'agent-1')
    assert.equal(done.runtime_id, 'rt-1')
  } finally {
    restore()
  }
})

test('a dispatch failure posts a courtesy error + emits turn.error (not turn.done)', async () => {
  const state = fakeConnection()
  connectionsByAccount.set('default', state)
  const channelRuntime = {
    reply: {
      finalizeInboundContext: (c: any) => c,
      async dispatchReplyWithBufferedBlockDispatcher() {
        throw new Error('model exploded')
      },
    },
    session: { resolveStorePath: () => '/tmp', recordInboundSession: async () => {} },
  }
  const sent: any[] = []
  const { calls, restore } = installFakeFetch(() => ({ json: { status: 0 } }))
  try {
    await dispatchTurnToAgent(
      state,
      { cfg: {}, accountId: 'default', abortSignal: new AbortController().signal, log: NOOP_LOG, channelRuntime } as any,
      baseFrame(),
      fakeWs(sent),
    )
    const errSend = calls.find((c) => c.url.endsWith('/agent_tools/message-send'))
    assert.ok(errSend, 'a courtesy error message is posted on dispatch failure')
    assert.equal(errSend!.body.verb, 'message-send')
    assert.ok(sent.find((f) => f.type === 'turn.error'))
    assert.equal(sent.find((f) => f.type === 'turn.done'), undefined)
  } finally {
    restore()
  }
})

test('an aborted dispatch emits turn.error (aborted), not turn.done', async () => {
  const state = fakeConnection()
  connectionsByAccount.set('default', state)
  const channelRuntime = {
    reply: {
      finalizeInboundContext: (c: any) => c,
      async dispatchReplyWithBufferedBlockDispatcher(params: any) {
        // Simulate a turn.abort: abort the signal, then throw as a cancelled run would.
        params.replyOptions.abortSignal.dispatchEvent?.(new Event('abort'))
        const ac = [...state.turns.values()].pop()!.abort
        ac.abort()
        throw new Error('aborted by signal')
      },
    },
    session: { resolveStorePath: () => '/tmp', recordInboundSession: async () => {} },
  }
  const sent: any[] = []
  const { restore } = installFakeFetch(() => ({ json: { status: 0 } }))
  try {
    await dispatchTurnToAgent(
      state,
      { cfg: {}, accountId: 'default', abortSignal: new AbortController().signal, log: NOOP_LOG, channelRuntime } as any,
      baseFrame(),
      fakeWs(sent),
    )
    const err = sent.find((f) => f.type === 'turn.error')
    assert.ok(err, 'turn.error emitted on abort')
    assert.equal(err.error, 'aborted')
    assert.equal(sent.find((f) => f.type === 'turn.done'), undefined)
  } finally {
    restore()
  }
})

test('a turn.abort that raced ahead of registration (pendingAborts) still aborts the turn', async () => {
  // Abort-before-register: the Stop frame arrived before the fire-and-forget
  // dispatch registered the turn. dispatchTurnToAgent must drain
  // pendingAborts right after registering and abort immediately.
  const state = fakeConnection()
  state.pendingAborts.add('turn-1') // matches baseFrame().turn_id
  connectionsByAccount.set('default', state)
  const channelRuntime = {
    reply: {
      finalizeInboundContext: (c: any) => c,
      async dispatchReplyWithBufferedBlockDispatcher(params: any) {
        // The signal must already be aborted by the time dispatch runs.
        if (params.replyOptions.abortSignal.aborted) throw new Error('aborted by signal')
        return {}
      },
    },
    session: { resolveStorePath: () => '/tmp', recordInboundSession: async () => {} },
  }
  const sent: any[] = []
  const { restore } = installFakeFetch(() => ({ json: { status: 0 } }))
  try {
    await dispatchTurnToAgent(
      state,
      { cfg: {}, accountId: 'default', abortSignal: new AbortController().signal, log: NOOP_LOG, channelRuntime } as any,
      baseFrame(),
      fakeWs(sent),
    )
    assert.ok(sent.find((f) => f.type === 'turn.error'), 'queued abort produces turn.error')
    assert.equal(sent.find((f) => f.type === 'turn.done'), undefined)
    assert.equal(state.pendingAborts.has('turn-1'), false, 'pendingAborts entry consumed')
  } finally {
    restore()
  }
})

test('an abort during a dispatch that RESOLVES normally still emits turn.error, not turn.done', async () => {
  // the run can finish just as Stop arrives (dispatch resolves without
  // throwing). We must still report turn.error(aborted), never a misleading done.
  const state = fakeConnection()
  connectionsByAccount.set('default', state)
  const channelRuntime = {
    reply: {
      finalizeInboundContext: (c: any) => c,
      async dispatchReplyWithBufferedBlockDispatcher(_params: any) {
        // turn.abort arrives mid-run; the dispatch still resolves cleanly.
        const ac = [...state.turns.values()].pop()!.abort
        ac.abort()
        return {}
      },
    },
    session: { resolveStorePath: () => '/tmp', recordInboundSession: async () => {} },
  }
  const sent: any[] = []
  const { restore } = installFakeFetch(() => ({ json: { status: 0 } }))
  try {
    await dispatchTurnToAgent(
      state,
      { cfg: {}, accountId: 'default', abortSignal: new AbortController().signal, log: NOOP_LOG, channelRuntime } as any,
      baseFrame(),
      fakeWs(sent),
    )
    const err = sent.find((f) => f.type === 'turn.error')
    assert.ok(err, 'turn.error emitted even though dispatch resolved')
    assert.equal(err.error, 'aborted')
    assert.equal(sent.find((f) => f.type === 'turn.done'), undefined, 'no turn.done after abort')
  } finally {
    restore()
  }
})
