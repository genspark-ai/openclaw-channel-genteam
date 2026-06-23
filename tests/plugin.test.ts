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
  responder: (url: string, init: any) => { status?: number; json?: any; text?: string },
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

test('attachment-view forces metadata_only (a model tool never streams bytes)', async () => {
  const state = fakeConnection()
  connectionsByAccount.set('default', state)
  const { calls, restore } = installFakeFetch(() => ({ json: { status: 0 } }))
  try {
    const tools = buildGenteamTools({ messageChannel: 'genteam', agentAccountId: 'default' })
    const view = tools.find((t: any) => t.name === 'de_attachment_view')
    await view.execute('c', { attachment_ref: 'm1:0' }, undefined)
    assert.equal(calls[0].body.verb, 'attachment-view')
    assert.equal(calls[0].body.metadata_only, true)
    assert.equal(calls[0].body.attachment_ref, 'm1:0')
  } finally {
    restore()
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
