// Build-and-load smoke test for the PUBLISHED plugin artifact.
//
// The published artifact is just dist/index.js — the package ships no
// node_modules, and the gateway installs plugin deps with `--omit=dev` and
// resolves modules from the loaded extension path. So anything left as an
// external runtime import must be resolvable in THAT environment, or the whole
// module load throws. Two real regressions this guards:
//
//   1. The shared upload core is imported from a sibling source dir
//      (../shared/attachment-upload). esbuild `--bundle` must INLINE it; the
//      old non-bundled command emitted a literal `from "../../shared/..."` that
//      fails at runtime load — and no source-level (tsx) test catches it,
//      because tsx resolves the .ts directly.
//   2. `typebox` must be INLINED, not left as a bare `import 'typebox'`. When it
//      was external it crashed the gateway with "Cannot find module 'typebox'"
//      (dev-only dependency + `--omit=dev`, or installed to a different path
//      than the loaded extension). Bundling removes the runtime dependency.
//
// Running the REAL build and importing the artifact is the proof. The static
// scan is the strong guard: a clean `await import` passes locally even when
// typebox is external, because the dev node_modules HAS typebox — production
// does not.
import assert from 'node:assert/strict'
import { execFileSync } from 'node:child_process'
import { existsSync, readFileSync } from 'node:fs'
import { dirname, join } from 'node:path'
import { fileURLToPath, pathToFileURL } from 'node:url'
import { test } from 'node:test'

const here = dirname(fileURLToPath(import.meta.url))
const pkgRoot = join(here, '..')

test('the bundled plugin loads with the shared upload core inlined', async () => {
  execFileSync('npm', ['run', 'build'], { cwd: pkgRoot, stdio: 'pipe' })
  const dist = join(pkgRoot, 'dist', 'index.js')
  assert.ok(existsSync(dist), 'dist/index.js must exist after build')

  // A dangling `../../shared/...` import throws here at module evaluation.
  const mod = await import(pathToFileURL(dist).href)
  assert.equal(typeof mod.plugin, 'object', 'plugin export must load')
  assert.equal(typeof mod.buildGenteamTools, 'function', 'buildGenteamTools export must load')
  assert.ok(Array.isArray(mod.DE_TOOL_NAMES) && mod.DE_TOOL_NAMES.length > 0)
})

test('the build inlines typebox and leaves only ws external', async () => {
  execFileSync('npm', ['run', 'build'], { cwd: pkgRoot, stdio: 'pipe' })
  const dist = join(pkgRoot, 'dist', 'index.js')
  const src = readFileSync(dist, 'utf8')

  // typebox MUST be bundled. A bare `from "typebox"` would resolve from the
  // gateway's node_modules at load time, which `--omit=dev` / the extension
  // path layout does not reliably populate — the exact "Cannot find module
  // 'typebox'" crash this artifact must never reintroduce.
  assert.ok(
    !/\bfrom\s*["']typebox["']/.test(src) && !/\brequire\(\s*["']typebox["']\s*\)/.test(src),
    'dist/index.js must NOT reference an external `typebox` module — it must be inlined by --bundle',
  )

  // ws stays external ON PURPOSE: it is resolved at runtime from the gateway so
  // the plugin reuses the gateway's WebSocket (the built-in global is corrupted
  // by the gateway's undici dispatcher). So a `"ws"` specifier must remain.
  assert.ok(/["']ws["']/.test(src), 'ws must remain a runtime resolution, not be bundled')
})
