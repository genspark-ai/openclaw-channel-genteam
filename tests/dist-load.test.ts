// Build-and-load smoke test for the PUBLISHED plugin artifact.
//
// The plugin imports the shared upload core from a sibling source file
// (../shared/attachment-upload). esbuild `--bundle` must INLINE it into
// dist/index.js; a non-bundled build (`esbuild src/index.ts` without
// `--bundle`) would emit a literal `from "../shared/attachment-upload.ts"` that
// fails at runtime load — and no source-level (tsx) test would catch it because
// tsx resolves the .ts directly. Run the REAL build, then import the artifact:
// a clean import IS the proof the shared core was inlined.
import assert from 'node:assert/strict'
import { execFileSync } from 'node:child_process'
import { existsSync } from 'node:fs'
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
