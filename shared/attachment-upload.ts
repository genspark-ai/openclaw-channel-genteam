// GenTeam attachment session+SAS direct-upload core.
//
// ONE implementation of the 1 GiB direct-upload flow — per-file
// ``attachment-session-init`` → streamed PUT to the Azure write SAS →
// streaming sha256 → a SINGLE ``attachment-session-finalize`` that sends every
// staged blob as ONE media message — reused by the `de` tool surfaces that
// speak it (the local/sandbox `de` CLI and this OpenClaw channel plugin). It is
// bundled (esbuild) into each package's own published artifact.
//
// Transport is INJECTED via ``post`` so each caller supplies its own base URL +
// auth (the plugin posts directly to the gateway-configured endpoint with the
// agent token). The SAS PUT always goes DIRECTLY to Azure Blob (the body never
// crosses the gateway / ingress), so it is done here with a plain ``fetch``.
//
// Returns a DISCRIMINATED result rather than calling process.exit / throwing —
// each caller maps it to its own surface (the CLI exits with a code + stderr
// envelope; the plugin returns a tool result). ``serverBody`` + ``httpStatus``
// are set ONLY when the failure is an actual backend HTTP rejection of an
// agent_tools verb, so a caller can echo the backend's typed envelope verbatim;
// all client-side failures (validation / malformed parse / network / SAS PUT /
// hashing) carry a typed ``code`` + ``stage`` instead.

import { createHash } from "node:crypto";
import { accessSync, constants as fsConstants, createReadStream, statSync } from "node:fs";
import { basename } from "node:path";
import { Readable } from "node:stream";

// 1 GiB — mirrors the backend per-file/per-message byte cap.
export const GENTEAM_DIRECT_UPLOAD_MAX_BYTES = 1_073_741_824;
// Mirrors the backend per-message attachment count cap (=10).
export const GENTEAM_ATTACHMENT_MAX_COUNT = 10;

const DEFAULT_PUT_TIMEOUT_MS = 300_000; // 5 min — large blobs over slow links

export type AttachmentUploadStage =
  | "validate"
  | "session_init"
  | "blob_put"
  | "session_finalize";

/** Caller-injected transport. POSTs a JSON ``agent_tools`` verb (handling the
 *  base URL + auth + any loopback proxy) and resolves with the upstream status
 *  + raw body. Throws on a transport (connect / DNS / timeout) failure. */
export type AgentToolPost = (
  verb: string,
  payload: Record<string, unknown>,
  signal?: AbortSignal,
) => Promise<{ status: number; body: string }>;

export interface AttachmentUploadParams {
  /** Resolved local file paths. Callers pre-validate any path allow-list
   *  (e.g. the plugin's ``attachmentRoots``) BEFORE calling. */
  files: string[];
  target: string;
  /** Optional caption — rides the finalize ``text`` field. */
  text?: string | null;
  idempotencyKey?: string | null;
  parentMessage?: string | null;
  postToChannel?: boolean;
  post: AgentToolPost;
  signal?: AbortSignal;
  /** SAS PUT timeout (default 5 min). */
  putTimeoutMs?: number;
  /** Per-file + aggregate byte cap (default 1 GiB). */
  maxBytes?: number;
  /** Max attachments per message (default 10). */
  maxCount?: number;
}

export type AttachmentUploadError = {
  ok: false;
  code: string;
  message: string;
  /** Exit-code hint for the CLI surface: 2 = local pre-flight validation
   *  (don't retry), 1 = network / storage / backend (retryable). */
  exitCode: 1 | 2;
  stage: AttachmentUploadStage;
  /** Set ONLY for an actual backend HTTP rejection of an agent_tools verb
   *  (init / finalize ``>= 400`` or a 2xx ``ok:false``). When set, callers echo
   *  the backend's typed envelope verbatim; ``httpStatus`` is set alongside.
   *  Client-side failures leave both undefined and rely on ``code``/``message``. */
  serverBody?: string;
  httpStatus?: number;
  detail?: Record<string, unknown>;
};

export type AttachmentUploadResult =
  | { ok: true; status: number; body: string }
  | AttachmentUploadError;

function localError(
  code: string,
  message: string,
  detail?: Record<string, unknown>,
): AttachmentUploadError {
  return { ok: false, code, message, exitCode: 2, stage: "validate", detail };
}

function networkError(
  stage: AttachmentUploadStage,
  err: unknown,
): AttachmentUploadError {
  const reason = err instanceof Error ? err.message : String(err);
  return {
    ok: false,
    code: "DE_NETWORK_ERROR",
    message: `network failure during ${stage}: ${reason}`,
    exitCode: 1,
    stage,
  };
}

/** Combine a caller-supplied cancellation ``signal`` with a per-request stall
 *  ``timeoutMs`` so BOTH bound the request — an XOR (``signal ?? timeout``)
 *  silently drops the timeout whenever a caller passes a signal (the plugin
 *  ALWAYS passes the turn's abort signal), leaving a stalled 1 GiB PUT bounded
 *  only by the turn lease. Uses ``AbortSignal.any`` where present (Node >= 20.3)
 *  and falls back to a manual fan-in on the Node 18 floor in ``engines``. */
export function combineSignals(
  signal: AbortSignal | undefined,
  timeoutMs: number,
): AbortSignal {
  const timeout = AbortSignal.timeout(timeoutMs);
  if (!signal) return timeout;
  if (typeof (AbortSignal as { any?: unknown }).any === "function") {
    return AbortSignal.any([signal, timeout]);
  }
  const controller = new AbortController();
  const listeners: Array<{ s: AbortSignal; fn: () => void }> = [];
  const cleanup = () => {
    for (const { s, fn } of listeners) s.removeEventListener("abort", fn);
  };
  for (const s of [signal, timeout]) {
    if (s.aborted) {
      controller.abort(s.reason);
      break;
    }
    const fn = () => {
      if (!controller.signal.aborted) controller.abort(s.reason);
    };
    s.addEventListener("abort", fn, { once: true });
    listeners.push({ s, fn });
  }
  // Drop the per-source listeners once the combined signal settles so a
  // listener never lingers on a longer-lived caller (turn) signal — the
  // timeout always fires within ``timeoutMs`` if nothing else does, so this
  // cleanup is guaranteed to run and bounds the listener's lifetime.
  if (controller.signal.aborted) cleanup();
  else controller.signal.addEventListener("abort", cleanup, { once: true });
  return controller.signal;
}

/** SHA-256 of a file, streamed so a 1 GiB attachment never materialises in
 *  memory. Manifest idempotency hint only (the backend does NOT re-hash the
 *  staged blob — see the backend's trust model). */
export function sha256File(path: string): Promise<string> {
  return new Promise((resolve, reject) => {
    const hash = createHash("sha256");
    const stream = createReadStream(path);
    stream.on("error", reject);
    stream.on("data", (chunk) => hash.update(chunk));
    stream.on("end", () => resolve(hash.digest("hex")));
  });
}

/** PUT a file body straight to the Azure Blob write SAS minted by
 *  ``attachment-session-init``. Streams from disk with an explicit
 *  Content-Length (Azure's single PutBlob requires it and rejects chunked
 *  transfer encoding) so a 1 GiB file never buffers. Returns the HTTP status;
 *  throws on a transport failure (after destroying the read stream so the file
 *  descriptor is released — cancelling the web stream does not reliably destroy
 *  the underlying ``fs`` stream). The SAS only carries write+create on this one
 *  blob key, so the URL by itself is not a general write capability.
 *
 *  Content-Length is taken from a FRESH stat here (not the pre-flight size):
 *  the body is a live ``createReadStream`` of whatever the file contains at PUT
 *  time, so declaring the pre-flight size for a file truncated/grown in the
 *  init round-trip window would either error (undici body/length mismatch) or
 *  silently ship a truncated blob. Stat + stream open back-to-back keep them in
 *  agreement; a stat throw (file vanished mid-flow) propagates as a blob_put
 *  transport failure. */
async function putBlobViaSas(
  uploadUrl: string,
  filePath: string,
  contentType: string,
  timeoutMs: number,
  signal?: AbortSignal,
): Promise<number> {
  const size = statSync(filePath).size;
  const nodeStream = createReadStream(filePath);
  const webStream = Readable.toWeb(nodeStream) as unknown as ReadableStream<Uint8Array>;
  const init: RequestInit & { duplex?: "half" } = {
    method: "PUT",
    headers: {
      "x-ms-blob-type": "BlockBlob",
      "Content-Type": contentType,
      "Content-Length": String(size),
    },
    body: webStream as unknown as RequestInit["body"],
    duplex: "half",
    signal: combineSignals(signal, timeoutMs),
  };
  try {
    const resp = await fetch(uploadUrl, init);
    if (resp.status >= 400) {
      // Azure responded early on a failed PUT (e.g. expired/forbidden SAS =
      // 403, lease conflict = 409) without draining the request body, so fetch
      // did NOT consume the stream to completion and the underlying fs read
      // stream is left open. Destroy it (and cancel the response body) so a
      // long-lived gateway does not leak a file descriptor on every failed PUT
      // (→ EMFILE). The 2xx path drains the body fully, closing the fd.
      nodeStream.destroy();
      try {
        await resp.body?.cancel();
      } catch {
        /* body already settled */
      }
    }
    return resp.status;
  } catch (err) {
    nodeStream.destroy();
    throw err;
  }
}

interface SessionInit {
  uploadUrl: string;
  uploadSession: string;
  contentType: string;
}

function parseSessionInit(body: string): SessionInit | null {
  let parsed: unknown;
  try {
    parsed = JSON.parse(body);
  } catch {
    return null;
  }
  const obj = (parsed ?? {}) as Record<string, unknown>;
  const uploadUrl = typeof obj.upload_url === "string" ? obj.upload_url : "";
  const uploadSession =
    typeof obj.upload_session === "string" ? obj.upload_session : "";
  const contentType =
    typeof obj.content_type === "string" && obj.content_type
      ? obj.content_type
      : "application/octet-stream";
  if (!uploadUrl || !uploadSession) return null;
  return { uploadUrl, uploadSession, contentType };
}

/** Upload N local files via the session+SAS direct-upload flow and send them as
 *  ONE media message (batch finalize). Pure of any process/exit concern:
 *  returns a discriminated result the caller maps to its own surface. */
export async function uploadAttachmentsViaSession(
  params: AttachmentUploadParams,
): Promise<AttachmentUploadResult> {
  const maxBytes = params.maxBytes ?? GENTEAM_DIRECT_UPLOAD_MAX_BYTES;
  const maxCount = params.maxCount ?? GENTEAM_ATTACHMENT_MAX_COUNT;
  const putTimeoutMs = params.putTimeoutMs ?? DEFAULT_PUT_TIMEOUT_MS;
  const { files, target, post, signal } = params;

  // ---- Pre-flight validation (BEFORE any upload — fail fast). Capture each
  // file's size here and REUSE it below so a file removed/renamed between
  // validation and upload is still classified as a pre-flight error rather
  // than crashing on a second stat. ----
  if (files.length === 0) {
    return localError(
      "DE_ATTACHMENT_REQUIRED",
      "at least one attachment path is required",
    );
  }
  if (files.length > maxCount) {
    return localError(
      "DE_TOO_MANY_ATTACHMENTS",
      `too many attachments (max ${maxCount})`,
      { count: files.length, max_count: maxCount },
    );
  }
  const validated: Array<{ path: string; size: number }> = [];
  let totalSize = 0;
  for (const path of files) {
    let st;
    try {
      st = statSync(path);
    } catch {
      return localError("DE_ATTACHMENT_NOT_FOUND", `attachment not found: ${path}`, {
        path,
      });
    }
    if (!st.isFile()) {
      return localError(
        "DE_ATTACHMENT_NOT_FOUND",
        `attachment is not a regular file: ${path}`,
        { path },
      );
    }
    if (st.size <= 0) {
      return localError("DE_ATTACHMENT_EMPTY", `attachment is empty: ${path}`, {
        path,
      });
    }
    if (st.size > maxBytes) {
      return localError(
        "DE_ATTACHMENT_TOO_LARGE",
        `attachment exceeds the per-file size cap (max ${maxBytes} bytes): ${path}`,
        { path, size: st.size, max_size: maxBytes },
      );
    }
    // Read-permission probe: a file we can stat (parent-dir traverse perm) but
    // not READ would otherwise pass pre-flight and only fail mid-PUT — after a
    // wasted session-init + SAS mint — as an opaque blob_put transport error.
    // Classify it here as a local validation error (no backend round-trip).
    try {
      accessSync(path, fsConstants.R_OK);
    } catch {
      return localError(
        "DE_ATTACHMENT_NOT_READABLE",
        `attachment is not readable: ${path}`,
        { path },
      );
    }
    validated.push({ path, size: st.size });
    totalSize += st.size;
  }
  // Aggregate (whole-message) cap — mirrors the backend finalize check; fail
  // fast instead of uploading everything and getting ATTACHMENTS_TOO_LARGE.
  if (files.length > 1 && totalSize > maxBytes) {
    return localError(
      "DE_ATTACHMENTS_TOO_LARGE",
      "combined attachment size exceeds the per-message cap",
      { size: totalSize, max_size: maxBytes },
    );
  }

  // ---- Per file: session-init → streamed SAS PUT → sha256 ----
  const filesPayload: Array<{ upload_session: string; file_sha256: string }> = [];
  for (const { path, size } of validated) {
    const fname = basename(path) || "file";
    // ``content_type`` is omitted — the backend resolves a canonical type from
    // the file extension and returns it for the PUT's Content-Type.
    const initPayload: Record<string, unknown> = {
      target,
      file_name: fname,
      file_size: size,
    };
    if (params.parentMessage) initPayload.parent_message = params.parentMessage;

    let initResp: { status: number; body: string };
    try {
      initResp = await post("attachment-session-init", initPayload, signal);
    } catch (err) {
      return networkError("session_init", err);
    }
    if (initResp.status >= 400) {
      // Backend rejection — surface the typed envelope verbatim.
      return {
        ok: false,
        code: "SESSION_INIT_REJECTED",
        message: `attachment-session-init failed (HTTP ${initResp.status})`,
        exitCode: 1,
        stage: "session_init",
        httpStatus: initResp.status,
        serverBody: initResp.body,
      };
    }
    const session = parseSessionInit(initResp.body);
    if (!session) {
      // 2xx but unparseable / missing fields — client-side malformation, NOT a
      // backend rejection, so emit a typed code (no serverBody echo).
      return {
        ok: false,
        code: "DE_SESSION_INIT_MALFORMED",
        message:
          "attachment-session-init response was not JSON or was missing upload_url / upload_session",
        exitCode: 1,
        stage: "session_init",
      };
    }

    let putStatus: number;
    try {
      putStatus = await putBlobViaSas(
        session.uploadUrl,
        path,
        session.contentType,
        putTimeoutMs,
        signal,
      );
    } catch (err) {
      return networkError("blob_put", err);
    }
    if (putStatus >= 400) {
      return {
        ok: false,
        code: "DE_SAS_PUT_FAILED",
        message: `direct upload to storage failed (HTTP ${putStatus})`,
        exitCode: 1,
        stage: "blob_put",
        detail: { http_code: putStatus },
      };
    }

    let fileSha: string;
    try {
      fileSha = await sha256File(path);
    } catch (err) {
      const reason = err instanceof Error ? err.message : String(err);
      return {
        ok: false,
        code: "DE_ATTACHMENT_SHA256_FAILED",
        message: `failed to hash attachment: ${reason}`,
        exitCode: 1,
        stage: "blob_put",
        detail: { path },
      };
    }
    filesPayload.push({ upload_session: session.uploadSession, file_sha256: fileSha });
  }

  // ---- Single batch finalize → ONE media message ----
  const finalizePayload: Record<string, unknown> = { target, files: filesPayload };
  if (params.text) finalizePayload.text = params.text;
  if (params.idempotencyKey) finalizePayload.idempotency_key = params.idempotencyKey;
  if (params.parentMessage) finalizePayload.parent_message = params.parentMessage;
  if (params.postToChannel) finalizePayload.post_to_channel = true;

  let finResp: { status: number; body: string };
  try {
    finResp = await post("attachment-session-finalize", finalizePayload, signal);
  } catch (err) {
    return networkError("session_finalize", err);
  }
  if (finResp.status >= 400) {
    return {
      ok: false,
      code: "SESSION_FINALIZE_REJECTED",
      message: `attachment-session-finalize failed (HTTP ${finResp.status})`,
      exitCode: 1,
      stage: "session_finalize",
      httpStatus: finResp.status,
      serverBody: finResp.body,
    };
  }
  // 2xx — a body whose ``ok`` is explicitly false is a logical backend failure.
  try {
    const parsed = JSON.parse(finResp.body) as { ok?: unknown };
    if (parsed && typeof parsed === "object" && parsed.ok === false) {
      return {
        ok: false,
        code: "SESSION_FINALIZE_FAILED",
        message: "attachment-session-finalize returned ok:false",
        exitCode: 1,
        stage: "session_finalize",
        httpStatus: finResp.status,
        serverBody: finResp.body,
      };
    }
  } catch {
    // non-JSON 2xx body — treat as success and pass it through verbatim.
  }
  return { ok: true, status: finResp.status, body: finResp.body };
}
