import { API_BASE } from './http'
import type { CiviSettings, CiviSettingsPayload } from './types'

/**
 * CIVI AI assistant HTTP surface (`/api/ai/*`).
 *
 * These calls deliberately bypass the shared axios client:
 *
 * 1. The chat endpoint is an SSE stream — fetch is the only way to read it.
 * 2. A missing `X-CIVI-Token` answers 401 *by design* (settings saves retry
 *    once with a freshly typed token). The shared client treats every 401 as
 *    "console session expired" and signs the user out, which would make the
 *    token gate log people out of the whole console.
 *
 * Same-origin fetch sends the session cookie by default, so the console
 * session gate on `/api/ai/*` keeps working without extra options.
 */

export const CIVI_TOKEN_STORAGE_KEY = 'civi_token'

/** Shared-token header, attached to POSTs when the user stored one. */
function civiHeaders(json: boolean): Record<string, string> {
  const headers: Record<string, string> = {}
  if (json) headers['Content-Type'] = 'application/json'
  const token = localStorage.getItem(CIVI_TOKEN_STORAGE_KEY)
  if (token) headers['X-CIVI-Token'] = token
  return headers
}

export function storedCiviToken(): string {
  return localStorage.getItem(CIVI_TOKEN_STORAGE_KEY) ?? ''
}

/**
 * Loads the CIVI settings payload. Returns `null` when the AI router is not
 * mounted (worker/transfer deployments, older backends): CIVI is optional and
 * must stay invisible instead of erroring there.
 */
export async function fetchCiviSettings(): Promise<CiviSettingsPayload | null> {
  try {
    const resp = await fetch(`${API_BASE}/ai/settings`, { headers: civiHeaders(false) })
    if (!resp.ok) return null
    return (await resp.json()) as CiviSettingsPayload
  } catch {
    return null
  }
}

/** Persists settings; throws `Error` with the backend `message` on failure. */
export async function saveCiviSettings(settings: CiviSettings): Promise<CiviSettingsPayload> {
  const resp = await fetch(`${API_BASE}/ai/settings`, {
    method: 'POST',
    headers: civiHeaders(true),
    body: JSON.stringify(settings)
  })
  if (!resp.ok) {
    const body = (await resp.json().catch(() => ({}))) as { message?: string }
    throw new Error(body.message || `HTTP ${resp.status}`)
  }
  return (await resp.json()) as CiviSettingsPayload
}

/** Opens the streaming chat request (SSE). The caller reads the body. */
export async function openCiviChat(
  body: Record<string, unknown>,
  signal: AbortSignal
): Promise<Response> {
  return fetch(`${API_BASE}/ai/chat`, {
    method: 'POST',
    headers: civiHeaders(true),
    body: JSON.stringify(body),
    signal
  })
}
