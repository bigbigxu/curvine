import type { TransferServiceInfo, TransferServiceInstance } from '@/api/types'
import type { StatusTone } from '@/components/resource-table'

/**
 * The instance's own state, plus the one state the backend cannot report:
 * `unreachable`. It is derived, not invented — an instance that does not answer
 * simply has no state to read.
 */
export type TransferInstanceState = 'normal' | 'degraded' | 'not_ready' | 'unreachable'

const KNOWN_STATES: TransferInstanceState[] = ['normal', 'degraded', 'not_ready']

/** Collapses `{reachable, info}` into the single state the badge renders. */
export function instanceState(instance: TransferServiceInstance): TransferInstanceState {
  if (!instance.reachable || !instance.info) return 'unreachable'
  const raw = String(instance.info.state ?? '').trim().toLowerCase()
  if (KNOWN_STATES.includes(raw as TransferInstanceState)) return raw as TransferInstanceState
  // An instance older than this RPC answers without a state: fall back to its
  // ready flag rather than pretending it is fine.
  return instance.info.ready === false ? 'not_ready' : 'normal'
}

/** Tone is derived from the raw state so it survives localisation. */
export function serviceStateTone(state: TransferInstanceState): StatusTone {
  switch (state) {
    case 'normal':
      return 'positive'
    case 'degraded':
      return 'warning'
    case 'not_ready':
    case 'unreachable':
      return 'negative'
    default:
      return 'neutral'
  }
}

/** Problems first: this page exists to show what needs attention. */
const SEVERITY: TransferInstanceState[] = ['unreachable', 'not_ready', 'degraded', 'normal']

export function sortInstances(
  items: TransferServiceInstance[]
): TransferServiceInstance[] {
  return [...items].sort(
    (a, b) => SEVERITY.indexOf(instanceState(a)) - SEVERITY.indexOf(instanceState(b))
  )
}

/** `3d 4h` / `5h 12m` / `42s`. Unknown or future timestamps render as an em dash. */
export function formatDurationMs(value: unknown): string {
  const ms = Number(value)
  if (!Number.isFinite(ms) || ms < 0) return '—'
  const seconds = Math.floor(ms / 1000)
  if (seconds < 60) return `${seconds}s`
  const minutes = Math.floor(seconds / 60)
  if (minutes < 60) return `${minutes}m`
  const hours = Math.floor(minutes / 60)
  if (hours < 24) return `${hours}h ${minutes % 60}m`
  return `${Math.floor(hours / 24)}d ${hours % 24}h`
}

/** First 7 characters of a commit, or the value unchanged when it is not a sha. */
export function shortCommit(commit: unknown): string {
  const text = String(commit ?? '').trim()
  if (!text || text === 'unknown') return ''
  return /^[0-9a-f]{7,40}$/i.test(text) ? text.slice(0, 7) : text
}

/**
 * Whether this instance's placement ring disagrees with the configured list.
 *
 * `transfer.endpoints` is the only instance list there is, so a ring that does
 * not match it means this process places jobs on a different set of instances
 * than the console (and the other processes) assume. Returns `false` when either
 * side is unknown, so a missing field never renders as a false alarm.
 */
export function ringDrift(info: TransferServiceInfo | null | undefined, configured: string[]): boolean {
  const members = (info?.ring_members || []).map((member) => member.trim()).filter(Boolean)
  const expected = configured.map((member) => member.trim()).filter(Boolean)
  if (!members.length || !expected.length) return false
  const left = [...members].sort()
  const right = [...expected].sort()
  return left.length !== right.length || left.some((member, index) => member !== right[index])
}

export interface TransferServicesSummary {
  total: number
  reachable: number
  healthy: number
  unreachable: number
  ownedJobs: number
}

/**
 * Fleet-level counts for the summary cards.
 *
 * `ownedJobs` sums each instance's own load, so it double-counts nothing: a job
 * lives in exactly one process' memory. Store-wide counters are deliberately not
 * summed — every instance reports the same number.
 */
export function summarizeInstances(items: TransferServiceInstance[]): TransferServicesSummary {
  let reachable = 0
  let healthy = 0
  let ownedJobs = 0
  for (const item of items) {
    const state = instanceState(item)
    if (state !== 'unreachable') reachable += 1
    if (state === 'normal') healthy += 1
    ownedJobs += Number(item.info?.in_memory_jobs || 0)
  }
  return {
    total: items.length,
    reachable,
    healthy,
    unreachable: items.length - reachable,
    ownedJobs
  }
}