import type { WorkerAction, WorkerInfo } from '@/api/types'

/**
 * The two orthogonal state axes of CV-60.
 *
 * `admin` is what an operator asked for; `liveness` is what the heartbeats say.
 * The console must never collapse them into one label: a `decommissioning` node
 * that is also `stale` is a different situation from a healthy `cordoned` one.
 */
export type WorkerAdmin = 'normal' | 'cordoned' | 'decommissioning' | 'decommissioned'
export type WorkerLiveness = 'running' | 'stale' | 'lost' | 'shutdown'
export type WorkerStateFilter = 'all' | WorkerAdmin | Exclude<WorkerLiveness, 'running'>

const ADMIN_ALIASES: Record<string, WorkerAdmin> = {
  normal: 'normal',
  cordoned: 'cordoned',
  decommissioning: 'decommissioning',
  decommissioned: 'decommissioned',
  // Legacy single-axis values from before the split, still returned by an old
  // master (and by the old test fixtures).
  live: 'normal',
  blacklist: 'cordoned',
  decommission: 'decommissioned',
}

const LIVENESS_ALIASES: Record<string, WorkerLiveness> = {
  running: 'running',
  stale: 'stale',
  lost: 'lost',
  shutdown: 'shutdown',
}

/** Operator intent for a worker, tolerating an old master's legacy `status`. */
export function workerAdmin(worker: WorkerInfo): WorkerAdmin {
  const fromState = String(worker.worker_state?.admin ?? '')
    .trim()
    .toLowerCase()
  if (fromState) return ADMIN_ALIASES[fromState] ?? 'normal'
  return ADMIN_ALIASES[String(worker.status ?? '').trim().toLowerCase()] ?? 'normal'
}

/** Heartbeat fact for a worker, tolerating an old master's legacy `status`. */
export function workerLiveness(worker: WorkerInfo): WorkerLiveness {
  const fromState = String(worker.worker_state?.liveness ?? '')
    .trim()
    .toLowerCase()
  if (fromState) return LIVENESS_ALIASES[fromState] ?? 'running'
  return String(worker.status ?? '').trim().toLowerCase() === 'lost' ? 'lost' : 'running'
}

/** `decommissioning+stale` style label; the plain admin state when running. */
export function workerStateLabel(admin: WorkerAdmin, liveness: WorkerLiveness): string {
  return liveness === 'running' ? admin : `${admin}+${liveness}`
}

export function matchesWorkerFilter(
  admin: WorkerAdmin,
  liveness: WorkerLiveness,
  filter: WorkerStateFilter
): boolean {
  if (filter === 'all') return true
  if (filter === 'stale' || filter === 'lost' || filter === 'shutdown') {
    return liveness === filter
  }
  return admin === filter
}

export interface WorkerActionOption {
  action: WorkerAction
  /** i18n key for the button and the confirm dialog title. */
  labelKey: string
  /** Confirm the action in red. */
  destructive?: boolean
  /** Show the extra "the residual blocks are deleted" warning. */
  dataLoss?: boolean
}

/**
 * The actions that make sense in a given admin state.
 *
 * Liveness deliberately does not gate them: an operator may record an intent
 * while the node is stale or lost, and the master enforces it when the node
 * comes back (design #16). Only the admin axis decides which transition is legal.
 */
export function availableWorkerActions(admin: WorkerAdmin): WorkerActionOption[] {
  switch (admin) {
    case 'normal':
      return [
        { action: 'cordon', labelKey: 'workers.actions.cordon' },
        { action: 'decommission', labelKey: 'workers.actions.decommission' },
      ]
    case 'cordoned':
      return [
        { action: 'uncordon', labelKey: 'workers.actions.uncordon' },
        { action: 'decommission', labelKey: 'workers.actions.decommission' },
      ]
    case 'decommissioning':
      return [
        { action: 'cancel-decommission', labelKey: 'workers.actions.cancelDecommission' },
        {
          action: 'force-decommission',
          labelKey: 'workers.actions.forceDecommission',
          destructive: true,
          dataLoss: true,
        },
      ]
    case 'decommissioned':
      return [{ action: 'recommission', labelKey: 'workers.actions.recommission' }]
  }
}

/** Look up the option behind a pending action, for the confirm dialog. */
export function workerActionOption(action: WorkerAction): WorkerActionOption {
  for (const admin of [
    'normal',
    'cordoned',
    'decommissioning',
    'decommissioned',
  ] as WorkerAdmin[]) {
    const match = availableWorkerActions(admin).find((option) => option.action === action)
    if (match) return match
  }
  // Legacy action names (`blacklist` / `allow`) can still arrive from an old
  // deep link; keep them usable.
  return { action, labelKey: `workers.actionVerb.${action}` }
}
