import type { WorkerAction, WorkerInfo } from '@/api/types'

/**
 * The single state of a worker (CV-60, `WorkerSummary` in curvine-model).
 *
 * A worker is in exactly one of these at any moment. The master stores the
 * state as two fields — the operator intent (`admin`) and the heartbeat fact
 * (`liveness`) — because a heartbeat must never overwrite an intent, but the
 * value an operator reads is always one of these seven. `lost` > `shutdown` >
 * `stale` > `decommissioning` > `decommissioned` > `readonly` > `normal`: when
 * the heartbeats stop, "what happened to the node" beats "what I asked for".
 */
export type WorkerState =
  | 'normal'
  | 'readonly'
  | 'decommissioning'
  | 'decommissioned'
  | 'stale'
  | 'lost'
  | 'shutdown'

/** Intent axis, only used to decide which actions are legal. */
export type WorkerAdmin = 'normal' | 'cordoned' | 'decommissioning' | 'decommissioned'
/** Heartbeat axis, only used to explain *why* a node reads as stale/lost. */
export type WorkerLiveness = 'running' | 'stale' | 'lost' | 'shutdown'
export type WorkerStateFilter = 'all' | WorkerState

/** Every state, ordered the way the filter bar shows them. */
export const WORKER_STATES: WorkerState[] = [
  'normal',
  'readonly',
  'decommissioning',
  'decommissioned',
  'stale',
  'lost',
  'shutdown',
]

const STATE_ALIASES: Record<string, WorkerState> = {
  normal: 'normal',
  readonly: 'readonly',
  'read-only': 'readonly',
  read_only: 'readonly',
  decommissioning: 'decommissioning',
  decommissioned: 'decommissioned',
  stale: 'stale',
  lost: 'lost',
  shutdown: 'shutdown',
  // Single-axis values from before the split, still returned by an old master
  // (and by old fixtures): blacklist became read-only, decommission is final.
  live: 'normal',
  blacklist: 'readonly',
  cordoned: 'readonly',
  decommission: 'decommissioned',
  // Composite labels (`admin+liveness`) an older build of this console sent.
  'normal+running': 'normal',
  'cordoned+running': 'readonly',
  'decommissioning+running': 'decommissioning',
  'decommissioned+running': 'decommissioned',
  'normal+stale': 'stale',
  'cordoned+stale': 'stale',
  'decommissioning+stale': 'stale',
  'decommissioned+stale': 'stale',
  'normal+lost': 'lost',
  'cordoned+lost': 'lost',
  'decommissioning+lost': 'lost',
  'decommissioned+lost': 'lost',
  'normal+shutdown': 'shutdown',
  'cordoned+shutdown': 'shutdown',
  'decommissioning+shutdown': 'shutdown',
  'decommissioned+shutdown': 'shutdown',
}

const ADMIN_ALIASES: Record<string, WorkerAdmin> = {
  normal: 'normal',
  cordoned: 'cordoned',
  decommissioning: 'decommissioning',
  decommissioned: 'decommissioned',
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

function normalise(value: unknown): string {
  return String(value ?? '')
    .trim()
    .toLowerCase()
}

/**
 * The one state to show.
 *
 * Prefers the master's derived `state`, falls back to the two axes (a master
 * between CV-60 and this console), and finally to the legacy single `status`.
 */
export function workerState(worker: WorkerInfo): WorkerState {
  const fromState = normalise(worker.state)
  const resolved = STATE_ALIASES[fromState]
  if (resolved) return resolved

  const admin = ADMIN_ALIASES[normalise(worker.worker_state?.admin)]
  const liveness = LIVENESS_ALIASES[normalise(worker.worker_state?.liveness)]
  if (admin || liveness) {
    // Same precedence as the master: heartbeat failures win.
    if (liveness && liveness !== 'running') return liveness
    return admin === 'cordoned' ? 'readonly' : (admin ?? 'normal')
  }

  return STATE_ALIASES[normalise(worker.status)] ?? 'normal'
}

/** Operator intent for a worker, tolerating an old master's legacy `status`. */
export function workerAdmin(worker: WorkerInfo): WorkerAdmin {
  const fromAxes = ADMIN_ALIASES[normalise(worker.worker_state?.admin)]
  if (fromAxes) return fromAxes
  return ADMIN_ALIASES[normalise(worker.status)] ?? 'normal'
}

/** Heartbeat fact for a worker, tolerating an old master's legacy `status`. */
export function workerLiveness(worker: WorkerInfo): WorkerLiveness {
  const fromAxes = LIVENESS_ALIASES[normalise(worker.worker_state?.liveness)]
  if (fromAxes) return fromAxes
  return normalise(worker.status) === 'lost' ? 'lost' : 'running'
}

export function matchesWorkerFilter(state: WorkerState, filter: WorkerStateFilter): boolean {
  return filter === 'all' || state === filter
}

export interface WorkerActionOption {
  action: WorkerAction
  /** i18n key for the button and the confirm dialog title. */
  labelKey: string
  /** i18n key of the verb phrase used inside the confirmation sentence. */
  verbKey: string
  /** Confirm the action in red. */
  destructive?: boolean
  /** Show the extra "the residual blocks are deleted" warning. */
  dataLoss?: boolean
  /** i18n key of the one-line explanation shown under the confirmation. */
  hintKey?: string
}

/**
 * The actions that make sense given the operator intent.
 *
 * Actions change the intent, not the heartbeat, so they follow `admin` rather
 * than the single displayed state: an operator may cordon or retire a node
 * while it is stale or lost, and the master applies the intent when it returns.
 */
export function availableWorkerActions(admin: WorkerAdmin): WorkerActionOption[] {
  switch (admin) {
    case 'normal':
      return [
        {
          action: 'cordon',
          labelKey: 'workers.actions.cordon',
          verbKey: 'workers.actionVerb.cordon',
          hintKey: 'workers.actions.cordonHint',
        },
        {
          action: 'decommission',
          labelKey: 'workers.actions.decommission',
          verbKey: 'workers.actionVerb.decommission',
          hintKey: 'workers.actions.decommissionHint',
        },
      ]
    case 'cordoned':
      return [
        {
          action: 'uncordon',
          labelKey: 'workers.actions.uncordon',
          verbKey: 'workers.actionVerb.uncordon',
          hintKey: 'workers.actions.uncordonHint',
        },
        {
          action: 'decommission',
          labelKey: 'workers.actions.decommission',
          verbKey: 'workers.actionVerb.decommission',
          hintKey: 'workers.actions.decommissionHint',
        },
      ]
    case 'decommissioning':
      return [
        {
          action: 'cancel-decommission',
          labelKey: 'workers.actions.cancelDecommission',
          verbKey: 'workers.actionVerb.cancel-decommission',
          hintKey: 'workers.actions.cancelHint',
        },
        {
          action: 'force-decommission',
          labelKey: 'workers.actions.forceDecommission',
          verbKey: 'workers.actionVerb.force-decommission',
          destructive: true,
          dataLoss: true,
        },
      ]
    case 'decommissioned':
      return [
        {
          action: 'recommission',
          labelKey: 'workers.actions.recommission',
          verbKey: 'workers.actionVerb.recommission',
        },
      ]
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
  return { action, labelKey: `workers.actionVerb.${action}`, verbKey: `workers.actionVerb.${action}` }
}
