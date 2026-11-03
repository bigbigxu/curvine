import { useCallback, useMemo, useState } from 'react'
import { ServerIcon } from 'lucide-react'
import { useNavigate, useParams } from 'react-router-dom'

import { useApi } from '@/api/api-context'
import type { WorkerAction, WorkerInfo } from '@/api/types'
import { ConfirmDialog } from '@/components/confirm-dialog'
import { ErrorAlert } from '@/components/data-state'
import { Meter, usagePercent } from '@/components/meter'
import { MetricCard } from '@/components/metric-card'
import { PageHeader } from '@/components/page-header'
import { ResourceTable, StatusBadge, workerStatusTone } from '@/components/resource-table'
import {
  availableWorkerActions,
  matchesWorkerFilter,
  workerActionOption,
  workerAdmin,
  workerLiveness,
  workerStateLabel,
  type WorkerAdmin,
  type WorkerLiveness,
  type WorkerStateFilter,
} from '@/lib/worker-state'
import { Input } from '@/components/ui/input'
import { TableCell, TableRow } from '@/components/ui/table'
import { ToggleGroup, ToggleGroupItem } from '@/components/ui/toggle-group'
import { useT } from '@/i18n'
import {
  formatBytes,
  formatTime,
  versionsMismatch,
  workerAddress,
  workerHostname,
  workerId,
  workerShortVersion
} from '@/lib/format'
import { translateDelta } from '@/lib/i18nLabels'
import { useMasterRuntime } from '@/master-runtime/master-runtime-context'
import { useRefreshSignal } from '@/refresh/refresh-context'
import { useAsync } from '@/lib/use-async'
import { useSlidingSheet } from '@/lib/use-sliding-sheet'
import { WorkerDetailSheet } from './worker-detail-sheet'
import { ProfileDialog } from './profile-dialog'

type WorkerRow = WorkerInfo & {
  /** `admin` or `admin+liveness`, the label the table shows. */
  rowStatus: string
  admin: WorkerAdmin
  liveness: WorkerLiveness
}

const FILTERS: WorkerStateFilter[] = [
  'all',
  'normal',
  'cordoned',
  'decommissioning',
  'decommissioned',
  'stale',
  'lost',
  'shutdown',
]

/** Unhealthy directories, i.e. the worker cannot take new writes for them. */
function unhealthyDirs(worker: WorkerInfo) {
  return Object.values(worker.storage_map || {}).filter(
    (dir) => String(dir.health_state ?? '').toLowerCase() === 'unusable'
  )
}

/** Worker fact (can it write), not the master's routing decision. */
function isHealthy(worker: WorkerInfo): boolean {
  if (typeof worker.accepting_allocations === 'boolean') return worker.accepting_allocations
  return unhealthyDirs(worker).length === 0
}

function healthTone(worker: WorkerInfo): 'positive' | 'negative' | 'neutral' {
  if (!isHealthy(worker)) return 'negative'
  return 'positive'
}

function ipAddress(worker: WorkerInfo): string {
  return worker.display_ip || worker.address?.ip_addr || worker.ip_addr || '-'
}

function rpcPort(worker: WorkerInfo): string | number {
  return worker.address?.rpc_port || '-'
}

export function WorkersPage() {
  const t = useT()
  const api = useApi()
  const navigate = useNavigate()
  const { id: detailSelector } = useParams()
  const detail = useSlidingSheet(Boolean(detailSelector), detailSelector)
  const { runtime } = useMasterRuntime()

  const [statusFilter, setStatusFilter] = useState<WorkerStateFilter>('all')
  const [query, setQuery] = useState('')
  const [pendingAction, setPendingAction] = useState<{ worker: WorkerRow; action: WorkerAction } | null>(
    null
  )
  const [submitting, setSubmitting] = useState(false)
  const [actionError, setActionError] = useState<string | null>(null)
  // Which worker's flame graph is open, if any. Kept as the worker object
  // rather than an id so the dialog does not re-resolve it after the list
  // refreshes behind the modal.
  const [profileWorker, setProfileWorker] = useState<WorkerInfo | null>(null)

  const load = useCallback(() => api.fetchWorkersData(), [api])
  const { data, loading, error, reload, refresh } = useAsync(load, [load])
  useRefreshSignal(refresh)

  const allWorkers = useMemo<WorkerRow[]>(() => {
    const decorate = (worker: WorkerInfo): WorkerRow => {
      const admin = workerAdmin(worker)
      const liveness = workerLiveness(worker)
      return { ...worker, admin, liveness, rowStatus: workerStateLabel(admin, liveness) }
    }
    if (data?.workers?.length) return data.workers.map(decorate)
    // An old master has no flat list and no state axes: rebuild what we can from
    // the legacy buckets, which is the only state signal it sends.
    const legacy: WorkerInfo[] = [
      ...(data?.live_workers || []),
      ...(data?.blacklist_workers || []),
      ...(data?.decommission_workers || []),
      ...(data?.lost_workers || []),
    ]
    return legacy.map(decorate)
  }, [data])

  /** Fleet count for one axis value; server-side buckets when available. */
  const count = useCallback(
    (axis: 'admin' | 'liveness', value: string): number => {
      const fromServer = data?.buckets?.[axis]?.[value]
      if (typeof fromServer === 'number') return fromServer
      return allWorkers.filter((worker) =>
        axis === 'admin' ? worker.admin === value : worker.liveness === value
      ).length
    },
    [allWorkers, data]
  )

  const filtered = useMemo(() => {
    const needle = query.toLowerCase()
    return allWorkers
      .filter(
        (worker) =>
          matchesWorkerFilter(worker.admin, worker.liveness, statusFilter) &&
          `${workerHostname(worker)} ${workerId(worker)} ${workerAddress(worker)} ${ipAddress(worker)} ${workerShortVersion(
            worker
          )}`
            .toLowerCase()
            .includes(needle)
      )
      // Surface problems first: this list exists to show what needs attention.
      .sort((a, b) => Number(isHealthy(b)) - Number(isHealthy(a)))
  }, [allWorkers, statusFilter, query])

  const activeWorkers = useMemo(
    () => allWorkers.filter((worker) => worker.admin !== 'decommissioned'),
    [allWorkers]
  )

  // Two axes, one badge: the admin intent, plus the liveness fact when it is not
  // `running` (`decommissioning · stale`). Keeping them apart is the whole point
  // of the state split, so the console must not collapse them.
  const stateText = (admin: WorkerAdmin, liveness: WorkerLiveness): string => {
    const base = t(`workers.states.${admin}`)
    return liveness === 'running' ? base : `${base} · ${t(`workers.liveness.${liveness}`)}`
  }
  const stateTone = (admin: WorkerAdmin, liveness: WorkerLiveness) =>
    workerStatusTone(liveness === 'running' ? admin : liveness)
  const totalCapacity = activeWorkers.reduce((sum, worker) => sum + Number(worker.capacity || 0), 0)
  const totalAvailable = activeWorkers.reduce((sum, worker) => sum + Number(worker.available || 0), 0)
  const clusterUsage = usagePercent(totalCapacity, totalAvailable)

  const delta = (value: string) => translateDelta(t, value)

  function workerVersionIssues(worker: WorkerInfo) {
    return versionsMismatch(workerShortVersion(worker), runtime.version)
  }

  async function confirmAction() {
    const target = pendingAction
    if (!target) return
    setSubmitting(true)
    setActionError(null)
    try {
      await api.updateWorkerAction(workerAddress(target.worker), target.action)
      setPendingAction(null)
      await reload()
    } catch (nextError) {
      setActionError(nextError instanceof Error ? nextError.message : t('common.error'))
    } finally {
      setSubmitting(false)
    }
  }

  return (
    <>
      <PageHeader
        eyebrow={t('nav.groupRuntime')}
        title={t('nav.workers')}
        description={t('nav.workersSubtitle')}
      />

      {error && <ErrorAlert message={error} />}

      <section className="grid gap-3 sm:grid-cols-2 xl:grid-cols-5">
        <MetricCard
          label={t('workers.metrics.liveWorkers')}
          value={count('admin', 'normal')}
          meta={t('workers.metrics.healthyHeartbeat')}
          delta={delta('online')}
          tone="good"
        />
        <MetricCard
          label={t('workers.metrics.managed')}
          value={
            count('admin', 'cordoned') +
            count('admin', 'decommissioning') +
            count('admin', 'decommissioned')
          }
          meta={t('workers.metrics.blacklistDecommission')}
          delta={`${count('admin', 'cordoned')} / ${count('admin', 'decommissioning')} / ${count(
            'admin',
            'decommissioned'
          )}`}
          tone={
            count('admin', 'cordoned') +
              count('admin', 'decommissioning') +
              count('admin', 'decommissioned')
              ? 'warn'
              : 'neutral'
          }
        />
        <MetricCard
          label={t('workers.metrics.totalCapacity')}
          value={formatBytes(totalCapacity)}
          meta={t('workers.metrics.activeWorkers')}
          delta={t('workers.metrics.usedPercent', { percent: clusterUsage })}
          tone="neutral"
        />
        <MetricCard
          label={t('workers.metrics.unhealthy')}
          value={activeWorkers.filter((worker) => !isHealthy(worker)).length}
          meta={t('workers.metrics.unhealthyMeta')}
          delta={delta(activeWorkers.some((worker) => !isHealthy(worker)) ? 'check' : 'none')}
          tone={activeWorkers.some((worker) => !isHealthy(worker)) ? 'bad' : 'good'}
        />
        <MetricCard
          label={t('workers.metrics.lostWorkers')}
          value={
            count('liveness', 'stale') + count('liveness', 'lost') + count('liveness', 'shutdown')
          }
          meta={t('workers.metrics.requiresAttention')}
          delta={`${count('liveness', 'stale')} / ${count('liveness', 'lost')} / ${count(
            'liveness',
            'shutdown'
          )}`}
          tone={
            count('liveness', 'stale') + count('liveness', 'lost') + count('liveness', 'shutdown')
              ? 'bad'
              : 'good'
          }
        />
      </section>

      <section className="flex flex-col gap-3">
        <div className="flex flex-wrap items-center justify-between gap-2">
          <div className="flex flex-col gap-0.5">
            <h2 className="font-heading text-base font-medium">{t('workers.list.title')}</h2>
            <p className="text-xs text-muted-foreground">{t('workers.list.subtitle')}</p>
          </div>
          <ToggleGroup
            type="single"
            value={statusFilter}
            onValueChange={(value) => {
              if (value) setStatusFilter(value as WorkerStateFilter)
            }}
            data-testid="workers-filter"
          >
            {FILTERS.map((filter) => (
              <ToggleGroupItem
                key={filter}
                value={filter}
                size="sm"
                data-testid={`workers-filter-${filter}`}
              >
                {t(`workers.filters.${filter}`)}
              </ToggleGroupItem>
            ))}
          </ToggleGroup>
        </div>

        <div className="flex flex-wrap items-center gap-2 rounded-lg border border-border bg-muted/30 px-3 py-2">
          <Input
            value={query}
            onChange={(event) => setQuery(event.target.value)}
            placeholder={t('workers.list.searchPlaceholder')}
            className="max-w-sm"
            aria-label={t('workers.list.searchPlaceholder')}
            data-testid="workers-search"
          />
          <span className="text-xs text-muted-foreground tabular-nums">
            {filtered.length} / {allWorkers.length}
          </span>
        </div>

        {actionError && <ErrorAlert message={actionError} />}

        <ResourceTable
          loading={loading}
          error={null}
          emptyTitle={t('workers.list.empty')}
          columnWidths={['19%', '7%', '8%', '19%', '14%', '5%', '10%', '18%']}
          columns={[
            t('workers.list.headerWorker'),
            t('workers.list.headerStatus'),
            t('workers.list.headerHealth'),
            t('workers.list.headerVersion'),
            t('workers.list.headerUsage'),
            t('workers.list.headerBlocks'),
            t('workers.list.headerHeartbeat'),
            t('workers.list.headerActions')
          ]}
          rows={filtered.map((worker) => {
            const usage = usagePercent(worker.capacity, worker.available)
            const versionIssue = workerVersionIssues(worker)
            const selector = workerAddress(worker)
            return (
              <TableRow key={`${worker.rowStatus}-${selector}`} data-testid={`worker-row-${selector}`}>
                <TableCell>
                  <div className="flex w-full min-w-0 items-center gap-2">
                    <ServerIcon
                      className="size-4 shrink-0 text-muted-foreground"
                      aria-hidden
                      data-testid={`worker-icon-${selector}`}
                    />
                    <button
                      type="button"
                      className="flex min-w-0 flex-1 items-baseline gap-1.5 text-left"
                      onClick={() => navigate(`/workers/${encodeURIComponent(selector)}`)}
                      data-testid={`worker-open-${selector}`}
                    >
                      <strong className="truncate font-medium hover:text-primary">
                        {workerHostname(worker)}
                      </strong>
                      <span className="truncate font-mono text-xs text-muted-foreground">
                        {t('workers.list.rpc', 'RPC')} {rpcPort(worker)}
                        {ipAddress(worker) !== '-' ? ` · ${ipAddress(worker)}` : ''}
                      </span>
                    </button>
                  </div>
                </TableCell>
                <TableCell>
                  <StatusBadge
                    value={stateText(worker.admin, worker.liveness)}
                    tone={stateTone(worker.admin, worker.liveness)}
                    testId={`worker-state-${selector}`}
                  />
                </TableCell>
                <TableCell>
                  <StatusBadge
                    value={isHealthy(worker) ? t('workers.health.healthy') : t('workers.health.unhealthy')}
                    tone={healthTone(worker)}
                    data-testid={`worker-health-${selector}`}
                  />
                </TableCell>
                <TableCell>
                  {/* Its own column: the reported version carries the build
                      commit and branch, so it needs more room than a subtitle —
                      and it is proportional, like the detail page's copy, because
                      monospace would push the branch past the column. */}
                  <span
                    className={`block truncate text-xs ${versionIssue ? 'text-warning' : undefined}`}
                    title={versionIssue ? t('workers.detail.runtime.versionMismatch') : undefined}
                    data-testid={`worker-version-${selector}`}
                  >
                    {workerShortVersion(worker) || t('workers.detail.runtime.notReported')}
                  </span>
                </TableCell>
                <TableCell>
                  {/* One line: `used` is derived from capacity - available, so
                      showing used/capacity keeps every figure recoverable. */}
                  <div className="flex w-full items-center gap-2">
                    {!isHealthy(worker) && (
                      <span
                        className="shrink-0 rounded bg-destructive/10 px-1.5 py-0.5 text-[10px] font-medium text-destructive"
                        title={unhealthyDirs(worker)
                          .map((dir) => `${dir.dir_path || dir.storage_id}: ${dir.fault_kind || 'unusable'}`)
                          .join('\n')}
                      >
                        {t('workers.health.notAllocatable')}
                      </span>
                    )}
                    <strong className="shrink-0 text-xs tabular-nums">{usage}%</strong>
                    <Meter
                      percent={usage}
                      tone={usage >= 90 ? 'destructive' : usage >= 75 ? 'warning' : 'primary'}
                      className="flex-1"
                    />
                    <span className="shrink-0 text-xs whitespace-nowrap text-muted-foreground tabular-nums">
                      {formatBytes(Math.max(0, Number(worker.capacity || 0) - Number(worker.available || 0)))}
                      {' / '}
                      {formatBytes(worker.capacity)}
                    </span>
                  </div>
                </TableCell>
                <TableCell className="tabular-nums">{worker.block_num || 0}</TableCell>
                <TableCell
                  className="truncate text-xs text-muted-foreground"
                  title={formatTime(worker.last_update_ms || worker.last_update)}
                >
                  {formatTime(worker.last_update_ms || worker.last_update)}
                </TableCell>
                <TableCell>
                  <div className="flex items-center gap-1">
                    <WorkerActionButton
                      label={t('workers.actions.detail')}
                      onClick={() => navigate(`/workers/${encodeURIComponent(selector)}`)}
                    />
                    <WorkerActionButton
                      label={t('workers.actions.profile')}
                      onClick={() => setProfileWorker(worker)}
                    />
                    {availableWorkerActions(worker.admin).map((option) => (
                      <WorkerActionButton
                        key={option.action}
                        label={t(option.labelKey)}
                        onClick={() => setPendingAction({ worker, action: option.action })}
                      />
                    ))}
                  </div>
                </TableCell>
              </TableRow>
            )
          })}
        />

        <ConfirmDialog
          open={pendingAction !== null}
          title={pendingAction ? t(workerActionOption(pendingAction.action).labelKey) : ''}
          description={(() => {
            if (!pendingAction) return ''
            const worker = workerAddress(pendingAction.worker)
            const option = workerActionOption(pendingAction.action)
            if (option.dataLoss) {
              return `${t('workers.actions.forceConfirm', { worker })} ${t(
                'workers.actions.forceWarning'
              )}`
            }
            const base = t('workers.actions.confirm', {
              action: t(option.labelKey),
              worker
            })
            if (option.action === 'decommission') {
              return `${base} ${t('workers.actions.decommissionHint')}`
            }
            if (option.action === 'cancel-decommission') {
              return `${base} ${t('workers.actions.cancelHint')}`
            }
            return base
          })()}
          confirmLabel={t('common.confirm')}
          cancelLabel={t('common.cancel')}
          pending={submitting}
          destructive={pendingAction ? workerActionOption(pendingAction.action).destructive : false}
          onOpenChange={(open) => {
            if (!open) setPendingAction(null)
          }}
          onConfirm={() => void confirmAction()}
          testId="workers-action-confirm"
        />
      </section>

      {/* The detail lives on the same route as the list (`/workers/:id?`), so it
          renders over it: the list stays mounted behind the panel, and a deep
          link or a back button lands on the same state. */}
      {detail.mounted && detail.value && (
        <WorkerDetailSheet
          open={detail.open}
          selector={detail.value}
          onClose={() => navigate('/workers')}
        />
      )}

      <ProfileDialog
        host={
          profileWorker
            ? profileWorker.display_ip ||
              profileWorker.address?.ip_addr ||
              profileWorker.address?.hostname ||
              profileWorker.ip_addr
            : null
        }
        webPort={profileWorker?.address?.web_port}
        target={
          profileWorker
            ? `${t('nav.workers')} ${workerHostname(profileWorker)}:${rpcPort(profileWorker)}`
            : undefined
        }
        open={profileWorker !== null}
        onOpenChange={(open) => {
          if (!open) setProfileWorker(null)
        }}
      />
    </>
  )
}

function WorkerActionButton({ label, onClick }: { label: string; onClick(): void }) {
  return (
    <button
      type="button"
      className="rounded-md px-1.5 py-0.5 text-xs font-medium text-primary transition-colors hover:bg-accent hover:text-accent-foreground"
      onClick={onClick}
    >
      {label}
    </button>
  )
}
