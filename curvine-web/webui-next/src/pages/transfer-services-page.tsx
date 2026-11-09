import { useCallback, useMemo, useState } from 'react'

import { useApi } from '@/api/api-context'
import type { TransferServiceInfo, TransferServiceInstance } from '@/api/types'
import { EmptyNotice } from '@/components/data-state'
import { DetailList, DetailRow } from '@/components/detail-list'
import { MetricCard } from '@/components/metric-card'
import { PageHeader } from '@/components/page-header'
import { MonoText, ResourceTable, StatusBadge } from '@/components/resource-table'
import { Button } from '@/components/ui/button'
import { Sheet, SheetContent, SheetDescription, SheetHeader, SheetTitle } from '@/components/ui/sheet'
import { TableCell, TableRow } from '@/components/ui/table'
import { useT } from '@/i18n'
import { formatNumber, formatTime } from '@/lib/format'
import {
  formatDurationMs,
  instanceState,
  ringDrift,
  serviceStateTone,
  shortCommit,
  sortInstances,
  summarizeInstances,
  type TransferInstanceState
} from '@/lib/transfer-services'
import { useAsync } from '@/lib/use-async'
import { useSlidingSheet } from '@/lib/use-sliding-sheet'
import { useRefreshSignal } from '@/refresh/refresh-context'

const COLUMN_WIDTHS = ['18rem', '7rem', '11rem', '9rem', '13rem', '8rem', '7rem', '11rem', '11rem']
const COLUMN_KEYS = [
  'instance',
  'state',
  'version',
  'uptime',
  'jobs',
  'tasks',
  'store',
  'snapshot',
  'ring'
] as const

/**
 * Runtime view of the transfer service, one row per configured instance.
 *
 * The instance list comes from `transfer.endpoints` (there is no registry), and
 * every row is the instance's own answer to `GetTransferServiceInfo` — not a
 * claim derived from shared metrics. An instance that does not answer keeps its
 * row: unreachable members are the ones this page exists to reveal.
 */
export function TransferServicesPage() {
  const api = useApi()
  const t = useT()
  const load = useCallback(() => api.fetchTransferServices(), [api])
  const { data, loading, error, reload, refresh } = useAsync(load, [load])
  useRefreshSignal(refresh)

  const items = useMemo(() => data?.items ?? [], [data])
  // Drift is measured against *this console's* `transfer.endpoints`, not against
  // whatever the instances agree on: two processes configured with different
  // lists still agree with each other, and only the console's own config can
  // tell that one of them will place jobs somewhere the cluster does not expect.
  const configured = useMemo(() => data?.configured ?? [], [data])

  const summary = useMemo(() => summarizeInstances(items), [items])
  const rows = useMemo(() => sortInstances(items), [items])

  const [selected, setSelected] = useState<string | null>(null)
  const detail = useSlidingSheet(Boolean(selected), rows.find((row) => row.addr === selected))

  const stateLabel = (state: TransferInstanceState) =>
    t(`transferServices.states.${state}`, state)

  return (
    <div className="flex flex-col gap-4">
      <PageHeader
        title={t('transferServices.title')}
        description={t('transferServices.subtitle')}
        actions={
          <Button variant="outline" size="sm" onClick={() => void reload()} disabled={loading}>
            {t('transferServices.refresh')}
          </Button>
        }
      />

      {!loading && !error && items.length === 0 ? (
        <EmptyNotice
          title={t('transferServices.empty.title')}
          description={t('transferServices.empty.description')}
        />
      ) : (
        <>
          <div className="grid grid-cols-2 gap-3 lg:grid-cols-4">
            <MetricCard
              label={t('transferServices.metrics.instances')}
              value={formatNumber(summary.total)}
              meta={t('transferServices.metrics.instancesMeta')}
            />
            <MetricCard
              label={t('transferServices.metrics.healthy')}
              value={formatNumber(summary.healthy)}
              meta={t('transferServices.metrics.healthyMeta')}
              tone={summary.healthy > 0 ? 'good' : 'neutral'}
            />
            <MetricCard
              label={t('transferServices.metrics.unreachable')}
              value={formatNumber(summary.unreachable)}
              meta={t('transferServices.metrics.unreachableMeta')}
              tone={summary.unreachable > 0 ? 'bad' : 'neutral'}
            />
            <MetricCard
              label={t('transferServices.metrics.ownedJobs')}
              value={formatNumber(summary.ownedJobs)}
              meta={t('transferServices.metrics.ownedJobsMeta')}
              delta={t('transferServices.metrics.detail', {
                reachable: summary.reachable,
                total: summary.total
              })}
              tone={summary.reachable === summary.total ? 'good' : 'warn'}
            />
          </div>

          {summary.unreachable > 0 && !loading && !error && (
            <p className="text-sm text-warning">{t('transferServices.unavailable')}</p>
          )}

          <ResourceTable
            columnWidths={COLUMN_WIDTHS}
            columns={COLUMN_KEYS.map((key) => t(`transferServices.table.header${capitalize(key)}`))}
            loading={loading}
            error={error}
            emptyTitle={t('transferServices.empty.title')}
            emptyDescription={t('transferServices.empty.description')}
            rows={rows.map((item) => (
              <InstanceRow
                key={item.addr}
                item={item}
                configured={configured}
                label={stateLabel(instanceState(item))}
                onOpen={() => setSelected(item.addr)}
              />
            ))}
          />
        </>
      )}

      <Sheet open={detail.open} onOpenChange={(open) => !open && setSelected(null)}>
        <SheetContent className="w-full sm:max-w-xl">
          <SheetHeader>
            <SheetTitle>{t('transferServices.detail.title')}</SheetTitle>
            <SheetDescription className="font-mono text-xs">{selected || ''}</SheetDescription>
          </SheetHeader>
          {detail.value && (
            <InstanceDetail item={detail.value} configured={configured} label={stateLabel} />
          )}
        </SheetContent>
      </Sheet>
    </div>
  )
}

function InstanceRow({
  item,
  configured,
  label,
  onOpen
}: {
  item: TransferServiceInstance
  configured: string[]
  label: string
  onOpen: () => void
}) {
  const t = useT()
  const state = instanceState(item)
  const info = item.info
  const drift = ringDrift(info, configured)
  const unknown = t('transferServices.values.unknown')

  return (
    <TableRow
      className="cursor-pointer"
      onClick={onOpen}
      data-testid={`transfer-instance-${item.addr}`}
    >
      <TableCell>
        <MonoText>{item.addr}</MonoText>
        {!item.reachable && item.error && (
          <p className="mt-1 line-clamp-2 text-xs text-muted-foreground">{item.error}</p>
        )}
      </TableCell>
      <TableCell>
        <StatusBadge
          value={label}
          tone={serviceStateTone(state)}
          testId={`transfer-instance-state-${item.addr}`}
        />
      </TableCell>
      <TableCell className="text-xs">
        {info?.version ? (
          <>
            <span className="block truncate">{info.version}</span>
            <span className="block truncate text-muted-foreground">
              {shortCommit(info.version_commit) || unknown}
            </span>
          </>
        ) : (
          unknown
        )}
      </TableCell>
      <TableCell className="text-xs tabular-nums">
        {info ? formatDurationMs(info.uptime_ms) : unknown}
      </TableCell>
      <TableCell className="text-xs tabular-nums">
        {info ? (
          t('transferServices.values.jobsHere', {
            owned: formatNumber(info.in_memory_jobs),
            active: formatNumber(info.active_jobs)
          })
        ) : (
          unknown
        )}
      </TableCell>
      <TableCell className="text-xs tabular-nums">
        {info ? formatNumber(info.in_memory_running_tasks) : unknown}
      </TableCell>
      <TableCell className="text-xs">{info?.store_backend || unknown}</TableCell>
      <TableCell className="text-xs">
        {info ? (
          <>
            <span className="block tabular-nums">
              {t('transferServices.values.snapshotAges', {
                version: formatNumber(info.snapshot_version),
                age: formatDurationMs(info.snapshot_staleness_ms)
              })}
            </span>
            <span className="block truncate text-muted-foreground">
              {t('transferServices.values.snapshotWorkers', {
                live: formatNumber(info.snapshot_live_workers),
                capable: formatNumber(info.snapshot_capable_workers)
              })}
            </span>
          </>
        ) : (
          unknown
        )}
      </TableCell>
      <TableCell className="text-xs">
        {info ? (
          <span className={drift ? 'text-warning' : undefined}>
            {t(
              drift ? 'transferServices.values.ringDrift' : 'transferServices.values.ringMatches',
              { count: formatNumber(info.ring_members?.length) }
            )}
          </span>
        ) : (
          unknown
        )}
      </TableCell>
    </TableRow>
  )
}

function InstanceDetail({
  item,
  configured,
  label
}: {
  item: TransferServiceInstance
  configured: string[]
  label: (state: TransferInstanceState) => string
}) {
  const t = useT()
  const info: TransferServiceInfo = item.info || {}
  const state = instanceState(item)
  const drift = ringDrift(info, configured)
  const number = (value: unknown) => formatNumber(value)

  return (
    <div className="flex flex-col gap-5 overflow-y-auto px-4 pb-6">
      <section className="flex flex-col gap-2">
        <h3 className="text-xs font-medium tracking-wider text-muted-foreground uppercase">
          {t('transferServices.detail.identity')}
        </h3>
        <DetailList>
          <DetailRow label={t('transferServices.detail.addr')}>
            <MonoText>{item.addr}</MonoText>
          </DetailRow>
          <DetailRow label={t('transferServices.detail.webAddr')}>
            <MonoText>{info.web_addr || '—'}</MonoText>
          </DetailRow>
          <DetailRow label={t('transferServices.detail.instanceId')}>
            <MonoText>{info.instance_id || '—'}</MonoText>
          </DetailRow>
          <DetailRow label={t('transferServices.detail.state')}>
            <StatusBadge value={label(state)} tone={serviceStateTone(state)} />
          </DetailRow>
          {info.state_reason && (
            <DetailRow label={t('transferServices.detail.stateReason')}>
              <span className="text-xs">{info.state_reason}</span>
            </DetailRow>
          )}
          {item.error && (
            <DetailRow label={t('transferServices.detail.error')}>
              <span className="text-xs text-destructive">{item.error}</span>
            </DetailRow>
          )}
        </DetailList>
      </section>

      <section className="flex flex-col gap-2">
        <h3 className="text-xs font-medium tracking-wider text-muted-foreground uppercase">
          {t('transferServices.detail.version')}
        </h3>
        <DetailList>
          <DetailRow label={t('transferServices.detail.version')}>{info.version || '—'}</DetailRow>
          <DetailRow label={t('transferServices.detail.commit')}>
            <MonoText>{info.version_commit || '—'}</MonoText>
          </DetailRow>
          <DetailRow label={t('transferServices.detail.started')}>
            {info.start_time_ms ? formatTime(info.start_time_ms) : '—'}
          </DetailRow>
          <DetailRow label={t('transferServices.detail.uptime')}>
            {formatDurationMs(info.uptime_ms)}
          </DetailRow>
          <DetailRow label={t('transferServices.detail.store')}>{info.store_backend || '—'}</DetailRow>
        </DetailList>
      </section>

      <section className="flex flex-col gap-2">
        <h3 className="text-xs font-medium tracking-wider text-muted-foreground uppercase">
          {t('transferServices.detail.workload')}
        </h3>
        <DetailList>
          <DetailRow label={t('transferServices.detail.inMemoryJobs')}>
            {number(info.in_memory_jobs)}
          </DetailRow>
          <DetailRow label={t('transferServices.detail.inMemoryRunningTasks')}>
            {number(info.in_memory_running_tasks)}
          </DetailRow>
          <DetailRow label={t('transferServices.detail.activeJobs')}>
            {number(info.active_jobs)}
          </DetailRow>
          <DetailRow label={t('transferServices.detail.executingJobs')}>
            {number(info.executing_jobs)}
          </DetailRow>
        </DetailList>
      </section>

      <section className="flex flex-col gap-2">
        <h3 className="text-xs font-medium tracking-wider text-muted-foreground uppercase">
          {t('transferServices.detail.limits')}
        </h3>
        <DetailList>
          <DetailRow label={t('transferServices.detail.maxRunning')}>
            {number(info.max_running_transfers)}
          </DetailRow>
          <DetailRow label={t('transferServices.detail.workerTaskLimit')}>
            {number(info.worker_task_limit)}
          </DetailRow>
          <DetailRow label={t('transferServices.detail.maxTasksPerTransfer')}>
            {number(info.max_tasks_per_transfer)}
          </DetailRow>
          <DetailRow label={t('transferServices.detail.maxPlanEntries')}>
            {number(info.max_plan_entries)}
          </DetailRow>
        </DetailList>
      </section>

      <section className="flex flex-col gap-2">
        <h3 className="text-xs font-medium tracking-wider text-muted-foreground uppercase">
          {t('transferServices.detail.snapshot')}
        </h3>
        <DetailList>
          <DetailRow label={t('transferServices.detail.snapshotVersion')}>
            {number(info.snapshot_version)}
          </DetailRow>
          <DetailRow label={t('transferServices.detail.snapshotUpdated')}>
            {info.snapshot_updated_at ? formatTime(info.snapshot_updated_at) : '—'}
          </DetailRow>
          <DetailRow label={t('transferServices.detail.snapshotStaleness')}>
            {formatDurationMs(info.snapshot_staleness_ms)}
          </DetailRow>
          <DetailRow label={t('transferServices.detail.snapshotWorkers')}>
            {number(info.snapshot_live_workers)} / {number(info.snapshot_capable_workers)}
          </DetailRow>
        </DetailList>
      </section>

      <section className="flex flex-col gap-2">
        <h3 className="text-xs font-medium tracking-wider text-muted-foreground uppercase">
          {t('transferServices.detail.ring')}
        </h3>
        <DetailList>
          <DetailRow label={t('transferServices.detail.ringSelf')}>
            <MonoText>{info.ring_self || '—'}</MonoText>
          </DetailRow>
          <DetailRow label={t('transferServices.detail.ringMembers')}>
            <div className="flex flex-col gap-1">
              {(info.ring_members || []).map((member) => (
                <MonoText key={member}>{member}</MonoText>
              ))}
              {!info.ring_members?.length && <span>—</span>}
            </div>
          </DetailRow>
        </DetailList>
        {drift && <p className="text-xs text-warning">{t('transferServices.detail.ringDrift')}</p>}
      </section>
    </div>
  )
}

function capitalize(value: string): string {
  return value.charAt(0).toUpperCase() + value.slice(1)
}