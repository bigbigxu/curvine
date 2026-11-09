import { useCallback, useEffect, useMemo, useState } from 'react'

import { useApi } from '@/api/api-context'
import type { TransferConfigPushResult, TransferConfigSnapshot } from '@/api/types'
import { ConfirmDialog } from '@/components/confirm-dialog'
import { EmptyNotice, ErrorAlert } from '@/components/data-state'
import { StatusBadge } from '@/components/resource-table'
import { Button } from '@/components/ui/button'
import { Input } from '@/components/ui/input'
import { Sheet, SheetContent, SheetDescription, SheetHeader, SheetTitle } from '@/components/ui/sheet'
import { Spinner } from '@/components/ui/spinner'
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from '@/components/ui/table'
import { useT } from '@/i18n'
import {
  aggregatedConfigValue,
  knobKind,
  parseConfigValue,
  TRANSFER_KNOBS
} from '@/lib/transfer-services'

/**
 * Live transfer knobs, edited across every instance at once.
 *
 * Two phases on purpose: the first push answers `needs_confirmation` and writes
 * **nothing** when an instance cannot be reached, so the operator gets a dialog
 * naming the silent instances before a cluster is half-applied. There is no
 * rollback — instances are independent processes — so the only honest guarantee
 * is "nothing changed unless you said so".
 */
export function TransferSettingsSheet({
  open,
  onOpenChange,
  onApplied
}: {
  open: boolean
  onOpenChange(open: boolean): void
  onApplied(): void
}) {
  const api = useApi()
  const t = useT()
  const [snapshots, setSnapshots] = useState<TransferConfigSnapshot[]>([])
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState<string | null>(null)
  const [drafts, setDrafts] = useState<Record<string, string>>({})
  const [busyKey, setBusyKey] = useState<string | null>(null)
  const [pendingConfirm, setPendingConfirm] = useState<{
    key: string
    value?: string
    reset: boolean
    result: TransferConfigPushResult
  } | null>(null)
  const [result, setResult] = useState<TransferConfigPushResult | null>(null)

  const load = useCallback(async () => {
    setLoading(true)
    setError(null)
    try {
      const data = await api.fetchTransferConfig()
      setSnapshots(data.items ?? [])
    } catch (nextError) {
      setError(nextError instanceof Error ? nextError.message : String(nextError))
    } finally {
      setLoading(false)
    }
  }, [api])

  useEffect(() => {
    if (open) void load()
  }, [open, load])

  const entries = useMemo(
    () => snapshots.flatMap((snapshot) => snapshot.entries ?? []),
    [snapshots]
  )
  const unreachable = useMemo(
    () => snapshots.filter((snapshot) => !snapshot.reachable),
    [snapshots]
  )

  /** First phase: no `apply_to_reachable_only`, so a partial fleet writes nothing. */
  async function submit(key: string, value: string | undefined, reset: boolean) {
    setBusyKey(key)
    setError(null)
    setResult(null)
    try {
      const next = await api.updateTransferConfig({
        key,
        value: reset ? undefined : value,
        reset
      })
      if (next.status === 'needs_confirmation') {
        setPendingConfirm({ key, value, reset, result: next })
        return
      }
      setResult(next)
      await load()
      onApplied()
    } catch (nextError) {
      setError(nextError instanceof Error ? nextError.message : String(nextError))
    } finally {
      setBusyKey(null)
    }
  }

  /** Second phase: the operator accepted the partial application. */
  async function confirmPartial() {
    const pending = pendingConfirm
    if (!pending) return
    setBusyKey(pending.key)
    try {
      const next = await api.updateTransferConfig({
        key: pending.key,
        value: pending.reset ? undefined : pending.value,
        reset: pending.reset,
        apply_to_reachable_only: true
      })
      setResult(next)
      setPendingConfirm(null)
      await load()
      onApplied()
    } catch (nextError) {
      setError(nextError instanceof Error ? nextError.message : String(nextError))
    } finally {
      setBusyKey(null)
    }
  }

  return (
    <>
      <Sheet open={open} onOpenChange={onOpenChange}>
        <SheetContent className="w-full sm:max-w-2xl">
          <SheetHeader>
            <SheetTitle>{t('transferServices.settings.title')}</SheetTitle>
            <SheetDescription>{t('transferServices.settings.subtitle')}</SheetDescription>
          </SheetHeader>

          <div className="flex min-h-0 flex-1 flex-col gap-3 overflow-auto px-4 pb-6">
            {error && <ErrorAlert message={error} />}

            {result && (
              <p
                className={
                  result.status === 'applied'
                    ? 'rounded-lg border border-border bg-muted/40 p-2.5 text-xs'
                    : 'rounded-lg border border-warning/40 bg-warning/10 p-2.5 text-xs text-warning'
                }
                data-testid="transfer-settings-result"
              >
                {t('transferServices.settings.result', {
                  status: t(`transferServices.settings.status.${result.status}`),
                  applied: result.applied ?? 0,
                  total: result.total ?? 0
                })}
              </p>
            )}

            {loading && snapshots.length === 0 ? (
              <div className="flex items-center gap-2 text-xs text-muted-foreground">
                <Spinner className="size-3.5" />
                {t('common.loading')}
              </div>
            ) : snapshots.length === 0 ? (
              <EmptyNotice
                title={t('transferServices.empty.title')}
                description={t('transferServices.empty.description')}
              />
            ) : (
              <>
                <p className="text-xs text-muted-foreground">
                  {t('transferServices.settings.instances', {
                    reachable: snapshots.length - unreachable.length,
                    total: snapshots.length
                  })}
                </p>
                {unreachable.length > 0 && (
                  <p className="text-xs text-warning" data-testid="transfer-settings-unreachable">
                    {t('transferServices.settings.unreachable', {
                      count: unreachable.length,
                      addrs: unreachable.map((snapshot) => snapshot.addr).join(', ')
                    })}
                  </p>
                )}
                <Table>
                  <TableHeader>
                    <TableRow>
                      <TableHead>{t('transferServices.settings.key')}</TableHead>
                      <TableHead>{t('transferServices.settings.effective')}</TableHead>
                      <TableHead className="text-right">
                        {t('transferServices.settings.actions')}
                      </TableHead>
                    </TableRow>
                  </TableHeader>
                  <TableBody>
                    {TRANSFER_KNOBS.map((knob) => {
                      const aggregated = aggregatedConfigValue(entries, knob.key)
                      const draft =
                        drafts[knob.key] ??
                        (aggregated.value !== undefined
                          ? String(parseConfigValue(aggregated.value) ?? '')
                          : '')
                      const busy = busyKey === knob.key
                      return (
                        <TableRow key={knob.key} data-testid={`transfer-knob-${knob.key}`}>
                          <TableCell>
                            <span className="font-mono text-xs">{knob.key}</span>
                          </TableCell>
                          <TableCell className="text-xs">
                            {aggregated.mixed ? (
                              <StatusBadge
                                value={t('transferServices.settings.mixed')}
                                tone="warning"
                              />
                            ) : (
                              <span className="tabular-nums">
                                {aggregated.value === undefined
                                  ? '—'
                                  : String(parseConfigValue(aggregated.value))}
                              </span>
                            )}
                          </TableCell>
                          <TableCell>
                            <div className="flex items-center justify-end gap-2">
                              <Input
                                className="h-8 w-32"
                                value={draft}
                                disabled={busy}
                                onChange={(event) =>
                                  setDrafts((current) => ({
                                    ...current,
                                    [knob.key]: event.target.value
                                  }))
                                }
                                data-testid={`transfer-knob-input-${knob.key}`}
                              />
                              <Button
                                size="sm"
                                disabled={busy || draft.trim() === ''}
                                onClick={() => void submit(knob.key, draft.trim(), false)}
                                data-testid={`transfer-knob-apply-${knob.key}`}
                              >
                                {t('transferServices.settings.apply')}
                              </Button>
                              <Button
                                size="sm"
                                variant="outline"
                                disabled={busy}
                                onClick={() => void submit(knob.key, undefined, true)}
                                data-testid={`transfer-knob-reset-${knob.key}`}
                              >
                                {t('transferServices.settings.reset')}
                              </Button>
                            </div>
                          </TableCell>
                        </TableRow>
                      )
                    })}
                  </TableBody>
                </Table>
                <p className="text-xs text-muted-foreground">
                  {t('transferServices.settings.hint')}
                </p>
                {knobKind('accept_jobs') === 'boolean' && (
                  <p className="text-xs text-muted-foreground">
                    {t('transferServices.settings.acceptJobsHint')}
                  </p>
                )}
              </>
            )}
          </div>
        </SheetContent>
      </Sheet>

      <ConfirmDialog
        open={Boolean(pendingConfirm)}
        title={t('transferServices.settings.confirmTitle')}
        description={t('transferServices.settings.confirmDescription', {
          unreachable: pendingConfirm?.result.unreachable?.length ?? 0,
          total: pendingConfirm?.result.configured?.length ?? 0,
          addrs: (pendingConfirm?.result.unreachable ?? []).join(', ')
        })}
        warning={t('transferServices.settings.confirmWarning')}
        confirmLabel={t('transferServices.settings.confirmApply')}
        cancelLabel={t('common.cancel')}
        destructive
        pending={busyKey !== null}
        onOpenChange={(next) => !next && setPendingConfirm(null)}
        onConfirm={() => void confirmPartial()}
        testId="transfer-settings-confirm"
      />
    </>
  )
}
