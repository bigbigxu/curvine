import { render, screen } from '@testing-library/react'
import { describe, expect, it, vi } from 'vitest'

import type { TransferInfo } from '@/api/types'
import { I18nProvider, LOCALE_STORAGE_KEY } from '@/i18n'
import { TransferDiagnosticsSheet } from '../transfer-diagnostics-sheet'

describe('TransferDiagnosticsSheet', () => {
  it('shows retained summary counts when task details have expired', async () => {
    window.localStorage.setItem(LOCALE_STORAGE_KEY, 'en')
    const transfer = {
      job_id: '12345678-1234-1234-1234-123456789abc',
      state: 'failed',
      failed_files: 12,
      running_files: 0,
      tasks: [],
      task_details_expired: true
    } as TransferInfo

    render(
      <I18nProvider>
        <TransferDiagnosticsSheet
          open
          transfer={transfer}
          loading={false}
          busy={false}
          taskNextPageToken=""
          taskPageSize={50}
          taskLoadingMore={false}
          onClose={vi.fn()}
          onCopy={vi.fn()}
          onRetry={vi.fn()}
          onLoadMore={vi.fn()}
        />
      </I18nProvider>
    )

    expect(await screen.findByText('Task details have expired; the summary counts are retained.'))
      .toBeInTheDocument()
    expect(screen.getByText('12')).toBeInTheDocument()
  })

  it('shows the owning instance and the job-level retry counters', async () => {
    window.localStorage.setItem(LOCALE_STORAGE_KEY, 'en')
    const transfer = {
      job_id: '12345678-1234-1234-1234-123456789abc',
      state: 'running',
      tasks: [],
      ownership: {
        placement_addr: 'cv-transfer-0:9010',
        owner_addr: 'cv-transfer-1:9010',
        client_request_id: 'job_abc123',
        takeover_count: 2,
        planning_attempts: 1,
        mount_snapshot_attempts: 3,
        overwrite: true,
        replicas: null
      }
    } as TransferInfo

    render(
      <I18nProvider>
        <TransferDiagnosticsSheet
          open
          transfer={transfer}
          loading={false}
          busy={false}
          taskNextPageToken=""
          taskPageSize={50}
          taskLoadingMore={false}
          onClose={vi.fn()}
          onCopy={vi.fn()}
          onRetry={vi.fn()}
          onLoadMore={vi.fn()}
        />
      </I18nProvider>
    )

    // Assigned and running differ: that is a takeover, and both addresses have
    // to be visible so the operator can see it.
    expect(await screen.findByText('Assigned to')).toBeInTheDocument()
    expect(screen.getByText('cv-transfer-0:9010')).toBeInTheDocument()
    expect(screen.getByText('Running on')).toBeInTheDocument()
    expect(screen.getByText('cv-transfer-1:9010')).toBeInTheDocument()
    expect(screen.getByText('job_abc123')).toBeInTheDocument()
    expect(screen.getByText('Takeovers')).toBeInTheDocument()
    expect(screen.getByText('Overwrite existing destination')).toBeInTheDocument()
    expect(screen.getByText('not set')).toBeInTheDocument()
  })
})
