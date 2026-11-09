import { render, screen, waitFor } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { MemoryRouter } from 'react-router-dom'
import { beforeEach, describe, expect, it } from 'vitest'

import { ApiProvider } from '@/api/api-context'
import type { ApiClient } from '@/api/client'
import type { TransferServiceInstance } from '@/api/types'
import { I18nProvider, LOCALE_STORAGE_KEY } from '@/i18n'
import { RefreshProvider } from '@/refresh/refresh-context'
import { TransferServicesPage } from './transfer-services-page'

const RING = ['cv-transfer-0:9010', 'cv-transfer-1:9010']

function healthy(): TransferServiceInstance {
  return {
    addr: 'cv-transfer-0:9010',
    reachable: true,
    error: null,
    info: {
      role: 'transfer',
      instance_id: 'instance-0',
      addr: 'cv-transfer-0:9010',
      web_addr: 'cv-transfer-0:9011',
      version: '0.2.2',
      version_commit: 'abcdef0123456789',
      start_time_ms: Date.now() - 3 * 86_400_000,
      uptime_ms: 3 * 86_400_000,
      ready: true,
      state: 'normal',
      state_reason: '',
      store_backend: 'sqlite',
      max_running_transfers: 8,
      worker_task_limit: 16,
      max_tasks_per_transfer: 500000,
      max_plan_entries: 500000,
      active_jobs: 5,
      executing_jobs: 2,
      in_memory_jobs: 2,
      in_memory_running_tasks: 7,
      snapshot_version: 11,
      snapshot_updated_at: Date.now() - 1500,
      snapshot_staleness_ms: 1500,
      snapshot_live_workers: 3,
      snapshot_capable_workers: 2,
      ring_self: 'cv-transfer-0:9010',
      ring_members: RING
    }
  }
}

function silent(): TransferServiceInstance {
  return {
    addr: 'cv-transfer-1:9010',
    reachable: false,
    error: 'no answer within 3s',
    info: null
  }
}

function drift(): TransferServiceInstance {
  return {
    addr: 'cv-transfer-2:9010',
    reachable: true,
    error: null,
    info: {
      ...healthy().info,
      addr: 'cv-transfer-2:9010',
      state: 'degraded',
      state_reason: 'cluster metadata snapshot is 90000ms stale (limit 60000ms)',
      in_memory_jobs: 1,
      // Names a set the rest of the fleet does not know about.
      ring_members: ['cv-transfer-0:9010', 'cv-transfer-2:9010']
    }
  }
}

function stubClient(items: TransferServiceInstance[]): ApiClient {
  return {
    fetchTransferServices: async () => ({ items, configured: RING })
  } as unknown as ApiClient
}

function renderPage(items: TransferServiceInstance[]) {
  return render(
    <MemoryRouter initialEntries={['/transfer-services']}>
      <ApiProvider client={stubClient(items)}>
        <I18nProvider>
          <RefreshProvider>
            <TransferServicesPage />
          </RefreshProvider>
        </I18nProvider>
      </ApiProvider>
    </MemoryRouter>
  )
}

beforeEach(() => {
  window.localStorage.clear()
  window.localStorage.setItem(LOCALE_STORAGE_KEY, 'en')
})

describe('TransferServicesPage', () => {
  it('lists every configured instance, keeping the ones that did not answer', async () => {
    renderPage([healthy(), silent()])

    await waitFor(() =>
      expect(screen.getByTestId('transfer-instance-cv-transfer-0:9010')).toBeInTheDocument()
    )

    const up = screen.getByTestId('transfer-instance-cv-transfer-0:9010')
    expect(up).toHaveTextContent('0.2.2')
    expect(up).toHaveTextContent('sqlite')
    expect(screen.getByTestId('transfer-instance-state-cv-transfer-0:9010')).toHaveTextContent(
      'Normal'
    )

    // Header keys are built by concatenation in the page, so nothing else would
    // catch a missing translation: assert the rendered text, not the key.
    for (const header of ['Instance', 'State', 'Version', 'Ring', 'Cluster snapshot']) {
      expect(screen.getByText(header)).toBeInTheDocument()
    }

    // A silent instance keeps its row: it is the state the page exists to show.
    const down = screen.getByTestId('transfer-instance-cv-transfer-1:9010')
    expect(down).toHaveTextContent('no answer within 3s')
    expect(screen.getByTestId('transfer-instance-state-cv-transfer-1:9010')).toHaveTextContent(
      'Unreachable'
    )
  })

  it('surfaces ring drift against the fleet ring', async () => {
    renderPage([healthy(), drift()])

    await waitFor(() =>
      expect(screen.getByTestId('transfer-instance-cv-transfer-2:9010')).toBeInTheDocument()
    )
    expect(screen.getByTestId('transfer-instance-cv-transfer-0:9010')).toHaveTextContent(
      'matches config'
    )
    expect(screen.getByTestId('transfer-instance-cv-transfer-2:9010')).toHaveTextContent(
      'differs from config'
    )
  })

  it('opens the per-instance detail with limits and ring members', async () => {
    const user = userEvent.setup()
    renderPage([healthy()])

    await waitFor(() =>
      expect(screen.getByTestId('transfer-instance-cv-transfer-0:9010')).toBeInTheDocument()
    )
    await user.click(screen.getByTestId('transfer-instance-cv-transfer-0:9010'))

    await waitFor(() => expect(screen.getByText('Dispatch limits')).toBeInTheDocument())
    expect(screen.getByText('Tasks per worker')).toBeInTheDocument()
    expect(screen.getByText('cv-transfer-1:9010')).toBeInTheDocument()
  })

  it('explains an empty instance list instead of rendering an empty table', async () => {
    renderPage([])
    await waitFor(() =>
      expect(screen.getByText('No transfer instances configured')).toBeInTheDocument()
    )
  })
})