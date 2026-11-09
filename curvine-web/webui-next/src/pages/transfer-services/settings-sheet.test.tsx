import { render, screen, waitFor } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { beforeEach, describe, expect, it, vi } from 'vitest'

import { ApiProvider } from '@/api/api-context'
import type { ApiClient } from '@/api/client'
import type { TransferConfigData, TransferConfigPushResult } from '@/api/types'
import { I18nProvider, LOCALE_STORAGE_KEY } from '@/i18n'
import { TransferSettingsSheet } from './settings-sheet'

const CONFIG: TransferConfigData = {
  configured: ['cv-transfer-0:9010', 'cv-transfer-1:9010'],
  items: [
    {
      addr: 'cv-transfer-0:9010',
      reachable: true,
      error: null,
      entries: [
        { key: 'accept_jobs', file_value_json: 'true', effective_value_json: 'true' },
        {
          key: 'max_running_transfers',
          file_value_json: '64',
          override_value: '32',
          effective_value_json: '32'
        }
      ]
    },
    {
      addr: 'cv-transfer-1:9010',
      reachable: false,
      error: 'no answer within 3s',
      entries: []
    }
  ]
}

function stubClient(overrides: Partial<ApiClient> = {}): ApiClient {
  return {
    fetchTransferConfig: async () => CONFIG,
    updateTransferConfig: async () => ({ status: 'applied', key: 'accept_jobs', applied: 2, total: 2 }),
    ...overrides
  } as unknown as ApiClient
}

function renderSheet(client: ApiClient) {
  return render(
    <ApiProvider client={client}>
      <I18nProvider>
        <TransferSettingsSheet open onOpenChange={() => {}} onApplied={() => {}} />
      </I18nProvider>
    </ApiProvider>
  )
}

beforeEach(() => {
  window.localStorage.clear()
  window.localStorage.setItem(LOCALE_STORAGE_KEY, 'en')
})

describe('TransferSettingsSheet', () => {
  it('shows the effective value and names the instance that did not answer', async () => {
    renderSheet(stubClient())

    await waitFor(() => expect(screen.getByTestId('transfer-knob-accept_jobs')).toBeInTheDocument())
    expect(screen.getByTestId('transfer-knob-max_running_transfers')).toHaveTextContent('32')
    expect(screen.getByTestId('transfer-settings-unreachable')).toHaveTextContent(
      'cv-transfer-1:9010'
    )
  })

  it('refuses to half-apply a fleet without an explicit confirmation', async () => {
    const user = userEvent.setup()
    type PushBody = {
      key: string
      value?: string
      reset?: boolean
      apply_to_reachable_only?: boolean
    }
    const updateTransferConfig = vi.fn(
      async (_body: PushBody): Promise<TransferConfigPushResult> => ({
        status: 'needs_confirmation',
        key: 'accept_jobs',
        unreachable: ['cv-transfer-1:9010'],
        configured: CONFIG.configured
      })
    )
    renderSheet(stubClient({ updateTransferConfig }))

    await waitFor(() => expect(screen.getByTestId('transfer-knob-accept_jobs')).toBeInTheDocument())
    await user.click(screen.getByTestId('transfer-knob-apply-accept_jobs'))

    // First phase must not pass apply_to_reachable_only: the server writes
    // nothing and answers needs_confirmation.
    await waitFor(() => expect(updateTransferConfig).toHaveBeenCalledTimes(1))
    expect(updateTransferConfig.mock.calls[0][0]).toMatchObject({
      key: 'accept_jobs',
      value: 'true'
    })
    expect(updateTransferConfig.mock.calls[0][0]).not.toHaveProperty('apply_to_reachable_only')

    // The dialog names what would be skipped, and warns that it cannot be undone.
    const confirm = await screen.findByTestId('transfer-settings-confirm')
    const dialog = confirm.closest('[role="alertdialog"]') as HTMLElement
    expect(dialog).toHaveTextContent('cv-transfer-1:9010')
    expect(dialog).toHaveTextContent('There is no rollback')
    await user.click(confirm)

    await waitFor(() => expect(updateTransferConfig).toHaveBeenCalledTimes(2))
    expect(updateTransferConfig.mock.calls[1][0]).toMatchObject({
      key: 'accept_jobs',
      value: 'true',
      apply_to_reachable_only: true
    })
  })
})
