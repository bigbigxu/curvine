import { describe, expect, it } from 'vitest'

import type { TransferServiceInstance } from '@/api/types'
import {
  aggregatedConfigValue,
  formatDurationMs,
  isTransferClosedError,
  instanceState,
  ringDrift,
  serviceStateTone,
  shortCommit,
  sortInstances,
  summarizeInstances
} from './transfer-services'

function instance(
  overrides: Partial<TransferServiceInstance> = {}
): TransferServiceInstance {
  return {
    addr: 'cv-transfer-0:9010',
    reachable: true,
    error: null,
    info: {
      role: 'transfer',
      addr: 'cv-transfer-0:9010',
      ready: true,
      state: 'normal',
      in_memory_jobs: 2,
      ring_members: ['cv-transfer-0:9010', 'cv-transfer-1:9010']
    },
    ...overrides
  }
}

describe('instanceState', () => {
  it('reads the instance-reported state', () => {
    expect(instanceState(instance())).toBe('normal')
    expect(instanceState(instance({ info: { state: 'degraded' } }))).toBe('degraded')
    expect(instanceState(instance({ info: { state: 'NOT_READY' } }))).toBe('not_ready')
  })

  it('treats a silent instance as unreachable rather than missing', () => {
    expect(instanceState({ addr: 'a:1', reachable: false, error: 'timeout' })).toBe('unreachable')
    expect(instanceState({ addr: 'a:1', reachable: true, info: null })).toBe('unreachable')
  })

  it('falls back to the ready flag for an instance that predates the state field', () => {
    expect(instanceState(instance({ info: { ready: false } }))).toBe('not_ready')
    expect(instanceState(instance({ info: { ready: true } }))).toBe('normal')
  })
})

describe('serviceStateTone', () => {
  it('maps every state to a distinct tone', () => {
    expect(serviceStateTone('normal')).toBe('positive')
    expect(serviceStateTone('degraded')).toBe('warning')
    expect(serviceStateTone('not_ready')).toBe('negative')
    expect(serviceStateTone('unreachable')).toBe('negative')
  })
})

describe('sortInstances', () => {
  it('surfaces problems first', () => {
    const sorted = sortInstances([
      instance(),
      instance({ addr: 'b:1', info: { state: 'degraded' } }),
      instance({ addr: 'c:1', reachable: false }),
      instance({ addr: 'd:1', info: { state: 'not_ready' } })
    ])
    expect(sorted.map((item) => item.addr)).toEqual(['c:1', 'd:1', 'b:1', 'cv-transfer-0:9010'])
  })
})

describe('formatDurationMs', () => {
  it('picks the largest sensible unit', () => {
    expect(formatDurationMs(42_000)).toBe('42s')
    expect(formatDurationMs(12 * 60_000)).toBe('12m')
    expect(formatDurationMs(5 * 3_600_000 + 12 * 60_000)).toBe('5h 12m')
    expect(formatDurationMs(3 * 86_400_000 + 4 * 3_600_000)).toBe('3d 4h')
  })

  it('renders unknown values as an em dash instead of NaN', () => {
    expect(formatDurationMs(undefined)).toBe('—')
    expect(formatDurationMs(-1)).toBe('—')
  })
})

describe('shortCommit', () => {
  it('shortens a sha and passes other values through', () => {
    expect(shortCommit('0123456789abcdef0123456789abcdef01234567')).toBe('0123456')
    expect(shortCommit('unknown')).toBe('')
    expect(shortCommit('v1.2.3')).toBe('v1.2.3')
  })
})

describe('ringDrift', () => {
  const configured = ['cv-transfer-0:9010', 'cv-transfer-1:9010']

  it('matches regardless of order', () => {
    expect(ringDrift({ ring_members: [...configured].reverse() }, configured)).toBe(false)
  })

  it('flags a ring that names a different instance set', () => {
    expect(ringDrift({ ring_members: ['cv-transfer-0:9010'] }, configured)).toBe(true)
    expect(
      ringDrift({ ring_members: ['cv-transfer-0:9010', 'cv-transfer-2:9010'] }, configured)
    ).toBe(true)
  })

  it('stays quiet when either side is unknown', () => {
    expect(ringDrift({}, configured)).toBe(false)
    expect(ringDrift({ ring_members: configured }, [])).toBe(false)
    expect(ringDrift(null, configured)).toBe(false)
  })
})

describe('summarizeInstances', () => {
  it('counts fleet health and this-cluster load', () => {
    const summary = summarizeInstances([
      instance(),
      instance({ addr: 'b:1', info: { state: 'degraded', in_memory_jobs: 3 } }),
      instance({ addr: 'c:1', reachable: false, info: null })
    ])
    expect(summary).toEqual({
      total: 3,
      reachable: 2,
      healthy: 1,
      unreachable: 1,
      ownedJobs: 5
    })
  })
})
describe('aggregatedConfigValue', () => {
  it('reports one value when every instance agrees', () => {
    const entries = [
      { key: 'max_running_transfers', effective_value_json: '32' },
      { key: 'max_running_transfers', effective_value_json: '32' }
    ]
    expect(aggregatedConfigValue(entries, 'max_running_transfers')).toEqual({
      value: '32',
      mixed: false
    })
  })

  it('reports mixed after a partially applied push instead of picking one', () => {
    const entries = [
      { key: 'max_running_transfers', effective_value_json: '32' },
      { key: 'max_running_transfers', effective_value_json: '64' }
    ]
    expect(aggregatedConfigValue(entries, 'max_running_transfers')).toEqual({
      value: undefined,
      mixed: true
    })
  })

  it('reports nothing when no instance answered', () => {
    expect(aggregatedConfigValue([], 'accept_jobs')).toEqual({ mixed: false })
  })
})

describe('isTransferClosedError', () => {
  it('recognises the backend brake message', () => {
    expect(
      isTransferClosedError(
        'transfer service temporarily closed: new submissions are rejected (accept_jobs=false); retry later'
      )
    ).toBe(true)
    expect(isTransferClosedError('source path is required')).toBe(false)
  })
})
