export default {
  transferServices: {
    title: 'Transfer Services',
    subtitle:
      'Every instance in this cluster\'s `transfer.endpoints` list, asked for its own state. Transfer has no registry, so the configured list is the instance list.',
    refresh: 'Refresh',
    metrics: {
      instances: 'Instances',
      instancesMeta: 'configured endpoints',
      healthy: 'Healthy',
      healthyMeta: 'reporting normal',
      unreachable: 'Unreachable',
      unreachableMeta: 'no answer or older build',
      ownedJobs: 'Jobs here',
      ownedJobsMeta: 'task state lives in these instances',
      detail: '{reachable} of {total} answering'
    },
    table: {
      headerInstance: 'Instance',
      headerState: 'State',
      headerVersion: 'Version',
      headerUptime: 'Uptime',
      headerJobs: 'Jobs',
      headerTasks: 'Running tasks',
      headerStore: 'Store',
      headerSnapshot: 'Cluster snapshot',
      headerRing: 'Ring'
    },
    states: {
      normal: 'Normal',
      degraded: 'Degraded',
      not_ready: 'Not ready',
      unreachable: 'Unreachable'
    },
    values: {
      unknown: '—',
      jobsHere: '{owned} here · {active} active in store',
      snapshotAges: 'v{version} · {age}',
      snapshotWorkers: '{live} live, {capable} transfer-capable',
      ringMatches: '{count} members · matches config',
      ringDrift: '{count} members · differs from config',
      ringUnknown: 'unknown',
      stale: 'stale'
    },
    detail: {
      title: 'Transfer instance',
      identity: 'Identity',
      instanceId: 'Instance ID',
      addr: 'RPC address',
      webAddr: 'Web address',
      state: 'State',
      stateReason: 'State reason',
      version: 'Version',
      commit: 'Commit',
      started: 'Started',
      uptime: 'Uptime',
      store: 'Metadata store',
      limits: 'Dispatch limits',
      maxRunning: 'Max running transfers',
      workerTaskLimit: 'Tasks per worker',
      maxTasksPerTransfer: 'Tasks per transfer',
      maxPlanEntries: 'Entries per planning pass',
      workload: 'Current load',
      activeJobs: 'Active jobs (store)',
      executingJobs: 'Executing jobs (store)',
      inMemoryJobs: 'Jobs owned here',
      inMemoryRunningTasks: 'Running tasks here',
      snapshot: 'Cluster snapshot',
      snapshotVersion: 'Version',
      snapshotUpdated: 'Updated',
      snapshotStaleness: 'Age',
      snapshotWorkers: 'Workers (live / capable)',
      ring: 'Placement ring',
      ringSelf: 'This instance',
      ringMembers: 'Members',
      ringDrift:
        'This instance\'s ring differs from the configured endpoints list. A drifted ring places jobs on different instances than the console expects.',
      error: 'Why it did not answer'
    },
    settings: {
      title: 'Transfer settings',
      subtitle:
        'Runtime knobs, pushed to every configured instance. Values apply immediately and are not persisted: a restart restores the file value.',
      instances: '{reachable} of {total} instance(s) answering',
      unreachable: 'Not answering ({count}): {addrs}. Nothing is applied to a partially reachable cluster unless you confirm.',
      key: 'Key',
      effective: 'Effective value',
      actions: 'Change',
      apply: 'Apply',
      reset: 'Reset',
      mixed: 'Mixed',
      hint: 'Values are pushed to every instance in transfer.endpoints; the file value is what a reset returns to.',
      bytesHint:
        'worker_inflight_budget_bytes takes a size (10GB, 512MB, 1.5GB, or a bare byte count). It is the only admission ceiling the scheduler dispatches against.',
      acceptJobsHint:
        'accept_jobs=false closes the service to new submissions with "temporarily closed, retry later". The process, its scheduler and running jobs are untouched.',
      closedBanner: '{count} instance(s) are closed to new submissions (`accept_jobs=false`). In-flight jobs keep running.',
      closedSubmit: 'The transfer service is temporarily closed; please retry later.',
      result: 'Push {status}: {applied}/{total} instance(s) applied.',
      status: {
        applied: 'applied',
        partial: 'partially applied',
        needs_confirmation: 'needs confirmation'
      },
      confirmTitle: 'Some instances did not answer',
      confirmDescription:
        '{unreachable} of {total} instance(s) cannot be reached: {addrs}. Apply the change to the reachable instances anyway?',
      confirmWarning:
        'Instances that are not applied keep their previous value until the next push. There is no rollback.',
      confirmApply: 'Apply to reachable instances'
    },
    empty: {
      title: 'No transfer instances configured',
      description:
        'This cluster configuration has no `transfer.endpoints` entry. A single-instance deployment falls back to `hostname:rpc_port`.'
    },
    unavailable: 'No transfer instance answered. Check `transfer.endpoints` and that the transfer services are running.'
  }
}