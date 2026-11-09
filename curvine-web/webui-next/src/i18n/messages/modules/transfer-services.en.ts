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
    empty: {
      title: 'No transfer instances configured',
      description:
        'This cluster configuration has no `transfer.endpoints` entry. A single-instance deployment falls back to `hostname:rpc_port`.'
    },
    unavailable: 'No transfer instance answered. Check `transfer.endpoints` and that the transfer services are running.'
  }
}