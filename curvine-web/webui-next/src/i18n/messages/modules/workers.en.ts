export default {
  workers: {
    metrics: {
      liveWorkers: 'Live Workers',
      managed: 'Managed',
      totalCapacity: 'Total Capacity',
      lostWorkers: 'Lost Workers',
      healthyHeartbeat: 'healthy heartbeat',
      blacklistDecommission: 'blacklist / decommission',
      activeWorkers: 'active workers',
      usedPercent: '{percent}% used',
      requiresAttention: 'requires attention',
      unhealthy: 'Unhealthy Workers',
      unhealthyMeta: 'cannot accept writes'
    },
    list: {
      title: 'Worker Nodes',
      subtitle: 'Worker status, capacity, heartbeat and management actions',
      searchPlaceholder: 'Search hostname or ip',
      headerWorker: 'Worker',
      headerStatus: 'Status',
      headerHealth: 'Health',
      headerVersion: 'Version',
      headerUsage: 'Usage',
      headerBlocks: 'Blocks',
      headerHeartbeat: 'Heartbeat',
      headerActions: 'Actions',
      rpc: 'RPC',
      empty: 'No workers matched.'
    },
    filters: {
      all: 'All',
      normal: 'Normal',
      cordoned: 'Cordoned',
      decommissioning: 'Decommissioning',
      decommissioned: 'Decommissioned',
      stale: 'Stale',
      lost: 'Lost',
      shutdown: 'Shutdown'
    },
    states: {
      normal: 'Normal',
      cordoned: 'Cordoned',
      decommissioning: 'Decommissioning',
      decommissioned: 'Decommissioned'
    },
    liveness: {
      running: 'Running',
      stale: 'Stale',
      lost: 'Lost',
      shutdown: 'Shutdown'
    },
    health: {
      healthy: 'Healthy',
      suspect: 'Suspect',
      unusable: 'Unusable',
      unhealthy: 'Unhealthy',
      notAllocatable: 'No writes',
      reason: 'Reason',
      since: 'Unhealthy for',
      fault: 'Fault',
      state: 'State'
    },
    actions: {
      detail: 'Detail',
      cordon: 'Cordon',
      uncordon: 'Uncordon',
      decommission: 'Decommission',
      cancelDecommission: 'Cancel drain',
      forceDecommission: 'Force retire',
      recommission: 'Recommission',
      profile: 'Profile',
      confirm: '{action} worker {worker}?',
      forceConfirm: 'Force-retire worker {worker}? Blocks that could not be moved are deleted, and blocks with no other replica become unreadable.',
      forceWarning: 'This deletes the blocks left on the node; the data cannot be restored by recommissioning it.',
      cancelHint: 'The drain stops and the node stays cordoned (no data is touched).',
      decommissionHint: 'Blocks are copied off the node first; the node stays readable until the drain finishes.'
    },
    actionVerb: {
      cordon: 'cordon',
      uncordon: 'uncordon',
      decommission: 'decommission',
      'cancel-decommission': 'cancel drain',
      'force-decommission': 'force retire',
      recommission: 'recommission',
      blacklist: 'cordon',
      allow: 'uncordon'
    },
    detail: {
      title: 'Worker Detail',
      runtime: {
        title: 'Runtime',
        subtitle: 'Address, heartbeat and reported counters',
        hostname: 'Hostname',
        workerId: 'Worker ID',
        rpcAddress: 'RPC Address',
        webPort: 'Web Port',
        ip: 'IP',
        heartbeat: 'Heartbeat',
        version: 'Version',
        versionCommit: 'Commit',
        files: 'Files',
        blocks: 'Blocks',
        notReported: 'Not reported',
        versionMismatch: 'Differs from Active Master version'
      },
      storage: {
        title: 'Storage',
        subtitle: 'Aggregated storage reported by this worker',
        curvineUsed: 'Curvine Used',
        nonFsUsed: 'Non-FS Used',
        reserved: 'Reserved',
        storageDirs: 'Storage Dirs',
        directoriesTitle: 'Storage Directories',
        directoriesSubtitle: 'Per-disk capacity and block distribution',
        type: 'Type',
        empty: 'No storage directories reported.'
      }
    },
    profile: {
      titleWithTarget: 'CPU Flame Graph · {target}',
      title: 'CPU Flame Graph',
      description:
        'Sampling for {seconds}s. The graph appears when the window ends — sampling is synchronous by design.',
      kind: 'Profile type',
      kindCpu: 'CPU',
      kindMemory: 'Memory',
      capabilityUnreachable: 'Could not probe profiling capability on {host} (cross-origin or unreachable); the memory view is unavailable.',
      capabilityUnsupported: 'This process was built without profiling support; the memory view is unavailable.',
      memoryDescription: 'Bytes still held, attributed to the allocating call site. jemalloc samples continuously from startup, so there is no window to wait for.',
      fullscreen: 'Fullscreen',
      exitFullscreen: 'Exit fullscreen',
      duration: 'Duration',
      seconds: '{count}s',
      sampling: 'Sampling, ~{seconds}s…',
      openInNewTab: 'Open in a new tab',
      unsupported: 'This process cannot serve a profile.',
      noWebPort: 'This process did not report a web port, so its profiling endpoint is unreachable.'
    }
  }
}
