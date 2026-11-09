export default {
  transferServices: {
    title: 'Transfer 服务',
    subtitle:
      '按本集群配置的 `transfer.endpoints` 列表，逐个询问每个实例自身的状态。Transfer 没有注册中心，配置列表就是实例列表。',
    refresh: '刷新',
    metrics: {
      instances: '实例数',
      instancesMeta: '配置的 endpoint 数量',
      healthy: '正常',
      healthyMeta: '上报 normal',
      unreachable: '不可达',
      unreachableMeta: '无响应或版本过旧',
      ownedJobs: '本实例承载',
      ownedJobsMeta: '任务状态在本进程内的 job 数',
      detail: '{reachable}/{total} 有响应'
    },
    table: {
      headerInstance: '实例',
      headerState: '状态',
      headerVersion: '版本',
      headerUptime: '运行时长',
      headerJobs: '任务',
      headerTasks: '执行中任务',
      headerStore: '存储后端',
      headerSnapshot: '集群快照',
      headerRing: '哈希环'
    },
    states: {
      normal: '正常',
      degraded: '降级',
      not_ready: '未就绪',
      unreachable: '不可达'
    },
    values: {
      unknown: '—',
      jobsHere: '本机 {owned} · 全库活动 {active}',
      snapshotAges: 'v{version} · {age}',
      snapshotWorkers: '{live} 在线，{capable} 可承载',
      ringMatches: '{count} 个成员 · 与配置一致',
      ringDrift: '{count} 个成员 · 与配置不一致',
      ringUnknown: '未知',
      stale: '已过期'
    },
    detail: {
      title: 'Transfer 实例',
      identity: '标识',
      instanceId: '实例 ID',
      addr: 'RPC 地址',
      webAddr: 'Web 地址',
      state: '状态',
      stateReason: '状态原因',
      version: '版本',
      commit: '提交',
      started: '启动时间',
      uptime: '运行时长',
      store: '元数据存储',
      limits: '调度上限',
      maxRunning: '最大并发 transfer',
      workerTaskLimit: '单 worker 任务上限',
      maxTasksPerTransfer: '单 transfer 任务上限',
      maxPlanEntries: '单次规划条目上限',
      workload: '当前负载',
      activeJobs: '活动 job（全库）',
      executingJobs: '执行中 job（全库）',
      inMemoryJobs: '本实例持有 job',
      inMemoryRunningTasks: '本实例执行中任务',
      snapshot: '集群快照',
      snapshotVersion: '版本',
      snapshotUpdated: '更新时间',
      snapshotStaleness: '已存在时长',
      snapshotWorkers: 'Worker（在线 / 可承载）',
      ring: '放置哈希环',
      ringSelf: '本实例',
      ringMembers: '成员',
      ringDrift:
        '本实例的哈希环与控制台配置的 endpoint 列表不一致。环漂移会让 job 落到与控制台预期不同的实例上。',
      error: '未响应的原因'
    },
    settings: {
      title: 'Transfer 热改配置',
      subtitle:
        '推送到全部已配置实例的运行时开关。立即生效、不落盘：重启后恢复为配置文件里的值。',
      instances: '{reachable}/{total} 个实例有响应',
      unreachable: '无响应（{count} 个）：{addrs}。集群部分可达时默认不做任何修改，需你确认后才会推送。',
      key: '配置项',
      effective: '当前生效值',
      actions: '调整',
      apply: '应用',
      reset: '还原',
      mixed: '不一致',
      hint: '值会推送到 transfer.endpoints 里的每个实例；还原即回到配置文件的值。',
      acceptJobsHint:
        'accept_jobs=false 表示关闭新任务提交（提示"功能临时关闭，请稍后重试"），进程、调度器和已在跑的任务都不受影响。',
      closedBanner: '{count} 个实例已关闭新任务提交（`accept_jobs=false`），已在跑的任务不受影响。',
      closedSubmit: '功能临时关闭，请稍后重试。',
      result: '推送{status}：{applied}/{total} 个实例已生效。',
      status: {
        applied: '成功',
        partial: '部分成功',
        needs_confirmation: '待确认'
      },
      confirmTitle: '部分实例无响应',
      confirmDescription:
        '{total} 个实例中有 {unreachable} 个无法连接：{addrs}。是否仍然把修改应用到可达的实例？',
      confirmWarning: '未应用的实例会保留旧值，直到下一次推送。该操作无法回滚。',
      confirmApply: '仅应用到可达实例'
    },
    empty: {
      title: '未配置 transfer 实例',
      description:
        '当前集群配置里没有 `transfer.endpoints`。单实例部署会回退到 `hostname:rpc_port`。'
    },
    unavailable: '没有 transfer 实例响应。请检查 `transfer.endpoints` 以及 transfer 服务是否在运行。'
  }
}