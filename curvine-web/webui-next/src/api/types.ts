/**
 * Response/domain types mirrored from the curvine-web backend handlers.
 *
 * Field names match the serde serialisation of the Rust structs exactly; most
 * are optional because the backend omits fields until the cluster reports them.
 */

export interface AuthSession {
  authenticated: boolean
  user?: string | null
  [key: string]: unknown
}

export interface LoginPayload {
  username: string
  password: string
}

export interface OverviewData {
  cluster_id?: string | null
  master_addr?: string | null
  active_master?: string | null
  active_master_display?: string | null
  master_state?: string | null
  version?: string | null
  version_commit?: string | null
  start_time?: string | null
  start_time_ms?: number | null
  local_hostname?: string | null
  /** Host of the active master's web port (serves `/metrics` and profiling). */
  master_metrics_host?: string | null
  /** Active master's `web_port`; not derivable from `master_addr` (RPC port). */
  master_web_port?: number | null
  capacity?: number | null
  fs_used?: number | null
  available?: number | null
  reserved_bytes?: number | null
  files_total?: number | null
  dir_total?: number | null
  block_total?: number | null
  live_workers?: number | null
  lost_workers?: number | null
  [key: string]: unknown
}

export interface MasterHaNode {
  addr: string
  display_addr?: string | null
  journal_addr?: string | null
  role?: string | null
  version?: string | null
  start_time?: string | null
  reachable?: boolean
  reachable_error?: string | null
  current?: boolean
  switchable?: boolean
  raft_id?: number | null
  /** Role reported by the master itself; null on builds without self-report. */
  self_reported_role?: string | null
}

export interface MasterHaData {
  nodes?: MasterHaNode[]
  active_master?: string | null
  active_master_configured?: string | null
  active_master_display?: string | null
  current_master?: string | null
  configured_master?: string | null
  master_state?: string | null
  failover_supported?: boolean
  active_raft_id?: number | null
  local_hostname?: string | null
  /** Set when masters self-report a role that disagrees with the console's
   * cluster config (e.g. stale pod IPs after recreation). */
  warning?: string | null
}

export interface WorkerAddress {
  hostname?: string | null
  ip_addr?: string | null
  rpc_port?: number | null
  web_port?: number | null
  worker_id?: number | null
}

export interface WorkerStorageInfo {
  storage_id: string
  dir_path?: string | null
  storage_type?: string | null
  capacity?: number | null
  available?: number | null
  block_num?: number | null
  failed?: boolean
  /** Health view reported by the worker. `failed` stays for older peers. */
  health_state?: 'Healthy' | 'Suspect' | 'Unusable' | string | null
  fault_kind?: string | null
  unhealthy_since_ms?: number | null
}

export interface WorkerInfo {
  address?: WorkerAddress
  hostname?: string | null
  display_ip?: string | null
  ip_addr?: string | null
  worker_id?: number | null
  capacity?: number | null
  available?: number | null
  fs_used?: number | null
  non_fs_used?: number | null
  reserved_bytes?: number | null
  block_num?: number | null
  last_update?: string | number | null
  last_update_ms?: string | number | null
  software_version?: string | null
  version?: string | null
  storage_map?: Record<string, WorkerStorageInfo> | null
  status?: string | null
  /**
   * The single state of the worker: `normal` | `readonly` | `decommissioning` |
   * `decommissioned` | `stale` | `lost` | `shutdown`. Preferred over the two
   * axes below, which stay available for explaining *why*.
   */
  state?: string | null
  /** CV-60 state axes: operator intent and heartbeat fact, kept separate. */
  worker_state?: WorkerState | null
  /** Worker fact: can it take new writes at all? */
  accepting_allocations?: boolean | null
  /** Master decision: does the scheduler route new blocks here? */
  schedulable?: boolean | null
  [key: string]: unknown
}

export interface WorkerState {
  /** The derived single state (`normal` | `readonly` | `stale` | ...). */
  state?: string | null
  /** normal | cordoned | decommissioning | decommissioned */
  admin?: string | null
  /** running | stale | lost | shutdown */
  liveness?: string | null
  writable?: boolean | null
  readable?: boolean | null
}

export interface WorkersData {
  /** Flat, deduplicated list carrying both state axes (preferred). */
  workers?: WorkerInfo[]
  /** Fleet counts per axis. */
  buckets?: {
    admin?: Record<string, number>
    liveness?: Record<string, number>
  }
  /** Legacy buckets, kept for old masters; still how an old response is shaped. */
  live_workers?: WorkerInfo[]
  blacklist_workers?: WorkerInfo[]
  decommission_workers?: WorkerInfo[]
  lost_workers?: WorkerInfo[]
  total?: number
}

export type WorkerAction =
  | 'cordon'
  | 'uncordon'
  | 'decommission'
  | 'cancel-decommission'
  | 'force-decommission'
  | 'recommission'
  // Legacy names still accepted by the master; kept here for deep links.
  | 'blacklist'
  | 'allow'

export interface WorkerDetailData {
  worker?: WorkerInfo
  state?: string | null
  version?: string | null
  version_commit?: string | null
  file_count?: number | null
}

export type ConfigValue = string | number | boolean | null | ConfigValue[] | { [key: string]: ConfigValue }

/**
 * Where a configuration leaf's current value came from.
 *
 * `derived` is not a file value: the loader computes it after deserialization
 * (the legacy `master.web_port` split, a hostname resolved from `net_interface`,
 * the synthesized `client.master_addrs`). `runtime` means a process-local
 * override is active — it is not persisted and disappears on restart.
 */
export type ConfigSource = 'default' | 'file' | 'env' | 'cli' | 'derived' | 'runtime'

/** One hot-tunable knob as the owning process currently sees it. */
export interface ConfigRuntimeEntry {
  /** Registry key, e.g. `master.dir_space_enabled`. Not a configuration path. */
  key: string
  /** Configuration path the key backs, e.g. `master.dir_space.enabled`. */
  path: string
  file_value: ConfigValue
  override_value?: ConfigValue
  effective_value: ConfigValue
}

export interface ConfigData {
  role?: string
  node?: string
  values: ConfigValue
  sources?: Record<string, ConfigSource>
  runtime?: ConfigRuntimeEntry[]
  /** Registry keys that may be changed through `POST /api/update_config`. */
  writable?: string[]
  /**
   * Registered keys whose configuration path could not be resolved in the
   * published tree. Non-empty means the backend and the configuration schema
   * have drifted; those keys are excluded from every other field.
   */
  unresolved?: string[]
  /** Whether hot-tunable changes survive a restart. Currently always false. */
  persistent?: boolean
}

/** Body of `POST /api/v1/config`. */
export interface ConfigUpdate {
  /** Registry key, e.g. `master.dir_space_enabled`. */
  key: string
  /** `null` clears the active override and restores the file-backed value. */
  value: string | null
}

/** What `POST /api/v1/config` reports after a push or a reset. */
export interface ConfigUpdateResult {
  key: string
  previous_value?: ConfigValue
  value?: ConfigValue
  effective_value?: ConfigValue
  reset?: boolean
  persistent?: boolean
}

export interface BrowseEntry {
  is_dir?: boolean
  name?: string | null
  path?: string | null
  replicas?: number | null
  mode?: number | null
  owner?: string | null
  group?: string | null
  nlink?: number | null
  block_size?: number | null
  len?: number | null
  mtime?: number | null
  atime?: number | null
  storage_policy?: {
    storage_type?: string | null
    ttl_ms?: number | null
    ttl_action?: string | number | null
    state?: string | number | null
  } | null
  [key: string]: unknown
}

export interface BrowseData {
  path?: string
  entries?: BrowseEntry[]
  items?: BrowseEntry[]
  /** Name of the last entry returned; feed back as `start_after` for the next page. */
  next_start_after?: string | null
  /** The backend filled the requested window, so older entries may exist. */
  has_more?: boolean
  [key: string]: unknown
}

export interface BlockLocationData {
  [key: string]: unknown
}

export interface UfsSyncJob {
  job_id?: string
  state?: string
  [key: string]: unknown
}

export interface DirSpaceState {
  enabled: boolean
  paths: string[]
}

export interface DirSpaceStatus {
  enabled: boolean
  scanning: boolean
  in_round: boolean
  phase: 'disabled' | 'idle' | 'scanning' | 'in_round' | 'completed' | string
  scope: string
  current_path: string
  last_outcome: string
  last_finished_ms: number
  next_scan_at_ms: number
  paths_total: number
  paths_done: number
}

export interface DirSpaceUpdate {
  enabled?: boolean
  path?: string
  tracked?: boolean
}

export interface DirSpaceSummary {
  path: string
  /** `failed` carries `error`; stale caches may still yield the legacy states. */
  state: 'ready' | 'pending' | 'off' | 'failed' | string
  logical_bytes?: number
  directory_count?: number
  file_count?: number
  /** Populated only with `state: 'failed'`: why this path could not be probed. */
  error?: string
}

export interface MountInfo {
  mount_id?: string | number | null
  cv_path: string
  ufs_path?: string | null
  /** 1/'FsMode' or 0/'CacheMode'. */
  write_type?: number | string | null
  ttl_ms?: number | null
  /** 0/'Mtime' (default) or 1/'Atime'. */
  ttl_type?: number | string | null
  provider?: string | null
  properties?: Record<string, string> | null
  replicas?: number | null
  block_size?: number | string | null
  storage_type?: string | null
  read_verify_ufs?: boolean | null
  auto_cache?: boolean | null
  /** 0/'ReadOnly' or 1/'ReadWrite'. */
  access_mode?: number | string | null
}

export interface JobTask {
  task_id?: string
  id?: string
  state?: string
  status?: string
  source_path?: string
  target_path?: string
  worker?: string
  message?: string
  progress?: { message?: string } | null
  update_time_ms?: number
  retry_count?: number
  retries?: number
  attempts?: number
  [key: string]: unknown
}

export interface JobInfo {
  job_id?: string
  id?: string
  state?: string
  status?: string
  done?: boolean
  path?: string
  source_path?: string
  target_path?: string
  source_type?: string
  type?: string
  submitter?: string
  created_by?: string
  trigger?: string
  trigger_event?: string
  mount_path?: string
  ufs_path?: string
  message?: string
  progress?: number
  total_files?: number
  completed_files?: number
  failed_files?: number
  failed_tasks?: number
  running_files?: number
  loading_files?: number
  pending_files?: number
  loaded_size?: number
  total_size?: number
  create_time_ms?: number
  update_time_ms?: number
  updated_at?: number
  tasks?: JobTask[]
  [key: string]: unknown
}

/**
 * Where a job ran, why it ran there, and how often it had to be retried.
 *
 * All three retry counters are job-level and answer different questions:
 * `takeover_count` is the previous owner dying, `planning_attempts` a failed
 * planning pass, `mount_snapshot_attempts` a missing mount snapshot. Task-level
 * retries are not part of this object.
 */
export interface TransferOwnership {
  /** Instance the placement ring assigned the job to. */
  placement_addr?: string | null
  /** Instance currently holding the lease, i.e. running it. */
  owner_addr?: string | null
  /** Idempotency key and placement hash key chosen by the submitter. */
  client_request_id?: string | null
  takeover_count?: number | null
  mount_snapshot_attempts?: number | null
  planning_attempts?: number | null
  overwrite?: boolean | null
  replicas?: number | null
}

export interface TransferInfo extends JobInfo {
  ownership?: TransferOwnership | null
  /** 'load' (UFS → Curvine) or 'export' (Curvine → UFS). */
  kind?: string | null
  tenant?: string | null
  run_id?: string | number | null
  completed_size?: number | null
  /** True when terminal task details were TTL-purged while summary counts remain. */
  task_details_expired?: boolean
  /** Present when more tasks exist than the requested page size. */
  next_page_token?: string | null
}

/**
 * One transfer instance's answer to "how are you".
 *
 * Field names mirror the RPC `TransferServiceInfoResponse` and the instance's own
 * `GET /api/v1/transfer/info` verbatim: the console serializes the RPC message
 * as-is, so there is exactly one shape to keep in sync.
 */
export interface TransferServiceInfo {
  role?: string
  /** Process-unique lease owner id; changes on restart, unlike `addr`. */
  instance_id?: string
  addr?: string
  web_addr?: string
  version?: string
  version_commit?: string
  start_time_ms?: number
  uptime_ms?: number
  ready?: boolean
  /** `normal` | `degraded` | `not_ready`. */
  state?: string
  state_reason?: string
  store_backend?: string
  max_running_transfers?: number
  /** False means the service is up but closed to new submissions. */
  accepts_jobs?: boolean | null
  worker_task_limit?: number
  max_tasks_per_transfer?: number
  max_plan_entries?: number
  /** Store-wide, identical on every instance sharing the store. */
  active_jobs?: number
  executing_jobs?: number
  /** Jobs whose file-level task state lives in this process. */
  in_memory_jobs?: number
  in_memory_running_tasks?: number
  snapshot_version?: number
  snapshot_updated_at?: number
  snapshot_staleness_ms?: number
  snapshot_live_workers?: number
  snapshot_capable_workers?: number
  ring_self?: string
  ring_members?: string[]
}

/**
 * One row per configured `transfer.endpoints` address. `info` is null when the
 * instance did not answer; `error` then says why. The row stays visible so a dead
 * HA member cannot hide behind the ones that are up.
 */
export interface TransferServiceInstance {
  addr: string
  reachable: boolean
  error?: string | null
  info?: TransferServiceInfo | null
}

/** One writable knob's value on one instance. */
export interface TransferConfigEntry {
  key: string
  file_value_json: string
  override_value?: string | null
  effective_value_json: string
}

/** One instance's answer to "what are your live knobs". */
export interface TransferConfigSnapshot {
  addr: string
  reachable: boolean
  error?: string | null
  entries?: TransferConfigEntry[]
}

export interface TransferConfigData {
  items?: TransferConfigSnapshot[]
  configured?: string[]
}

/** Result of pushing one knob to one instance. */
export interface TransferConfigPushItem {
  addr: string
  reachable: boolean
  applied: boolean
  error?: string | null
  previous_value_json?: string | null
  effective_value_json?: string | null
}

export interface TransferConfigPushResult {
  /** `applied` | `partial` | `needs_confirmation`. */
  status: 'applied' | 'partial' | 'needs_confirmation'
  key: string
  applied?: number
  total?: number
  items?: TransferConfigPushItem[]
  /** Present on `needs_confirmation`: instances that did not answer. */
  unreachable?: string[]
  configured?: string[]
}

export interface TransferServicesData {
  items?: TransferServiceInstance[]
  /**
   * This console's own `transfer.endpoints`. Comparing it with each instance's
   * `ring_members` is what turns a silent placement-config drift into a visible
   * warning: transfer has no registry to reconcile the two.
   */
  configured?: string[]
}

/**
 * Per-bucket job counts over the whole store, so the tabs and the summary cards
 * stay accurate while the table itself is paged.
 */
export interface TransferListCounts {
  all?: number
  active?: number
  completed?: number
  failed?: number
  canceled?: number
}

export interface TransferListData {
  items?: TransferInfo[]
  page?: number
  page_size?: number
  total?: number
  counts?: TransferListCounts
}

export interface ObservabilityHealth {
  enabled: boolean
  mode: string
  active_provider: string
  prometheus_configured?: boolean
  provider_health?: string | null
  fallback_reason?: string | null
  prometheus_url?: string | null
  scrape_interval_ms: number
  retention_ms: number
  series_count: number
  sample_count: number
  last_discovery_ms?: number | null
  last_collect_ms?: number | null
}

export interface ObservabilitySettings {
  scrape_interval_ms: number
  retention_ms: number
  scrape_interval_options_ms: number[]
  retention_options_ms: number[]
  max_retention_ms: number
  max_samples_per_series_cap: number
  max_total_samples: number
  max_series: number
  estimated_peak_samples: number
}

export interface TargetHealth {
  id: string
  component: string
  instance: string
  url: string
  worker_id?: number | null
  worker_state?: string | null
  active_master?: string | null
  up: boolean
  last_scrape_ms?: number | null
  last_success_ms?: number | null
  last_scrape_duration_ms?: number | null
  last_error: string
  last_warning: string
  sample_count: number
  parse_error_count: number
}

export interface InstantMetric {
  metric: string
  labels: Record<string, string>
  timestamp_ms: number
  value: number
}

export interface RangeSeries {
  metric: string
  labels: Record<string, string>
  points: [number, number][]
}

export interface MetricQueryParams {
  metric?: string
  component?: string
  instance?: string
  range?: string
  step?: string
}

export interface DashboardQueryParams {
  range?: string
  step?: string
}

export interface SeriesSummary {
  current?: number | null
  min?: number | null
  max?: number | null
  avg?: number | null
}

export interface DashboardSeries {
  name: string
  labels: Record<string, string>
  points: [number, number][]
  summary?: SeriesSummary | null
}

export type PanelKind = 'timeseries' | 'table' | 'instant'

export interface DashboardTableRow {
  id: string
  component: string
  instance: string
  scrape_up: boolean
  worker_id?: number | null
  worker_state?: string | null
  status: string
  detail?: string
}

export interface DashboardPanel {
  id: string
  title: string
  subtitle: string
  kind: PanelKind
  series: DashboardSeries[]
  rows?: DashboardTableRow[]
  section?: string | null
  error?: string | null
}

export interface DashboardGroup {
  id: string
  title: string
  panels: DashboardPanel[]
}

export interface DashboardCard {
  id: string
  title: string
  value: string
  unit?: string | null
  tone?: string | null
  meta?: string | null
}

export interface DashboardWindow {
  range: string
  step: string
  start_ms: number
  end_ms: number
  last_collect_ms?: number | null
}

export interface ObservabilityDashboard {
  window: DashboardWindow
  cards: DashboardCard[]
  groups: DashboardGroup[]
}

export type DiagnosticStatus = 'pass' | 'warning' | 'fail' | 'skipped'

export interface DiagnosticCheck {
  id: string
  title: string
  status: DiagnosticStatus
  summary: string
  detail?: string
  suggested_fix?: string
}

export interface MissingTarget {
  id: string
  source: string
  component: string
  instance: string
  url: string
  reason: string
}

export interface DiagnosticsPayload {
  generated_ms: number
  active_provider: string
  overall_status: DiagnosticStatus
  checks: DiagnosticCheck[]
  discovered_master_targets: number
  discovered_live_worker_targets: number
  discovered_transfer_targets: number
  prometheus_covered_targets: number
  active_covered_targets: number
  expected_targets: number
  missing_targets: MissingTarget[]
}

export type AlertSeverity = 'critical' | 'warning'

export interface OperationsAlert {
  id: string
  severity: AlertSeverity
  title: string
  summary: string
  source: string
  target_id?: string | null
  suggested_fix: string
}

export interface OperationsPayload {
  generated_ms: number
  diagnostics: DiagnosticsPayload
  alerts: OperationsAlert[]
}

/**
 * One cluster the console can target, as `GET /api/v1/clusters` reports it.
 *
 * `default` is the cluster the process itself is configured with (always present,
 * never editable or removable); `file` entries come from the clusters directory
 * and can be updated/deleted. Credentials are deliberately absent — the backend
 * projects them away before answering.
 */
export type ClusterStatus = 'ready' | 'invalid' | 'unreachable'

export interface ClusterSummary {
  id: string
  display_name: string
  source: 'default' | 'file'
  master_addrs: string[]
  status: ClusterStatus
  error?: string | null
  editable: boolean
  removable: boolean
}

/** Dry-run answer for a TOML payload: parsed, but not persisted. */
export interface ClusterValidation {
  ok: boolean
  cluster?: ClusterSummary | null
  errors: string[]
  warnings: string[]
}

/** Connectivity probe of one cluster's master, used before switching to it. */
export interface ClusterProbe {
  reachable: boolean
  active_master?: string | null
  version?: string | null
  latency_ms?: number | null
  message?: string | null
}

// ---------------------------------------------------------------------------
// CIVI — the AI assistant served by the web backend (`/api/ai/*`).
//
// Secrets are masked by the server: `api_key` / `api_token` come back as `***`
// and posting the mask keeps the stored value. The optional shared token the
// user types is kept in localStorage (`civi_token`) and sent as `X-CIVI-Token`.
// ---------------------------------------------------------------------------

/** An external MCP server whose tools merge into the agent toolset. */
export interface CiviMcpServer {
  name: string
  url: string
}

export interface CiviSettings {
  enabled: boolean
  endpoint: string
  api_key: string
  model: string
  temperature: number
  max_iterations: number
  rate_limit_per_min: number
  api_token: string
  enabled_tools: string[]
  mcp_servers: CiviMcpServer[]
}

/** One whitelisted tool with its permission tier and current switch state. */
export interface CiviToolInfo {
  name: string
  description: string
  perm: string
  enabled: boolean
}

/** Curated one-click prompt surfaced as a chip in the chat panel. */
export interface CiviSkill {
  id: string
  title: string
  title_zh: string
  prompt: string
  /** Chinese variant; the console picks by locale. */
  prompt_zh: string
  tools: string[]
}

export interface CiviSettingsPayload {
  settings: CiviSettings
  api_key_set: boolean
  available_tools: CiviToolInfo[]
  skills: CiviSkill[]
}
