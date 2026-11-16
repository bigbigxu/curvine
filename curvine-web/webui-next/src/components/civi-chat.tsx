import * as React from 'react'
import {
  CircleAlertIcon,
  HistoryIcon,
  Loader2Icon,
  MessageSquarePlusIcon,
  PlayIcon,
  SendIcon,
  Settings2Icon,
  SquareIcon,
  Trash2Icon,
  WrenchIcon,
  XIcon
} from 'lucide-react'

import { AiBotIcon } from '@/components/ai-bot-icon'
import { CiviMarkdown } from '@/components/civi-markdown'

import { fetchCiviSettings, openCiviChat, saveCiviSettings } from '@/api/civi'
import type { CiviSettings, CiviSettingsPayload, CiviSkill, CiviToolInfo } from '@/api/types'
import { Badge } from '@/components/ui/badge'
import { Button } from '@/components/ui/button'
import { Input } from '@/components/ui/input'
import { Label } from '@/components/ui/label'
import { Popover, PopoverContent, PopoverTrigger } from '@/components/ui/popover'
import {
  Sheet,
  SheetContent,
  SheetDescription,
  SheetHeader,
  SheetTitle
} from '@/components/ui/sheet'
import { Spinner } from '@/components/ui/spinner'
import { Switch } from '@/components/ui/switch'
import { Textarea } from '@/components/ui/textarea'
import { useI18n } from '@/i18n'
import { PANEL_FALLBACK_CLASS, PANEL_SHARE, useContentWidth } from '@/lib/use-content-width'
import { cn } from '@/lib/utils'
import { useOptionalCluster } from '@/cluster/cluster-context'

/** Payload the backend accepts for a fresh conversation turn. */
interface NewChatBody {
  messages: Array<{ role: string; content: string }>
  cluster?: string
  /** Console locale (`en` / `zh-CN`) — the backend injects the reply language. */
  locale?: string
}

/** One tool invocation rendered as a compact step row. */
interface CiviStep {
  name: string
  /** Aggregated tools dispatch on an action, shown next to the tool name. */
  action?: string
  status: 'running' | 'done'
  ok: boolean | null
  preview: string
}

/** A write-tool call parked until the user approves or rejects it. */
interface CiviConfirm {
  callId: string
  name: string
  args: unknown
  status: 'pending' | 'approved' | 'rejected'
}

interface CiviMessage {
  /** Stable identity so streaming updates patch the right bubble. */
  id: string
  role: 'user' | 'assistant'
  content: string
  steps: CiviStep[]
  confirms: CiviConfirm[]
  status: 'running' | 'done'
}

/** A saved conversation; titles come from the first user message. */
interface CiviSession {
  id: string
  title: string
  createdAt: number
  messages: CiviMessage[]
}

let civiMessageSeq = 0
function nextMessageId(): string {
  civiMessageSeq += 1
  return `civi-msg-${civiMessageSeq}`
}

const MAX_SESSIONS = 20

function sessionTitle(messages: CiviMessage[]): string {
  const first = messages.find((m) => m.role === 'user')?.content ?? ''
  return first.length > 40 ? `${first.slice(0, 40)}…` : first
}

const SUGGESTIONS: Record<'en' | 'zh-CN', string[]> = {
  en: [
    'What is the current cluster capacity and usage?',
    'Show me the lost workers, if any',
    'What data exists under the root directory?',
    'Are there any active client sessions?'
  ],
  'zh-CN': [
    '当前集群的容量和使用率是多少？',
    '有哪些丢失的 worker？',
    '根目录下有哪些数据？',
    '当前有哪些活跃的客户端会话？'
  ]
}

/**
 * Follow-up chips rendered under the latest answer. Each tool leaves a
 * natural next question; the rest fills up from the generic pool. The model
 * is not consulted — these are cheap, deterministic hints.
 */
const FOLLOW_UPS: Record<'en' | 'zh-CN', Record<string, string[]>> = {
  en: {
    _generic: [
      'Show the lost workers, if any',
      'Summarize the storage capacity and usage',
      'What data lives under the root directory?',
      'Are there any zombie client sessions?',
      'Review the cluster configuration highlights'
    ],
    'fs_insight/list_dir': [
      'What is inside the largest directory?',
      'How many files and directories does the cluster hold?',
      'List another directory for me'
    ],
    'fs_insight/stat': [
      'Show the block locations of this file',
      'What is the TTL and replica count of this path?',
      'Summarize the usage of its parent directory'
    ],
    'fs_insight/trash_list': [
      'Restore the most recent deleted path from trash',
      'What happens if we empty the trash now?'
    ],
    'cluster_insight/overview': [
      'Compare per-worker capacity and usage',
      'Which workers are close to full?',
      'What is growing fastest in the filesystem?'
    ],
    'cluster_insight/config': [
      'Which configuration values look risky?',
      'Summarize the master configuration'
    ],
    'cluster_insight/observability': [
      'How did the throughput change over the last hour?',
      'Show the current RPC latency',
      'Summarize the JVM memory usage'
    ],
    'mount_insight/list': [
      'Validate the UFS path of one mount',
      'Which mounts are read-only?',
      'Show the details of the first mount'
    ],
    'job_insight/list': [
      'Show the status of the most recent job',
      'Are there any failed jobs?',
      'List the running transfer jobs'
    ],
    'worker_insight/list': [
      'Compare per-worker capacity and usage',
      'Which workers are close to full?',
      'What blocks does the busiest worker hold?'
    ],
    'worker_insight/detail': [
      'How many blocks does this worker hold?',
      'Should we decommission this worker?',
      'Compare it against the other workers'
    ]
  },
  'zh-CN': {
    _generic: [
      '展示当前丢失的 worker（如有）',
      '总结一下集群的容量和使用率',
      '根目录下有哪些数据？',
      '有没有僵尸挂载的客户端会话？',
      '巡检一下集群配置的关键项'
    ],
    'fs_insight/list_dir': [
      '看看最大的目录里有什么',
      '集群里现在有多少文件和目录？',
      '帮我再列一个目录'
    ],
    'fs_insight/stat': [
      '看看这个文件的块分布',
      '这个路径的 TTL 和副本数是多少？',
      '汇总一下它父目录的使用情况'
    ],
    'fs_insight/trash_list': [
      '从回收站恢复最近删除的路径',
      '现在清空回收站会有什么影响？'
    ],
    'cluster_insight/overview': [
      '对比各 worker 的容量和使用情况',
      '哪些 worker 磁盘快满了？',
      '文件系统里哪块数据增长最快？'
    ],
    'cluster_insight/config': [
      '哪些配置项看起来有风险？',
      '总结一下 master 的关键配置'
    ],
    'cluster_insight/observability': [
      '过去一小时吞吐有什么变化？',
      '当前 RPC 延迟如何？',
      '总结一下 JVM 内存使用'
    ],
    'mount_insight/list': [
      '校验某个挂载的 UFS 路径',
      '哪些挂载是只读的？',
      '看看第一个挂载的详情'
    ],
    'job_insight/list': [
      '看看最近一个任务的状态',
      '有失败的任务吗？',
      '列出运行中的 transfer 任务'
    ],
    'worker_insight/list': [
      '对比各 worker 的容量和使用情况',
      '哪些 worker 磁盘快满了？',
      '最繁忙的 worker 上有哪些块？'
    ],
    'worker_insight/detail': [
      '这个 worker 上有多少块？',
      '要不要把这个 worker 下线（decommission）？',
      '把它和其他 worker 对比一下'
    ]
  }
}

/** Picks 4 follow-ups: contextual by the tool+action used, then generic. */
function pickFollowUps(
  locale: 'en' | 'zh-CN',
  lastAnswer: CiviMessage | undefined,
  count = 4
): string[] {
  const pools = FOLLOW_UPS[locale] ?? FOLLOW_UPS.en
  const used = lastAnswer?.steps
    .filter((s) => s.status === 'done')
    .map((s) => `${s.name}/${s.action ?? '_'}`) ?? []
  const picked: string[] = []
  const push = (candidates: string[]) => {
    for (const c of candidates) {
      if (picked.length >= count) return
      if (!picked.includes(c)) picked.push(c)
    }
  }
  // Steps used in this answer first, in reverse call order (latest = most relevant).
  for (const key of [...used].reverse()) {
    if (picked.length >= count) break
    push(pools[key] ?? [])
  }
  push(pools._generic ?? [])
  return picked
}

/** Small labelled settings row — mirrors the legacy CIVI panel layout. */
function SettingsField({
  label,
  hint,
  children
}: {
  label: string
  hint?: React.ReactNode
  children: React.ReactNode
}) {
  return (
    <div className='space-y-1.5'>
      <Label className='text-xs'>
        {label}
        {hint ? <span className='ms-1 font-normal text-muted-foreground'>{hint}</span> : null}
      </Label>
      {children}
    </div>
  )
}

function CiviSettingsPanel({
  payload,
  saving,
  error,
  onSave,
  onChange
}: {
  payload: CiviSettingsPayload
  saving: boolean
  error: string
  onSave: () => void
  onChange: (next: CiviSettings) => void
}) {
  const { t } = useI18n()
  const settings = payload.settings
  const update = (patch: Partial<CiviSettings>) => onChange({ ...settings, ...patch })

  return (
    <div className='min-h-0 flex-1 space-y-4 overflow-y-auto p-4'>
      {error ? (
        <p className='rounded-md border border-destructive/40 bg-destructive/10 px-3 py-2 text-xs text-destructive'>
          {error}
        </p>
      ) : null}

      <div className='flex items-center justify-between rounded-lg border px-3 py-2.5'>
        <span className='text-sm font-medium'>{t('civi.enabled')}</span>
        <div className='flex items-center gap-2'>
          <span className='text-xs text-muted-foreground'>
            {settings.enabled ? t('civi.enabledOn') : t('civi.enabledOff')}
          </span>
          <Switch
            checked={settings.enabled}
            aria-label={t('civi.enabled')}
            onCheckedChange={(checked) => update({ enabled: checked })}
          />
        </div>
      </div>

      <SettingsField label={t('civi.endpoint')}>
        <Input
          value={settings.endpoint}
          placeholder='https://api.deepseek.com/v1/chat/completions'
          onChange={(e) => update({ endpoint: e.target.value })}
        />
      </SettingsField>

      <SettingsField
        label={t('civi.apiKey')}
        hint={payload.api_key_set ? t('civi.apiKeySavedHint') : undefined}
      >
        <Input
          type='password'
          value={settings.api_key}
          placeholder='sk-...'
          onChange={(e) => update({ api_key: e.target.value })}
        />
      </SettingsField>

      <SettingsField label={t('civi.model')}>
        <Input
          value={settings.model}
          placeholder='deepseek-chat'
          onChange={(e) => update({ model: e.target.value })}
        />
      </SettingsField>

      <div className='grid grid-cols-3 gap-3'>
        <SettingsField label={t('civi.temperature')}>
          <Input
            type='number'
            step='0.1'
            min='0'
            max='2'
            value={settings.temperature}
            onChange={(e) => update({ temperature: Number(e.target.value) })}
          />
        </SettingsField>
        <SettingsField label={t('civi.maxIterations')}>
          <Input
            type='number'
            min='1'
            max='16'
            value={settings.max_iterations}
            onChange={(e) => update({ max_iterations: Number(e.target.value) })}
          />
        </SettingsField>
        <SettingsField label={t('civi.rateLimit')}>
          <Input
            type='number'
            min='0'
            value={settings.rate_limit_per_min}
            onChange={(e) => update({ rate_limit_per_min: Number(e.target.value) })}
          />
        </SettingsField>
      </div>

      <SettingsField
        label={t('civi.apiToken')}
        hint={payload.settings.api_token ? t('civi.apiKeySavedHint') : undefined}
      >
        <Input
          type='password'
          value={settings.api_token}
          placeholder='X-CIVI-Token'
          onChange={(e) => update({ api_token: e.target.value })}
        />
        <p className='text-[11px] text-muted-foreground'>{t('civi.apiTokenHint')}</p>
      </SettingsField>

      <SettingsField label={t('civi.allowedTools')}>
        <div className='space-y-2'>
          {payload.available_tools.map((tool: CiviToolInfo) => (
            <label
              key={tool.name}
              title={tool.description}
              className='flex cursor-pointer items-center gap-2 text-sm'
            >
              <input
                type='checkbox'
                className='size-3.5 accent-primary'
                checked={settings.enabled_tools.includes(tool.name)}
                onChange={(e) => {
                  const next = e.target.checked
                    ? [...settings.enabled_tools, tool.name]
                    : settings.enabled_tools.filter((name) => name !== tool.name)
                  update({ enabled_tools: next })
                }}
              />
              <span className='font-mono text-[13px]'>{tool.name}</span>
              <Badge variant={tool.perm === 'write' ? 'destructive' : 'secondary'}>
                {tool.perm}
              </Badge>
              {tool.perm === 'write' ? (
                <span className='text-[11px] text-muted-foreground'>
                  {t('civi.needsConfirmation')}
                </span>
              ) : null}
            </label>
          ))}
        </div>
      </SettingsField>

      <SettingsField label={t('civi.mcpServers')}>
        <div className='space-y-2'>
          {settings.mcp_servers.map((srv, i) => (
            <div key={i} className='flex items-center gap-2'>
              <Input
                className='w-28 shrink-0'
                value={srv.name}
                placeholder={t('civi.mcpNamePlaceholder')}
                onChange={(e) => {
                  const next = [...settings.mcp_servers]
                  next[i] = { ...srv, name: e.target.value }
                  update({ mcp_servers: next })
                }}
              />
              <Input
                value={srv.url}
                placeholder={t('civi.mcpUrlPlaceholder')}
                onChange={(e) => {
                  const next = [...settings.mcp_servers]
                  next[i] = { ...srv, url: e.target.value }
                  update({ mcp_servers: next })
                }}
              />
              <Button
                variant='ghost'
                size='icon-sm'
                aria-label={t('common.close')}
                onClick={() =>
                  update({
                    mcp_servers: settings.mcp_servers.filter((_, index) => index !== i)
                  })
                }
              >
                <XIcon className='size-4' />
              </Button>
            </div>
          ))}
          <Button
            variant='outline'
            size='sm'
            onClick={() =>
              update({ mcp_servers: [...settings.mcp_servers, { name: '', url: '' }] })
            }
          >
            {t('civi.mcpAdd')}
          </Button>
        </div>
      </SettingsField>

      <Button className='w-full' disabled={saving} onClick={onSave}>
        {saving ? <Spinner className='size-4' /> : null}
        {saving ? t('civi.saving') : t('civi.save')}
      </Button>
    </div>
  )
}

/** One tool execution step: spinner while running, ✓/✗ once resolved. */
function StepRow({ step }: { step: CiviStep }) {
  const { t } = useI18n()
  return (
    <div className='flex items-baseline gap-1.5 text-xs'>
      {step.status === 'running' ? (
        <Loader2Icon className='size-3 shrink-0 animate-spin text-muted-foreground' />
      ) : step.ok ? (
        <span className='text-emerald-600'>✓</span>
      ) : (
        <span className='text-destructive'>✗</span>
      )}
      <span className='inline-flex items-center gap-1 font-medium text-foreground/80'>
        <WrenchIcon className='size-3' />
        {step.name}
        {step.action ? <span className='text-muted-foreground'>·{step.action}</span> : null}
      </span>
      {step.preview ? (
        <span className='max-w-44 truncate text-muted-foreground' title={step.preview}>
          {step.preview}
        </span>
      ) : (
        <span className='sr-only'>{t('civi.toolStep')}</span>
      )}
    </div>
  )
}

/** Write-tool confirmation card (approval flow). */
function ConfirmCard({
  confirm,
  sending,
  onResolve
}: {
  confirm: CiviConfirm
  sending: boolean
  onResolve: (callId: string, approve: boolean) => void
}) {
  const { t } = useI18n()
  return (
    <div className='rounded-lg border border-destructive/40 bg-destructive/5 p-2.5 text-xs'>
      <p className='font-semibold text-destructive'>⚠ {t('civi.confirmTitle')}</p>
      <p className='mt-1 font-mono text-[12px]'>{confirm.name}</p>
      <pre className='mt-1 max-h-36 overflow-auto rounded-md border bg-background/60 p-2 font-mono text-[11px]'>
        {JSON.stringify(confirm.args, null, 2)}
      </pre>
      <div className='mt-2 flex items-center gap-2'>
        {confirm.status === 'pending' ? (
          <>
            <Button
              variant='destructive'
              size='sm'
              disabled={sending}
              onClick={() => onResolve(confirm.callId, true)}
            >
              {t('civi.approve')}
            </Button>
            <Button
              variant='outline'
              size='sm'
              disabled={sending}
              onClick={() => onResolve(confirm.callId, false)}
            >
              {t('civi.reject')}
            </Button>
          </>
        ) : (
          <Badge variant={confirm.status === 'approved' ? 'default' : 'secondary'}>
            {confirm.status === 'approved' ? t('civi.approved') : t('civi.rejected')}
          </Badge>
        )}
      </div>
    </div>
  )
}

/**
 * CIVI — the AI assistant drawer.
 *
 * Port of the legacy Vue `CiviChat.vue` onto the React console: floating
 * action button, streaming chat drawer with tool-step visualisation, write
 * tool confirmation cards, skill shortcuts and the settings form. All state
 * talks to `/api/ai/*` through `api/civi.ts`; the backend keeps API keys
 * server-side, so the browser only ever handles the optional shared token.
 */
export function CiviChat() {
  const { t, locale } = useI18n()
  const cluster = useOptionalCluster()
  // Same panel sizing as the worker detail sheet: 80% of the console content
  // box, so a chat with Markdown tables has room to breathe.
  const contentWidth = useContentWidth()

  const [open, setOpen] = React.useState(false)
  const [showSettings, setShowSettings] = React.useState(false)
  const [payload, setPayload] = React.useState<CiviSettingsPayload | null>(null)
  /** `false` once the backend proved it does not serve `/api/ai/*`. */
  const [available, setAvailable] = React.useState(true)
  const [savingSettings, setSavingSettings] = React.useState(false)
  const [settingsError, setSettingsError] = React.useState('')
  const [messages, setMessages] = React.useState<CiviMessage[]>([])
  const [draft, setDraft] = React.useState('')
  const [sending, setSending] = React.useState(false)
  const [error, setError] = React.useState('')
  const abortRef = React.useRef<AbortController | null>(null)
  const msgAreaRef = React.useRef<HTMLDivElement>(null)

  // Conversations persist per selected cluster in localStorage as a session
  // list; switching the top-bar cluster swaps the transcript (the model
  // context is cluster-bound). `activeId` names the session being viewed; a
  // session entry is created on the first saved turn.
  const storageKey = `civi-sessions:${cluster?.clusterId || 'default'}`
  const loadedKeyRef = React.useRef<string | null>(null)
  const [sessions, setSessions] = React.useState<CiviSession[]>([])
  const [activeId, setActiveId] = React.useState<string | null>(null)

  React.useEffect(() => {
    // CIVI is optional: an unavailable router simply keeps the button silent.
    fetchCiviSettings().then((data) => {
      if (data) setPayload(data)
      else setAvailable(false)
    })
    return () => abortRef.current?.abort()
  }, [])

  React.useEffect(() => {
    if (loadedKeyRef.current === storageKey) return
    loadedKeyRef.current = storageKey
    try {
      const raw = localStorage.getItem(storageKey)
      const restored: CiviSession[] = raw ? JSON.parse(raw) : []
      const list = Array.isArray(restored) ? restored : []
      setSessions(list)
      // Reopen the newest session, if any.
      const last = list[list.length - 1]
      if (last) {
        setActiveId(last.id)
        setMessages(last.messages)
      } else {
        setActiveId(null)
        setMessages([])
      }
    } catch {
      setSessions([])
      setActiveId(null)
      setMessages([])
    }
  }, [storageKey])

  React.useEffect(() => {
    if (loadedKeyRef.current !== storageKey) return
    if (!activeId) return
    try {
      // Persist only settled turns, capped, so localStorage never grows
      // unbounded.
      const done = messages.filter((m) => m.status === 'done').slice(-60)
      setSessions((prev) => {
        const existing = prev.find((s) => s.id === activeId)
        let next: CiviSession[]
        if (existing) {
          next = prev.map((s) =>
            s.id === activeId ? { ...s, messages: done, title: sessionTitle(done) || s.title } : s
          )
        } else {
          // Skip empty sessions: no turn has been saved yet.
          if (done.length === 0) return prev
          next = [
            ...prev,
            {
              id: activeId,
              title: sessionTitle(done) || '…',
              createdAt: Date.now(),
              messages: done
            }
          ]
        }
        next = next.slice(-MAX_SESSIONS)
        localStorage.setItem(storageKey, JSON.stringify(next))
        return next
      })
    } catch {
      // Storage full / unavailable — history is best effort.
    }
  }, [messages, activeId, storageKey])

  const openSession = (id: string) => {
    if (sending) return
    const session = sessions.find((s) => s.id === id)
    if (!session) return
    stop()
    setActiveId(id)
    setMessages(session.messages)
    setError('')
    setShowSettings(false)
  }

  const deleteSession = (id: string) => {
    if (sending) return
    setSessions((prev) => {
      const next = prev.filter((s) => s.id !== id)
      try {
        localStorage.setItem(storageKey, JSON.stringify(next))
      } catch {
        // ignore
      }
      return next
    })
    if (activeId === id) {
      setActiveId(null)
      setMessages([])
    }
  }

  const newChat = () => {
    stop()
    // The current session is already persisted (if it had turns); a fresh id
    // means the next turn starts a new entry.
    setActiveId(nextMessageId())
    setMessages([])
    setError('')
    setShowSettings(false)
  }

  // Keep the newest message in view while streaming.
  React.useEffect(() => {
    const el = msgAreaRef.current
    if (el) el.scrollTop = el.scrollHeight
  }, [messages])

  const settings = payload?.settings ?? null
  const enabled = settings?.enabled ?? false

  const finishRunningMessage = () => {
    setMessages((prev) => {
      const last = prev[prev.length - 1]
      if (!last || last.role !== 'assistant' || last.status !== 'running') return prev
      return [...prev.slice(0, -1), { ...last, status: 'done' }]
    })
  }

  const stop = () => {
    abortRef.current?.abort()
    abortRef.current = null
    setSending(false)
    finishRunningMessage()
  }

  /** Runs one chat request and applies its SSE events to the open message. */
  const streamChat = async (
    body: NewChatBody | Record<string, unknown>,
    botId: string
  ) => {
    const payloadBody: Record<string, unknown> = { ...body }
    const applyEvent = (name: string, data: Record<string, unknown>) => {
      setMessages((prev) =>
        prev.map((msg) => {
          if (msg.id !== botId) return msg
          if (name === 'delta') {
            return { ...msg, content: msg.content + String(data.content ?? '') }
          }
          if (name === 'tool_call') {
            return {
              ...msg,
              steps: [
                ...msg.steps,
                {
                  name: String(data.name),
                  action:
                    data.args && typeof data.args === 'object'
                      ? String((data.args as Record<string, unknown>).action ?? '')
                      : '',
                  status: 'running',
                  ok: null,
                  preview: ''
                }
              ]
            }
          }
          if (name === 'tool_result') {
            const steps = [...msg.steps]
            for (let i = steps.length - 1; i >= 0; i--) {
              const step = steps[i]
              if (step.name === data.name && step.status === 'running') {
                steps[i] = {
                  ...step,
                  ok: !!data.ok,
                  status: 'done',
                  preview: String(data.preview ?? '').slice(0, 120)
                }
                break
              }
            }
            return { ...msg, steps }
          }
          if (name === 'confirm_required') {
            return {
              ...msg,
              confirms: [
                ...msg.confirms,
                {
                  callId: String(data.call_id),
                  name: String(data.name),
                  args: data.args,
                  status: 'pending'
                }
              ]
            }
          }
          return msg
        })
      )
      if (name === 'done') finishRunningMessage()
      if (name === 'error') {
        setError(String(data.message ?? ''))
        finishRunningMessage()
      }
    }

    const resp = await openCiviChat(payloadBody, abortRef.current!.signal)
    if (!resp.ok || !resp.body) {
      const bodyJson = (await resp.json().catch(() => ({}))) as { message?: string }
      let message = bodyJson.message || `HTTP ${resp.status}`
      if (resp.status === 401) message += t('civi.tokenHint')
      throw new Error(message)
    }

    // SSE framing: events are separated by a blank line; `data:` lines are
    // concatenated exactly like the legacy reader did.
    const reader = resp.body.getReader()
    const decoder = new TextDecoder()
    let buffer = ''
    for (;;) {
      const { done, value } = await reader.read()
      if (done) break
      buffer += decoder.decode(value, { stream: true })
      let idx: number
      while ((idx = buffer.indexOf('\n\n')) >= 0) {
        const rawEvent = buffer.slice(0, idx)
        buffer = buffer.slice(idx + 2)
        let name = 'message'
        let data = ''
        for (const line of rawEvent.split('\n')) {
          if (line.startsWith('event:')) name = line.slice(6).trim()
          else if (line.startsWith('data:')) data += line.slice(5).trim()
        }
        if (!data) continue
        try {
          applyEvent(name, JSON.parse(data))
        } catch {
          // Ignore malformed events.
        }
      }
    }
  }

  const runChat = async (body: NewChatBody | Record<string, unknown>, botId: string) => {
    setMessages((prev) => [
      ...prev,
      {
        id: botId,
        role: 'assistant',
        content: '',
        steps: [],
        confirms: [],
        status: 'running'
      }
    ])
    setSending(true)
    setError('')
    abortRef.current = new AbortController()
    try {
      await streamChat(body, botId)
    } catch (e) {
      if ((e as Error).name !== 'AbortError') {
        setError((e as Error).message)
      }
    } finally {
      finishRunningMessage()
      setSending(false)
      abortRef.current = null
    }
  }

  const send = (preset?: string) => {
    const text = (preset ?? draft).trim()
    if (!text || sending) return
    if (!payload) return
    if (!enabled) {
      setShowSettings(true)
      return
    }
    setDraft('')
    const history = messages
      .filter((m) => m.status !== 'running')
      .map((m) => ({ role: m.role, content: m.content }))
    setMessages((prev) => [
      ...prev,
      { id: nextMessageId(), role: 'user', content: text, steps: [], confirms: [], status: 'done' }
    ])
    const body: NewChatBody = {
      messages: [...history, { role: 'user', content: text }],
      // Language preference from the console locale switcher.
      locale
    }
    // Follow the cluster selected in the top bar; the backend resolves it
    // against its registry (empty/undefined = default cluster).
    const clusterId = cluster?.clusterId
    if (clusterId) body.cluster = clusterId
    void runChat(body, nextMessageId())
  }

  const resume = (callId: string, approve: boolean) => {
    if (sending) return
    // Mark the card resolved, then continue the paused conversation.
    setMessages((prev) =>
      prev.map((m) => ({
        ...m,
        confirms: m.confirms.map((c) =>
          c.callId === callId && c.status === 'pending'
            ? { ...c, status: approve ? 'approved' : 'rejected' }
            : c
        )
      }))
    )
    void runChat({ resume_call_id: callId, approve }, nextMessageId())
  }

  const saveSettings = async () => {
    if (!payload) return
    setSavingSettings(true)
    setSettingsError('')
    try {
      const token = payload.settings.api_token.trim()
      let saved: CiviSettingsPayload | null = null
      try {
        saved = await saveCiviSettings(payload.settings)
        setPayload(saved)
        setShowSettings(false)
      } catch (e) {
        const message = (e as Error).message
        // The token gate answers 401: persist a freshly typed token and retry
        // once before surfacing the failure.
        if (message.toLowerCase().includes('unauthorized') && token && token !== '***') {
          localStorage.setItem('civi_token', token)
          saved = await saveCiviSettings(payload.settings)
          setPayload(saved)
          setShowSettings(false)
        } else {
          setSettingsError(t('civi.saveFailed', { message }))
        }
      }
      // A typed token also sticks after a plain successful save so later
      // requests (chat retries, MCP gating) carry it.
      if (saved && token && token !== '***') {
        localStorage.setItem('civi_token', token)
      }
    } finally {
      setSavingSettings(false)
    }
  }

  const skills: CiviSkill[] = payload?.skills ?? []

  return (
    <>
      {available && !open ? (
        <Button
          size='icon'
          aria-label={t('civi.fab')}
          title={t('civi.fab')}
          className={cn(
            'fixed right-6 bottom-6 z-50 size-14 rounded-full shadow-lg',
            'transition-transform hover:scale-105'
          )}
          onClick={() => setOpen(true)}
        >
          <AiBotIcon className='size-6!' />
        </Button>
      ) : null}

      <Sheet open={open} onOpenChange={setOpen}>
        <SheetContent
          showCloseButton={false}
          className={cn(
            'flex w-full flex-col gap-0 p-0 sm:max-w-none',
            // Same sizing as the worker detail sheet: 80% of the console
            // content box, with a fallback until the shell is measured.
            contentWidth === null && PANEL_FALLBACK_CLASS
          )}
          style={contentWidth === null ? undefined : { width: contentWidth * PANEL_SHARE }}
        >
          <SheetHeader className='border-b px-4 py-3'>
            <div className='flex items-center justify-between'>
              <div className='flex items-center gap-2.5'>
                <span className='flex size-9 items-center justify-center rounded-full bg-primary/10 text-primary'>
                  <AiBotIcon className='size-5' />
                </span>
                <div>
                  <SheetTitle className='text-base leading-tight'>{t('civi.title')}</SheetTitle>
                  <SheetDescription className='text-[11px]'>
                    {t('civi.subtitle')}
                  </SheetDescription>
                </div>
              </div>
              <div className='flex items-center gap-1'>
                <Popover>
                  <PopoverTrigger asChild>
                    <Button
                      variant='ghost'
                      size='icon-sm'
                      aria-label={t('civi.history')}
                      title={t('civi.history')}
                    >
                      <HistoryIcon className='size-4' />
                    </Button>
                  </PopoverTrigger>
                  <PopoverContent align='end' className='w-72 p-1.5'>
                    <p className='px-2 py-1.5 text-xs font-medium text-muted-foreground'>
                      {t('civi.history')}
                    </p>
                    {sessions.length === 0 ? (
                      <p className='px-2 py-3 text-xs text-muted-foreground'>
                        {t('civi.emptyHistory')}
                      </p>
                    ) : (
                      <div className='max-h-72 overflow-y-auto'>
                        {[...sessions].reverse().map((session) => (
                          <div
                            key={session.id}
                            className={cn(
                              'group flex items-center gap-1 rounded-md px-2 py-1.5 hover:bg-muted',
                              session.id === activeId && 'bg-muted/70'
                            )}
                          >
                            <button
                              className='min-w-0 flex-1 text-left'
                              onClick={() => openSession(session.id)}
                            >
                              <span className='block truncate text-[13px]'>{session.title}</span>
                              <span className='block text-[11px] text-muted-foreground'>
                                {new Date(session.createdAt).toLocaleString(locale)}
                              </span>
                            </button>
                            <Button
                              variant='ghost'
                              size='icon-xs'
                              aria-label={t('civi.delete')}
                              title={t('civi.delete')}
                              className='opacity-0 group-hover:opacity-100'
                              onClick={() => deleteSession(session.id)}
                            >
                              <Trash2Icon className='size-3.5 text-destructive' />
                            </Button>
                          </div>
                        ))}
                      </div>
                    )}
                  </PopoverContent>
                </Popover>
                <Button
                  variant='ghost'
                  size='icon-sm'
                  aria-label={t('civi.newChat')}
                  title={t('civi.newChat')}
                  onClick={newChat}
                >
                  <MessageSquarePlusIcon className='size-4' />
                </Button>
                <Button
                  variant='ghost'
                  size='icon-sm'
                  aria-label={t('civi.settings')}
                  title={t('civi.settings')}
                  onClick={() => setShowSettings((v) => !v)}
                >
                  <Settings2Icon className='size-4' />
                </Button>
                <Button
                  variant='ghost'
                  size='icon-sm'
                  aria-label={t('civi.close')}
                  title={t('civi.close')}
                  onClick={() => setOpen(false)}
                >
                  <XIcon className='size-4' />
                </Button>
              </div>
            </div>
          </SheetHeader>

          {!payload ? (
            <div className='flex min-h-0 flex-1 items-center justify-center'>
              <Spinner className='size-6' />
            </div>
          ) : showSettings ? (
            <CiviSettingsPanel
              payload={payload}
              saving={savingSettings}
              error={settingsError}
              onSave={() => void saveSettings()}
              onChange={(next) =>
                setPayload((prev) => (prev ? { ...prev, settings: next } : prev))
              }
            />
          ) : (
            <div className='flex min-h-0 flex-1 flex-col'>
              {!enabled ? (
                <div className='mx-4 mt-4 rounded-lg border border-amber-500/40 bg-amber-500/10 px-3 py-2 text-xs text-amber-700 dark:text-amber-400'>
                  {t('civi.disabledNotice')}
                </div>
              ) : null}

              <div ref={msgAreaRef} className='min-h-0 flex-1 space-y-3 overflow-y-auto p-4'>
                {messages.length === 0 ? (
                  <div className='space-y-2'>
                    {skills.length > 0 ? (
                      <p className='text-xs text-muted-foreground'>{t('civi.suggestTitle')}</p>
                    ) : null}
                    {skills.map((skill) => (
                      <div
                        key={skill.id}
                        title={locale === 'zh-CN' ? skill.prompt_zh : skill.prompt}
                        className='flex items-center gap-2 rounded-lg border bg-card px-3 py-2 text-sm hover:border-primary'
                      >
                        <span className='min-w-0 flex-1 truncate'>
                          {locale === 'zh-CN' ? skill.title_zh : skill.title}
                          <span className='ms-1 text-[11px] text-muted-foreground'>
                            {locale === 'zh-CN' ? skill.title : skill.title_zh}
                          </span>
                        </span>
                        <Button
                          size='sm'
                          variant='secondary'
                          className='h-6 px-2 text-[11px]'
                          onClick={() => send(locale === 'zh-CN' ? skill.prompt_zh : skill.prompt)}
                        >
                          <PlayIcon className='size-3' />
                          {t('civi.run')}
                        </Button>
                      </div>
                    ))}
                    <p className='pt-1 text-xs text-muted-foreground'>{t('civi.askTitle')}</p>
                    {(SUGGESTIONS[locale] ?? SUGGESTIONS.en).map((q) => (
                      <button
                        key={q}
                        className='block w-full rounded-lg border bg-card px-3 py-2 text-left text-sm hover:border-primary hover:text-primary'
                        onClick={() => send(q)}
                      >
                        {q}
                      </button>
                    ))}
                  </div>
                ) : null}

                {messages.map((msg, i) =>
                  msg.role === 'user' ? (
                    <div
                      key={i}
                      className='ms-auto max-w-[92%] rounded-lg bg-primary px-3 py-2 text-sm whitespace-pre-wrap text-primary-foreground'
                    >
                      {msg.content}
                    </div>
                  ) : (
                    <div
                      key={i}
                      className='me-auto max-w-[92%] space-y-2 rounded-lg border bg-card px-3 py-2 text-sm'
                    >
                      {msg.steps.length > 0 ? (
                        <div className='space-y-1'>
                          {msg.steps.map((step, j) => (
                            <StepRow key={j} step={step} />
                          ))}
                        </div>
                      ) : null}
                      {msg.confirms.map((c) => (
                        <ConfirmCard
                          key={c.callId}
                          confirm={c}
                          sending={sending}
                          onResolve={resume}
                        />
                      ))}
                      {msg.content ? <CiviMarkdown content={msg.content} /> : null}
                      {msg.status === 'running' &&
                      !msg.content &&
                      msg.steps.length === 0 &&
                      msg.confirms.length === 0 ? (
                        <p className='text-xs text-muted-foreground italic'>{t('civi.thinking')}</p>
                      ) : null}
                    </div>
                  )
                )}

                {error ? (
                  <div className='flex items-start gap-2 rounded-lg border border-destructive/40 bg-destructive/10 px-3 py-2 text-xs text-destructive'>
                    <CircleAlertIcon className='mt-0.5 size-3.5 shrink-0' />
                    <span className='break-words'>{error}</span>
                  </div>
                ) : null}

                {/* Follow-up hints under the latest settled answer: contextual
                    to the tools the answer used, then generic guidance. */}
                {!sending && messages.length > 0
                  ? (() => {
                      const last = messages[messages.length - 1]
                      if (
                        last.role !== 'assistant' ||
                        last.status !== 'done' ||
                        !last.content
                      )
                        return null
                      const followUps = pickFollowUps(locale, last)
                      if (followUps.length === 0) return null
                      return (
                        <div className='space-y-1.5 pt-1'>
                          {followUps.map((q) => (
                            <button
                              key={q}
                              className='block w-full rounded-lg border border-dashed bg-transparent px-3 py-2 text-left text-[13px] text-muted-foreground hover:border-primary hover:text-primary'
                              onClick={() => send(q)}
                            >
                              {q}
                            </button>
                          ))}
                        </div>
                      )
                    })()
                  : null}
              </div>

              <div className='flex items-end gap-2 border-t bg-card p-3'>
                <Textarea
                  value={draft}
                  rows={2}
                  className='min-h-0 flex-1 resize-none text-sm'
                  placeholder={t('civi.placeholder')}
                  disabled={sending}
                  onChange={(e) => setDraft(e.target.value)}
                  onKeyDown={(e) => {
                    if (e.key === 'Enter' && !e.shiftKey && !e.nativeEvent.isComposing) {
                      e.preventDefault()
                      send()
                    }
                  }}
                />
                {sending ? (
                  <Button
                    variant='destructive'
                    size='icon'
                    className='size-10'
                    aria-label={t('civi.stop')}
                    title={t('civi.stop')}
                    onClick={stop}
                  >
                    <SquareIcon className='size-4' />
                  </Button>
                ) : (
                  <Button
                    size='icon'
                    className='size-10'
                    aria-label={t('civi.send')}
                    title={t('civi.send')}
                    disabled={!draft.trim()}
                    onClick={() => send()}
                  >
                    <SendIcon className='size-4' />
                  </Button>
                )}
              </div>
            </div>
          )}
        </SheetContent>
      </Sheet>
    </>
  )
}
