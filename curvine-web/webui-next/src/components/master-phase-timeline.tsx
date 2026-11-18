// Copyright 2025 OPPO.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

import { Check, X } from 'lucide-react'
import { cn } from '@/lib/utils'
import type { MasterPhaseInfo } from '@/api/types'

/** One stop on the master's boot/recovery pipeline. */
export type PhaseStageState = 'done' | 'current' | 'pending' | 'problem'

export interface PhaseStage {
  id: 'starting' | 'replaying' | 'catching-up' | 'serving'
  state: PhaseStageState
}

export interface PhaseTimeline {
  stages: PhaseStage[]
  /** `null` when the master did not report a phase (unreachable or older build). */
  phase: string | null
  /** Recovery takes the stage labels; a standby shows "备" on the last stop. */
  standby: boolean
  /** Safe mode / quarantine / exiting: red cross instead of a spinner. */
  problem: boolean
}

const STAGE_IDS: PhaseStage['id'][] = ['starting', 'replaying', 'catching-up', 'serving']

function stagesUpTo(index: number): PhaseStage[] {
  return STAGE_IDS.map((id, position) => ({
    id,
    state: position < index ? 'done' : position === index ? 'current' : 'pending'
  }))
}

/**
 * Map a master phase onto the four visible stops.
 *
 * The pipeline is the one an operator waits on: process up, journal replayed,
 * committed tail applied, serving. A standby is a healthy steady state that is
 * not serving, so it keeps the first three done and leaves the last hollow.
 */
export function buildPhaseTimeline(phase: string | null | undefined): PhaseTimeline {
  const normalized = (phase || '').trim()

  if (!normalized) {
    return { stages: stagesUpTo(-1), phase: null, standby: false, problem: false }
  }

  const problem = normalized === 'SafeMode' || normalized === 'Quarantined' || normalized === 'Exiting'
  if (problem) {
    // Keep whatever is already durable: a safe-mode master has a usable
    // namespace prefix, but it is not serving and needs a human.
    const done = normalized === 'Exiting' ? 0 : 2
    const stages = stagesUpTo(done).map((stage, position) =>
      position === done ? { ...stage, state: 'problem' as PhaseStageState } : stage
    )
    return { stages, phase: normalized, standby: false, problem: true }
  }

  switch (normalized) {
    case 'Starting':
      return { stages: stagesUpTo(0), phase: normalized, standby: false, problem: false }
    case 'Replaying':
      return { stages: stagesUpTo(1), phase: normalized, standby: false, problem: false }
    case 'CatchingUp':
      return { stages: stagesUpTo(2), phase: normalized, standby: false, problem: false }
    case 'Serving':
      return { stages: stagesUpTo(4), phase: normalized, standby: false, problem: false }
    case 'StandingBy':
      // Standby: first three done, no check on "serving" because it does not.
      return { stages: stagesUpTo(3), phase: normalized, standby: true, problem: false }
    default:
      return { stages: stagesUpTo(-1), phase: normalized, standby: false, problem: false }
  }
}

const STAGE_LABEL_KEY: Record<PhaseStage['id'], string> = {
  starting: 'starting',
  replaying: 'replaying',
  'catching-up': 'catchingUp',
  serving: 'serving'
}

const PHASE_LABEL_KEY: Record<string, string> = {
  Starting: 'starting',
  Replaying: 'replaying',
  CatchingUp: 'catchingUp',
  Serving: 'serving',
  StandingBy: 'standingBy',
  Quarantined: 'quarantined',
  SafeMode: 'safeMode',
  Exiting: 'exiting'
}

/** Short human label for the current phase; `-` when nothing was reported. */
export function phaseLabel(t: (key: string) => string, phase: string | null | undefined): string {
  const key = PHASE_LABEL_KEY[(phase || '').trim()]
  return key ? t(`overview.phase.${key}`) : '-'
}

function StageDot({ stage, standby }: { stage: PhaseStage; standby: boolean }) {
  const isLast = stage.id === 'serving'
  const hollow = standby && isLast

  return (
    <span
      className={cn(
        'flex h-4 w-4 shrink-0 items-center justify-center rounded-full border text-[10px] leading-none',
        stage.state === 'done' && 'border-success/60 bg-success/15 text-success',
        stage.state === 'current' && 'border-primary bg-primary/15 text-primary',
        stage.state === 'problem' && 'border-destructive bg-destructive/15 text-destructive',
        stage.state === 'pending' && 'border-muted-foreground/30 bg-transparent text-muted-foreground/50',
        hollow && 'border-dashed border-muted-foreground/60 text-muted-foreground'
      )}
      data-state={stage.state}
      data-stage={stage.id}
    >
      {stage.state === 'done' ? <Check className="h-3 w-3" strokeWidth={3} /> : null}
      {stage.state === 'problem' ? <X className="h-3 w-3" strokeWidth={3} /> : null}
      {stage.state === 'current' && !hollow ? <span className="h-1.5 w-1.5 rounded-full bg-primary" /> : null}
    </span>
  )
}

export interface MasterPhaseTimelineProps {
  phase?: string | null
  info?: MasterPhaseInfo | null
  label: (key: string) => string
  className?: string
  /** Hide the textual phase label when the table already shows it. */
  compact?: boolean
  /** Override the root test id when several timelines share a page. */
  testId?: string
}

/**
 * The master's boot/recovery pipeline as connected dots: a green check per
 * finished stage, a filled ring for the current one, a red cross on damage.
 * Hovering explains what it is waiting for, because "which dot is which" is not
 * something an operator should have to look up during an incident.
 */
export function MasterPhaseTimeline({
  phase,
  info,
  label,
  className,
  compact = false,
  testId = 'master-phase-timeline'
}: MasterPhaseTimelineProps) {
  const timeline = buildPhaseTimeline(phase)
  const currentLabel = phaseLabel(label, timeline.phase)

  const detail = info?.detail || ''
  const blocking = info?.blocking_on ? label(`overview.blocking.${info.blocking_on}`) : ''
  // The detail line carries the recorded error (e.g. "journal entry corrupt"),
  // which the blocking label cannot: keep both when they differ.
  const detailText = detail && detail !== blocking ? detail : ''
  const rate = typeof info?.apply_rate === 'number' && info.apply_rate > 0 ? `${Math.round(info.apply_rate)}/s` : ''
  const progress =
    typeof info?.progress_ratio === 'number'
      ? `${Math.round(info.progress_ratio * 100)}%`
      : typeof info?.lag === 'number' && info.lag > 0
        ? `lag ${info.lag}`
        : ''

  const title = [
    currentLabel,
    blocking,
    detailText,
    progress,
    rate,
    info?.needs_operator ? label('overview.phase.needsOperator') : ''
  ]
    .filter(Boolean)
    .join(' · ')

  return (
    <div className={cn('flex items-center gap-2', className)} title={title} data-testid={testId}>
      <div className="flex items-center">
        {timeline.stages.map((stage, index) => (
          <span key={stage.id} className="flex items-center">
            <StageDot stage={stage} standby={timeline.standby} />
            {index < timeline.stages.length - 1 ? (
              <span
                className={cn(
                  'h-px w-2.5',
                  stage.state === 'done' ? 'bg-success/50' : 'bg-muted-foreground/25'
                )}
              />
            ) : null}
          </span>
        ))}
      </div>
      {!compact ? (
        <span
          className={cn(
            'text-xs',
            timeline.problem ? 'text-destructive' : timeline.standby ? 'text-muted-foreground' : 'text-foreground'
          )}
          data-testid="master-phase-label"
        >
          {currentLabel}
        </span>
      ) : null}
      {info?.needs_operator ? (
        <span className="text-[10px] font-medium text-destructive">{label('overview.phase.needsOperator')}</span>
      ) : null}
    </div>
  )
}

export { STAGE_LABEL_KEY }
