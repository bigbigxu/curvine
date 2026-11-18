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

import { describe, expect, it } from 'vitest'
import { renderToStaticMarkup } from 'react-dom/server'
import { buildPhaseTimeline, MasterPhaseTimeline } from './master-phase-timeline'

const states = (phase: string | null | undefined) => buildPhaseTimeline(phase).stages.map((s) => s.state)

describe('buildPhaseTimeline', () => {
  it('walks the pipeline as a recovery advances', () => {
    expect(states('Starting')).toEqual(['current', 'pending', 'pending', 'pending'])
    expect(states('Replaying')).toEqual(['done', 'current', 'pending', 'pending'])
    expect(states('CatchingUp')).toEqual(['done', 'done', 'current', 'pending'])
    expect(states('Serving')).toEqual(['done', 'done', 'done', 'done'])
  })

  it('keeps a healthy standby off the serving stop', () => {
    const timeline = buildPhaseTimeline('StandingBy')
    // First three stops are behind it; the last one stays open because a
    // standby does not serve.
    expect(timeline.stages.map((s) => s.state)).toEqual(['done', 'done', 'done', 'current'])
    expect(timeline.standby).toBe(true)
    expect(timeline.problem).toBe(false)
  })

  it('marks damage with a cross instead of progress', () => {
    for (const phase of ['SafeMode', 'Quarantined']) {
      const timeline = buildPhaseTimeline(phase)
      expect(timeline.problem).toBe(true)
      // The cross sits where the pipeline stopped, not on the last stop.
      expect(timeline.stages[2].state).toBe('problem')
      expect(timeline.stages.some((s) => s.state === 'current')).toBe(false)
    }

    // Nothing is trustworthy once the process is going away.
    expect(states('Exiting')[0]).toBe('problem')
  })

  it('renders nothing it cannot know', () => {
    for (const phase of [null, undefined, '', 'SomeFuturePhase']) {
      const timeline = buildPhaseTimeline(phase)
      expect(timeline.stages.every((s) => s.state === 'pending')).toBe(true)
      expect(timeline.problem).toBe(false)
    }
  })
})

describe('MasterPhaseTimeline', () => {
  const label = (key: string) => key.split('.').pop() ?? key

  it('renders one dot per stage and a check per finished stage', () => {
    const html = renderToStaticMarkup(
      <MasterPhaseTimeline phase="Replaying" label={label} />
    )
    // Four stops, three connector lines.
    expect(html.match(/data-stage=/g)?.length).toBe(4)
    expect(html.match(/data-state="done"/g)?.length).toBe(1)
    expect(html.match(/data-state="current"/g)?.length).toBe(1)
  })

  it('surfaces the blocking reason and the operator request in the tooltip', () => {
    const html = renderToStaticMarkup(
      <MasterPhaseTimeline
        phase="SafeMode"
        label={label}
        info={{
          phase: 'SafeMode',
          blocking_on: 'safe_mode_damage',
          detail: 'journal entry corrupt',
          needs_operator: true,
          lag: 0
        }}
      />
    )
    expect(html).toContain('title=')
    expect(html).toContain('journal entry corrupt')
    expect(html).toContain('needsOperator')
  })

  it('shows a moving-denominator lag instead of a percentage', () => {
    const html = renderToStaticMarkup(
      <MasterPhaseTimeline
        phase="CatchingUp"
        label={label}
        info={{ phase: 'CatchingUp', lag: 4200, denominator: 'moving', apply_rate: 900 }}
      />
    )
    expect(html).toContain('lag 4200')
    expect(html).not.toContain('%')
    expect(html).toContain('900/s')
  })
})
