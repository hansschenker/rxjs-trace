// Law tests for the `trace` operator, following the lawful-operator checklist.
//
// Classification: transparent (identity) operator with side effects into a sink.
// Equivalence: E3, values + order + virtual time, via TestScheduler marbles.
// Inputs are generated with fast-check: arbitrary marble diagrams with an optional
// terminal notification, and an optional unsubscription frame.

import fc from 'fast-check';
import { TestScheduler } from 'rxjs/testing';
import { describe, expect, it } from 'vitest';
import { trace, type TraceEvent, type TraceKind } from '../src/index.js';

const values: Record<string, number> = { a: 1, b: 2, c: 3 };
const error = new Error('boom');

const arbMarble = fc
  .tuple(
    fc.array(fc.constantFrom('-', 'a', 'b', 'c'), { minLength: 1, maxLength: 10 }),
    fc.constantFrom('', '|', '#'),
  )
  .map(([frames, terminal]) => frames.join('') + terminal);

const arbUnsubscribe = fc.option(fc.integer({ min: 0, max: 12 }), { nil: null });

function scheduler(): TestScheduler {
  return new TestScheduler((actual: unknown, expected: unknown) => {
    expect(actual).toEqual(expected);
  });
}

function unsubscribeMarble(frame: number | null): string | undefined {
  return frame === null ? undefined : `${'-'.repeat(frame)}!`;
}

function terminalFrame(marble: string): number | null {
  const last = marble[marble.length - 1];
  return last === '|' || last === '#' ? marble.length - 1 : null;
}

/** Frame at which the source subscription ends, or null when it stays open. */
function endFrame(marble: string, unsubscribe: number | null): number | null {
  const terminal = terminalFrame(marble);
  if (terminal === null) return unsubscribe;
  if (unsubscribe === null) return terminal;
  return Math.min(unsubscribe, terminal);
}

function subscriptionMarble(end: number | null): string {
  if (end === null) return '^';
  if (end === 0) return '(^!)';
  return `^${'-'.repeat(end - 1)}!`;
}

/** What the traced stream must emit: the source, cut off at the unsubscription frame. */
function expectedOutput(marble: string, unsubscribe: number | null): string {
  return unsubscribe === null ? marble : marble.slice(0, unsubscribe);
}

type ExpectedEvent = Omit<TraceEvent, 'at'>;

/** What the sink must receive, with `elapsedMs` equal to the virtual frame. */
function expectedSink(marble: string, unsubscribe: number | null): ExpectedEvent[] {
  const out: ExpectedEvent[] = [];
  let seq = 0;
  const push = (kind: TraceKind, elapsedMs: number, extra: Partial<ExpectedEvent> = {}): void => {
    out.push({ trace: 'run', span: 's', seq: seq++, kind, elapsedMs, ...extra });
  };

  push('subscribe', 0);
  const delivered = unsubscribe ?? Number.POSITIVE_INFINITY;
  for (let frame = 0; frame < marble.length && frame < delivered; frame++) {
    const char = marble[frame];
    if (char === undefined || char === '-') continue;
    if (char === '|') push('complete', frame);
    else if (char === '#') push('error', frame, { error: 'Error: boom' });
    else push('next', frame, { value: values[char] });
  }
  const end = endFrame(marble, unsubscribe);
  if (end !== null) push('unsubscribe', end);
  return out;
}

const silent = (): void => undefined;

describe('trace laws', () => {
  it('identity (E3): source.pipe(trace()) has the marbles of source under any unsubscription', () => {
    fc.assert(
      fc.property(arbMarble, arbUnsubscribe, (marble, unsubscribe) => {
        scheduler().run(({ cold, expectObservable, expectSubscriptions }) => {
          const source = cold(marble, values, error);
          expectObservable(source.pipe(trace('s', silent)), unsubscribeMarble(unsubscribe)).toBe(
            expectedOutput(marble, unsubscribe),
            values,
            error,
          );
          expectSubscriptions(source.subscriptions).toBe(
            subscriptionMarble(endFrame(marble, unsubscribe)),
          );
        });
      }),
      { numRuns: 300 },
    );
  });

  it('composition (E3): stacking traces is still the identity', () => {
    fc.assert(
      fc.property(arbMarble, arbUnsubscribe, (marble, unsubscribe) => {
        scheduler().run(({ cold, expectObservable, expectSubscriptions }) => {
          const source = cold(marble, values, error);
          expectObservable(
            source.pipe(trace('a', silent), trace('b', silent), trace('c', silent)),
            unsubscribeMarble(unsubscribe),
          ).toBe(expectedOutput(marble, unsubscribe), values, error);
          expectSubscriptions(source.subscriptions).toBe(
            subscriptionMarble(endFrame(marble, unsubscribe)),
          );
        });
      }),
      { numRuns: 300 },
    );
  });

  it('sink protocol (E3): events mirror the notification flow with virtual timestamps', () => {
    fc.assert(
      fc.property(arbMarble, arbUnsubscribe, (marble, unsubscribe) => {
        const received: TraceEvent[] = [];
        const virtual = scheduler();
        virtual.run(({ cold, expectObservable }) => {
          const source = cold(marble, values, error);
          const traced = source.pipe(
            trace(
              's',
              (event) => {
                received.push(event);
              },
              { trace: 'run', summarize: (v) => v, now: () => virtual.now() },
            ),
          );
          expectObservable(traced, unsubscribeMarble(unsubscribe)).toBe(
            expectedOutput(marble, unsubscribe),
            values,
            error,
          );
        });

        expect(received.map(({ at, ...rest }) => rest)).toEqual(expectedSink(marble, unsubscribe));
        expect(received.every((e) => typeof e.at === 'string')).toBe(true);
      }),
      { numRuns: 300 },
    );
  });

  it('hygiene: lazy, one state per subscription, silence after unsubscribe', () => {
    fc.assert(
      fc.property(arbMarble, arbUnsubscribe, (marble, unsubscribe) => {
        const received: TraceEvent[] = [];
        const virtual = scheduler();
        virtual.run(({ cold, expectObservable }) => {
          const traced = cold(marble, values, error).pipe(
            trace(
              's',
              (event) => {
                received.push(event);
              },
              { now: () => virtual.now() },
            ),
          );
          // A2 Eager Beaver: building the pipeline records nothing.
          expect(received).toHaveLength(0);
          // A1 Mutant: two subscriptions, each with its own counter and id.
          expectObservable(traced, unsubscribeMarble(unsubscribe));
          expectObservable(traced, unsubscribeMarble(unsubscribe));
        });

        const byTrace = new Map<string, TraceEvent[]>();
        for (const event of received) {
          byTrace.set(event.trace, [...(byTrace.get(event.trace) ?? []), event]);
        }
        expect(byTrace.size).toBe(2);
        for (const events of byTrace.values()) {
          expect(events[0]?.kind).toBe('subscribe');
          expect(events.map((e) => e.seq)).toEqual(events.map((_, i) => i));
          const terminals = events.filter((e) => e.kind === 'complete' || e.kind === 'error');
          expect(terminals.length).toBeLessThanOrEqual(1);
          const unsubscribes = events.filter((e) => e.kind === 'unsubscribe');
          expect(unsubscribes.length).toBeLessThanOrEqual(1);
          // A4 Time Traveler: nothing after unsubscribe, and the terminal sits right before it.
          if (unsubscribes.length === 1) {
            expect(events[events.length - 1]?.kind).toBe('unsubscribe');
            if (terminals.length === 1) {
              expect(events[events.length - 2]?.kind).toBe(terminals[0]?.kind);
            }
          }
        }
      }),
      { numRuns: 300 },
    );
  });
});
