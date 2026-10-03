import { Subject, lastValueFrom, of, throwError, toArray } from 'rxjs';
import { describe, expect, it } from 'vitest';
import { isTraceEvent, trace, type TraceEvent } from '../src/index.js';

function recorder(): { events: TraceEvent[]; sink: (event: TraceEvent) => void } {
  const events: TraceEvent[] = [];
  return {
    events,
    sink: (event) => {
      events.push(event);
    },
  };
}

const kindsOf = (events: readonly TraceEvent[]): string[] => events.map((e) => e.kind);

describe('trace', () => {
  it('records the lifecycle of a completing stream with the injected clock', () => {
    const { events, sink } = recorder();
    const clock = { t: 1_000 };
    const source = new Subject<number>();
    const received: number[] = [];

    source
      .pipe(trace('span', sink, { trace: 'run-1', summarize: (n) => n * 10, now: () => clock.t }))
      .subscribe({ next: (v) => received.push(v), complete: () => received.push(-1) });

    clock.t = 1_005;
    source.next(1);
    clock.t = 1_012;
    source.next(2);
    clock.t = 1_020;
    source.complete();

    expect(received).toEqual([1, 2, -1]);
    const base = { trace: 'run-1', span: 'span' };
    expect(events).toEqual([
      { ...base, seq: 0, kind: 'subscribe', at: '1970-01-01T00:00:01.000Z', elapsedMs: 0 },
      { ...base, seq: 1, kind: 'next', at: '1970-01-01T00:00:01.005Z', elapsedMs: 5, value: 10 },
      { ...base, seq: 2, kind: 'next', at: '1970-01-01T00:00:01.012Z', elapsedMs: 12, value: 20 },
      { ...base, seq: 3, kind: 'complete', at: '1970-01-01T00:00:01.020Z', elapsedMs: 20 },
      { ...base, seq: 4, kind: 'unsubscribe', at: '1970-01-01T00:00:01.020Z', elapsedMs: 20 },
    ]);
  });

  it('records errors and lets them through unchanged', () => {
    const { events, sink } = recorder();
    const failure = new Error('boom');
    let caught: unknown;

    throwError(() => failure)
      .pipe(trace('s', sink, { trace: 'r' }))
      .subscribe({
        error: (error: unknown) => {
          caught = error;
        },
      });

    expect(caught).toBe(failure);
    expect(kindsOf(events)).toEqual(['subscribe', 'error', 'unsubscribe']);
    expect(events[1]?.error).toBe('Error: boom');
  });

  it('describes non-Error rejections too', () => {
    const { events, sink } = recorder();
    throwError(() => 'plain string')
      .pipe(trace('s', sink))
      .subscribe({ error: () => undefined });
    expect(events[1]?.error).toBe('plain string');
  });

  it('records no values unless summarize is given', () => {
    const { events, sink } = recorder();
    of({ secret: 1 }).pipe(trace('s', sink)).subscribe();
    const next = events[1];
    expect(next?.kind).toBe('next');
    expect(next !== undefined && 'value' in next).toBe(false);
  });

  it('records an early unsubscribe and tears the source down', () => {
    const { events, sink } = recorder();
    const source = new Subject<number>();

    const subscription = source.pipe(trace('s', sink, { trace: 'r' })).subscribe();
    source.next(1);
    expect(source.observed).toBe(true);

    subscription.unsubscribe();
    expect(source.observed).toBe(false);
    expect(kindsOf(events)).toEqual(['subscribe', 'next', 'unsubscribe']);

    source.next(2);
    expect(events).toHaveLength(3);
  });

  it('is lazy: nothing is recorded until subscription', () => {
    const { events, sink } = recorder();
    const traced = of(1).pipe(trace('s', sink));
    expect(events).toHaveLength(0);
    traced.subscribe();
    expect(kindsOf(events)).toEqual(['subscribe', 'next', 'complete', 'unsubscribe']);
  });

  it('keeps one state per subscription and a fresh trace id by default', () => {
    const { events, sink } = recorder();
    const traced = of(1, 2).pipe(trace('s', sink));
    traced.subscribe();
    traced.subscribe();

    expect(events.map((e) => e.seq)).toEqual([0, 1, 2, 3, 4, 0, 1, 2, 3, 4]);
    const ids = new Set(events.map((e) => e.trace));
    expect(ids.size).toBe(2);
    for (const id of ids) expect(id.length).toBeGreaterThan(0);
  });

  it('shares an explicit trace id across subscriptions and spans', () => {
    const { events, sink } = recorder();
    const traced = of(1).pipe(trace('a', sink, { trace: 'run' }), trace('b', sink, { trace: 'run' }));
    traced.subscribe();
    traced.subscribe();
    expect(new Set(events.map((e) => e.trace))).toEqual(new Set(['run']));
  });

  it('contains a throwing sink and a throwing summarizer', async () => {
    const values = await lastValueFrom(
      of(1, 2).pipe(
        trace('s', () => {
          throw new Error('sink');
        }),
        toArray(),
      ),
    );
    expect(values).toEqual([1, 2]);

    const { events, sink } = recorder();
    const traced = of(1).pipe(
      trace('s', sink, {
        summarize: () => {
          throw new Error('bad');
        },
      }),
      toArray(),
    );
    expect(await lastValueFrom(traced)).toEqual([1]);
    expect(events[1]?.value).toBe('[summarize threw: Error: bad]');
  });

  it('orders events across nested spans along the notification flow', () => {
    const { events, sink } = recorder();
    of(1)
      .pipe(trace('a', sink, { trace: 'r' }), trace('b', sink, { trace: 'r' }))
      .subscribe();
    expect(events.map((e) => `${e.span}:${e.kind}`)).toEqual([
      'b:subscribe',
      'a:subscribe',
      'a:next',
      'b:next',
      'a:complete',
      'b:complete',
      'a:unsubscribe',
      'b:unsubscribe',
    ]);
  });

  it('accepts a Subject or any observer-like object as the sink', () => {
    const subject = new Subject<TraceEvent>();
    const seen: string[] = [];
    subject.subscribe((e) => seen.push(e.kind));
    of(1).pipe(trace('s', subject)).subscribe();
    expect(seen).toEqual(['subscribe', 'next', 'complete', 'unsubscribe']);

    const observerLike = { next: (e: TraceEvent) => seen.push(`again:${e.kind}`) };
    of(1).pipe(trace('s', observerLike)).subscribe();
    expect(seen.slice(4)).toEqual([
      'again:subscribe',
      'again:next',
      'again:complete',
      'again:unsubscribe',
    ]);
  });

  it('produces events that survive a JSON round trip', () => {
    const { events, sink } = recorder();
    of({ n: 1 })
      .pipe(trace('s', sink, { summarize: (v) => v.n }))
      .subscribe();
    const wire = events.map((e) => JSON.parse(JSON.stringify(e)) as unknown);
    expect(wire.every(isTraceEvent)).toBe(true);
    expect(wire).toEqual(events);
  });

  it('isTraceEvent rejects look-alikes', () => {
    const { events, sink } = recorder();
    of(1).pipe(trace('s', sink)).subscribe();
    expect(isTraceEvent(null)).toBe(false);
    expect(isTraceEvent('subscribe')).toBe(false);
    expect(isTraceEvent({ trace: 'r', span: 's', seq: 0, kind: 'nope', at: '', elapsedMs: 0 })).toBe(
      false,
    );
    expect(isTraceEvent({ ...events[0], seq: '0' })).toBe(false);
  });
});
