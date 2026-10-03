// Laws (verified in test/trace.laws.test.ts under E3, values + order + virtual time):
//   identity     source.pipe(trace(s, sink)) ≡ source
//   composition  source.pipe(trace(a, sink), trace(b, sink)) ≡ source
//   hygiene      lazy (no sink events before subscribe), one state per subscription,
//                teardown on unsubscribe, silence after the terminal notification.
// Sink protocol: exactly one `subscribe` first and one `unsubscribe` last, `seq`
// consecutive from 0, at most one terminal (`error` | `complete`) immediately
// before `unsubscribe`, `next` values in source order.

import { Observable, type MonoTypeOperatorFunction, type Observer } from 'rxjs';

export type TraceKind = 'subscribe' | 'next' | 'error' | 'complete' | 'unsubscribe';

/** One lifecycle notification of a traced subscription. Serializes cleanly to JSON. */
export interface TraceEvent {
  /** Correlation id shared by every span of one pipeline run. */
  readonly trace: string;
  /** Label of the traced stage. */
  readonly span: string;
  /** Order within one subscription, starting at 0. */
  readonly seq: number;
  readonly kind: TraceKind;
  /** ISO timestamp taken on the client. */
  readonly at: string;
  /** Milliseconds since the subscription started. */
  readonly elapsedMs: number;
  /** Summarized value, only on `next` and only when `summarize` was given. */
  readonly value?: unknown;
  /** Description of the error, only on `error`. */
  readonly error?: string;
}

/** Where trace events go: a plain function, or anything with `next` (a Subject, an Observer). */
export type TraceSink = ((event: TraceEvent) => void) | Pick<Observer<TraceEvent>, 'next'>;

export interface TraceOptions<T> {
  /** Correlation id for the pipeline run. Defaults to a fresh random id per subscription. */
  readonly trace?: string;
  /** Turns a value into what gets recorded on `next`. Omit to record no values. */
  readonly summarize?: (value: T) => unknown;
  /** Clock in epoch milliseconds, injectable for tests. Defaults to `Date.now`. */
  readonly now?: () => number;
}

const TRACE_KINDS: ReadonlySet<string> = new Set<TraceKind>([
  'subscribe',
  'next',
  'error',
  'complete',
  'unsubscribe',
]);

/** Type guard for events that went through JSON or another untyped channel. */
export function isTraceEvent(value: unknown): value is TraceEvent {
  if (typeof value !== 'object' || value === null) return false;
  const candidate = value as Record<string, unknown>;
  return (
    typeof candidate['trace'] === 'string' &&
    typeof candidate['span'] === 'string' &&
    typeof candidate['seq'] === 'number' &&
    typeof candidate['kind'] === 'string' &&
    TRACE_KINDS.has(candidate['kind']) &&
    typeof candidate['at'] === 'string' &&
    typeof candidate['elapsedMs'] === 'number'
  );
}

/**
 * Reports every lifecycle notification of the subscription to `sink` and passes
 * the stream through unchanged. Tracing never alters the traced stream: a sink
 * or summarizer that throws is contained.
 *
 * With several traced stages in one pipe, `subscribe` events arrive outer to
 * inner and every other event inner to outer, following the notification flow.
 */
export function trace<T>(
  span: string,
  sink: TraceSink,
  options: TraceOptions<T> = {},
): MonoTypeOperatorFunction<T> {
  const deliver: (event: TraceEvent) => void =
    typeof sink === 'function' ? sink : (event) => sink.next(event);
  const now = options.now ?? Date.now;
  const summarize = options.summarize;

  return (source) =>
    new Observable<T>((subscriber) => {
      const traceId = options.trace ?? randomTraceId();
      const startedAt = now();
      let seq = 0;

      const record = (kind: TraceKind, extra?: Partial<Pick<TraceEvent, 'value' | 'error'>>): void => {
        const t = now();
        const event: TraceEvent = {
          trace: traceId,
          span,
          seq,
          kind,
          at: new Date(t).toISOString(),
          elapsedMs: t - startedAt,
          ...extra,
        };
        seq += 1;
        try {
          deliver(event);
        } catch {
          // Tracing must never break the traced stream.
        }
      };

      record('subscribe');
      const subscription = source.subscribe({
        next: (value) => {
          record('next', summarize === undefined ? undefined : { value: summarizeSafely(summarize, value) });
          subscriber.next(value);
        },
        error: (error: unknown) => {
          record('error', { error: describeError(error) });
          subscriber.error(error);
        },
        complete: () => {
          record('complete');
          subscriber.complete();
        },
      });

      return () => {
        subscription.unsubscribe();
        record('unsubscribe');
      };
    });
}

function summarizeSafely<T>(summarize: (value: T) => unknown, value: T): unknown {
  try {
    return summarize(value);
  } catch (error: unknown) {
    return `[summarize threw: ${describeError(error)}]`;
  }
}

function describeError(error: unknown): string {
  if (error instanceof Error) {
    return `${error.name}: ${error.message}`;
  }
  try {
    return String(error);
  } catch {
    return 'unknown error';
  }
}

function randomTraceId(): string {
  // Web Crypto is global from Node 19 and in browsers; older runtimes fall back.
  const id: string | undefined = globalThis.crypto?.randomUUID?.();
  return id ?? `${Date.now().toString(36)}-${Math.random().toString(36).slice(2, 10)}`;
}
