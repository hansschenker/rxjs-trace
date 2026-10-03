# rxjs-trace

A law-tested RxJS operator that reports every lifecycle step of a
subscription, `subscribe`, `next`, `error`, `complete` and `unsubscribe`, to a
sink of your choice, and passes the stream through unchanged.

```ts
import { trace } from 'rxjs-trace';

query$
  .pipe(
    trace('raw', console.log),
    debounceTime(300),
    trace('debounced', console.log),
    switchMap(search),
    trace('results', console.log, { summarize: (rows) => rows.length }),
  )
  .subscribe();
```

Each event is a plain JSON-friendly object:

```ts
interface TraceEvent {
  trace: string;      // correlation id for one pipeline run
  span: string;       // label of the traced stage
  seq: number;        // order within one subscription, from 0
  kind: 'subscribe' | 'next' | 'error' | 'complete' | 'unsubscribe';
  at: string;         // ISO timestamp taken on the client
  elapsedMs: number;  // since the subscription started
  value?: unknown;    // on next, only when summarize was given
  error?: string;     // on error
}
```

## Install

```bash
npm install rxjs-trace
```

`rxjs` 7 is a peer dependency. Node 18+ or any modern browser.

## Usage

### Worked example

A typeahead pipeline traced at three points, with a one-line console sink.
The output below is real.

```ts
import { Subject, debounceTime, delay, distinctUntilChanged, of, switchMap } from 'rxjs';
import { trace, type TraceEvent } from 'rxjs-trace';

// A sink: one compact line per event. Any function or object with `next` works.
const log = (e: TraceEvent): void =>
  console.log(
    `${String(e.elapsedMs).padStart(4)}ms  ${e.span.padEnd(12)}${e.kind}` +
      (e.value !== undefined ? `  ${JSON.stringify(e.value)}` : '') +
      (e.error !== undefined ? `  ${e.error}` : ''),
  );

// Stand-in search backend that answers after 50 ms.
const search = (q: string) => of([`${q}-1`, `${q}-2`]).pipe(delay(50));

const input$ = new Subject<string>();
const runId = 'typeahead-1'; // same id on every span, so the events correlate

const results$ = input$.pipe(
  trace('input', log, { trace: runId, summarize: (q) => q }),
  debounceTime(100),
  distinctUntilChanged(),
  trace('debounced', log, { trace: runId, summarize: (q) => q }),
  switchMap((q) => search(q).pipe(trace(`search:${q}`, log, { trace: runId }))),
  trace('results', log, { trace: runId, summarize: (rows) => rows.length }),
);

const subscription = results$.subscribe();
input$.next('r');
input$.next('rx');
setTimeout(() => input$.next('rxjs'), 30);
setTimeout(() => subscription.unsubscribe(), 400);
```

```
   0ms  results     subscribe
   0ms  debounced   subscribe
   0ms  input       subscribe
   1ms  input       next  "r"
   1ms  input       next  "rx"
  46ms  input       next  "rxjs"
 154ms  debounced   next  "rxjs"
   0ms  search:rxjs subscribe
  61ms  search:rxjs next
 219ms  results     next  2
  62ms  search:rxjs complete
  62ms  search:rxjs unsubscribe
 402ms  input       unsubscribe
 402ms  debounced   unsubscribe
 405ms  results     unsubscribe
```

What the trace tells you:

- **Subscription order is outer to inner**, then everything else flows inner
  to outer. That is why `results` subscribes first but `input` emits first.
- **The debounce did its job.** Three keystrokes went in, one query came out,
  about 100 ms after the last one.
- **The inner span has its own clock.** `search:rxjs` reads 0 ms at subscribe
  and 61 ms at its result, because `elapsedMs` is per subscription. The outer
  `results` span shows the same emission at 219 ms from the start of the run.
- **No leaks.** Every span that subscribed also unsubscribed, including the
  inner search, which tore down right after completing.

### Sinks

The sink is a function or anything with a `next` method, so a `Subject`, an
RxJS `Observer`, or an adapter for your logging backend all work.

```ts
import { Subject } from 'rxjs';
import { trace, type TraceEvent } from 'rxjs-trace';

const traces = new Subject<TraceEvent>();
traces.subscribe((e) => logger.info(e));          // or ship them anywhere

source$.pipe(trace('source', traces)).subscribe();
```

To collect events in a test, a plain array push is enough:

```ts
const events: TraceEvent[] = [];
source$.pipe(trace('s', (e) => events.push(e))).subscribe();
```

### Correlating spans

Every subscription gets its own random `trace` id unless you pass one. Pass
the same id to every span of one run to correlate them when reading the
events back.

```ts
const runId = crypto.randomUUID();
source$.pipe(
  trace('a', sink, { trace: runId }),
  map(work),
  trace('b', sink, { trace: runId }),
);
```

With nested spans, `subscribe` events arrive outer to inner and every other
event inner to outer, following the notification flow:
`b:subscribe, a:subscribe, a:next, b:next, a:complete, b:complete, a:unsubscribe, b:unsubscribe`.

### Values and errors

Values are not recorded unless you pass `summarize`, so nothing sensitive or
large leaves the process by accident. Errors are recorded as
`"<name>: <message>"`. A sink or summarizer that throws is contained: the
traced stream never sees the exception.

### Reading events back

`isTraceEvent` is a type guard for events that came through JSON or another
untyped channel.

```ts
import { isTraceEvent } from 'rxjs-trace';

const open = new Set<string>();
for (const t of rows.filter(isTraceEvent).sort((a, b) => a.seq - b.seq)) {
  if (t.kind === 'subscribe') open.add(t.span);
  if (t.kind === 'unsubscribe') open.delete(t.span);
}
// `open` now lists spans that never tore down: a leak report.
```

Sort on `seq` and `at`, not on whatever timestamp your log backend assigns on
receipt.

### Testing with a fake clock

`now` is injectable and returns epoch milliseconds. Inside a `TestScheduler`
run, `() => scheduler.now()` makes `elapsedMs` equal to the virtual frame.

## Guarantees

`trace` is law-tested with fast-check and `TestScheduler` under E3, meaning
values, order and virtual time:

- **Identity.** `source.pipe(trace(s, sink))` has exactly the marbles of
  `source`, under any unsubscription point, and tears the source down at the
  same frame.
- **Composition.** Stacking traces is still the identity.
- **Sink protocol.** Exactly one `subscribe` first and one `unsubscribe` last,
  `seq` consecutive from 0, at most one terminal immediately before
  `unsubscribe`, `next` values in source order, timestamps equal to the frames.
- **Hygiene.** Lazy (nothing recorded until subscription), one state per
  subscription, silence after unsubscribe.

See `PRINCIPLES.md` and `test/trace.laws.test.ts`. The suite was mutation-checked:
removing the teardown fails all four properties, and sharing state across
subscriptions fails the hygiene property.

## Build and test

```bash
npm install
npm run build        # tsup → dist/ (ESM + CJS + .d.ts)
npm test             # vitest, including the law suite
npm run typecheck
```

## License

Apache 2.0, see `LICENSE`. The operator was first written for
[rxjs-loggly](https://github.com/hansschenker/rxjs-loggly), a TypeScript and
RxJS port of Erik Meijer's Lx, and extracted because it is useful on its own.
