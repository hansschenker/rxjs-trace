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
