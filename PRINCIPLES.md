# Principles

Every operator in this package ships with law tests (`test/*.laws.test.ts`).
A law is a refactoring guarantee: every law an operator satisfies is a
transformation users may apply to their pipelines without changing behavior.
Laws are stated with their equivalence relation; E3 means values, order, and
virtual time under `TestScheduler`.

| Operator | Laws | Hygiene |
| -------- | ---- | ------- |
| `trace` | identity (E3), composition with itself (E3), sink protocol (E3) | lazy, one state per subscription, teardown on unsubscribe, silence after terminal |

The law suite generates its inputs with fast-check: arbitrary marble diagrams
with an optional terminal notification and an optional unsubscription frame,
300 cases per property. The same harness is reusable for other transparent or
mono-type operators; see the helpers at the top of `test/trace.laws.test.ts`.
