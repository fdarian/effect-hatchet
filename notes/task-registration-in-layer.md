# Move task registration into `Hatchet.layer`

Status: planned, not implemented. Targets Effect v4 (`effect@4.0.0-rc.111`).

## Why

Today a caller has to run three things before a task works:

```ts
const hatchet = yield* Hatchet
yield* hatchet.register(greet)
yield* hatchet.startWorker()
// ... and `Effect.scoped` at the bottom, because `register` requires `Scope`
```

The order is load-bearing and nothing enforces it. Registering after
`startWorker()` is silently broken, not loudly broken:

- `InternalWorker.start()` → `createListener()` sends one `register` RPC
  carrying `actions: Object.keys(this.action_registry)` — a snapshot.
- Reconnects (`listenV2({ workerId })`) and heartbeats (`{ workerId,
  heartbeatAt }`) carry no action list. There is no re-register path.
- So a late-registered task *does* get its definition into the engine
  (`putWorkflow` runs), which means `run()` enqueues successfully — and then
  waits forever, because no worker ever told the dispatcher it serves that
  action.

A worker's task set is fixed for the life of the process. The API should say
so by construction.

## API change

```ts
// before
program.pipe(
  Effect.provide(Hatchet.layer()),
  Effect.scoped,
  Effect.runPromise,
)

// after
program.pipe(
  Effect.provide(Hatchet.layer({ tasks: [greet, sendEmail] })),
  Effect.runPromise,
)
```

- `hatchet.register` and `hatchet.startWorker` leave the public `Hatchet`
  interface. They move under `_internal` — the layer is the only caller.
- `Effect.scoped` is no longer needed in user code: `Layer.scoped` owns the
  scope that registration (and the worker fiber) live in.
- The task's own `R` becomes the layer's `R`, so the compiler enforces what
  the README used to warn about in prose ("provide `Mailer` before you call
  `register`").

Rejected alternatives are in [Alternatives considered](#alternatives-considered).

## Types

```ts
// src/core/task.ts
// biome-ignore lint/suspicious/noExplicitAny: INPUT/OUTPUT are contravariant
export type AnyTask = Task<any, any, any, any>

// distributes over the union, so a heterogeneous tuple yields `R1 | R2 | ...`
type RequirementsOf<T> = T extends Task<any, any, any, infer R> ? R : never
export type TasksRequirements<T extends ReadonlyArray<AnyTask>> =
  RequirementsOf<T[number]>
```

```ts
// src/impl/live.ts
export type Options<Tasks extends ReadonlyArray<AnyTask>> = {
  readonly tasks?: Tasks
  readonly runPrefersThisWorker?: boolean
}

export const layer: <const Tasks extends ReadonlyArray<AnyTask>>(
  options?: Options<Tasks>,
) => Layer.Layer<Hatchet, ConfigError.ConfigError, TasksRequirements<Tasks>>
```

`const Tasks` keeps the tuple from widening to `AnyTask[]`, which is what
preserves the per-task `R` in the union.

## Steps

### 1. `src/core/hatchet.ts`

Move `register` and `startWorker` into `_internal` alongside `run` /
`runNoWait` / `schedule`. Nothing else on the interface changes.

### 2. `src/impl/live.ts`

`make()` stays as-is. The layer grows the sequencing:

```ts
export const layer = <const Tasks extends ReadonlyArray<AnyTask>>(
  options?: Options<Tasks>,
) =>
  Layer.scoped(
    HatchetTag,
    Effect.gen(function* () {
      const hatchet = yield* make(options)
      yield* Effect.forEach(
        options?.tasks ?? [],
        (task) => hatchet._internal.register(task),
        { discard: true },
      )
      yield* hatchet._internal.startWorker()
      return hatchet
    }),
  )
```

`Layer.scoped` (not `Layer.effect`) — v4's `Layer.effect` keeps `R` intact,
so `Scope` would leak into the layer's requirements. `Layer.scoped` is
`Layer<I, E, Exclude<R, Scope>>`.

`Effect.forEach` over a heterogeneous tuple may need a cast at the boundary;
the exported signature above is the contract that matters.

`startWorker` already ends with `worker.waitUntilReady()`, so the layer is
not ready until the engine has the registration — keep that.

### 3. `src/impl/in-memory.ts`

Same `{ tasks }` option, same `Layer.scoped` shape, no worker to start
(`startWorker` stays `Effect.void`).

**Enforce the same fixed-set rule even though nothing forces it here.** The
in-memory engine could accept registrations at any time; if it does, a test
passes and production hangs, and the layer swap stops being a swap.

### 4. `src/index.ts`

`Hatchet.layer` / `Hatchet.layerInMemory` pick up the generic `Tasks`
parameter. `layerInMemory` currently takes no arguments and returns a bare
layer value — it becomes a function of `Options`.

### 5. `tests/shared-suite.ts`

This is the largest mechanical change. Every test today defines its task
*inside* the test body and registers at runtime:

```ts
it.effect("...", () => Effect.gen(function* () {
  const greet = Task.make({ ... })
  const hatchet = yield* Hatchet
  yield* hatchet.register(greet)
  yield* hatchet.startWorker()
  ...
}))
```

`registerSharedHatchetTests(it)` receives an `it` already scoped to one layer
per file, so tasks can no longer be defined per-test. Hoist every task
definition to module scope and pass the whole set into the layer that
`tests/hatchet.test.ts` and `tests/hatchet.real.test.ts` build. Names are
already unique across the suite (the real-engine run shares one tenant).

Keep `cron._testFire` — it is unaffected.

### 6. `README.md`

The Quick start is already written against this API. The Guides sections
"Registering tasks" and "Starting the worker" describe the old flow and go
away; fold what survives (the `R`-captured-at-registration note, the
fixed-task-set constraint) into a single "Wiring tasks into the layer"
section.

## Open decisions

- **`tasks: []` / omitted.** Today that yields a client that can't dispatch
  anything, because `_internal.run` looks up a local declaration map. Either
  keep dying with the current "make sure you have registered the task"
  message, or make it a type error. Leaning: keep the die, revisit under
  the follow-up below.
- **Changeset timing.** The major changeset lands with this plan; do not cut
  a release until the implementation is in, or `changeset version` will
  publish a major describing an API that doesn't exist yet.

## Alternatives considered

**Per-task `greet.layer` + a `HatchetRegistry` service.** Task layers write
into a registry, and something reads it after they've all built. This is the
`@effect/workflow` shape (`Workflow.toLayer` is
`Layer.effectDiscard(Effect.flatMap(EngineTag, (engine) => engine.register(...)))`).
It composes better — each task's dependencies get provided at its own layer
instead of unioning at the top.

It needs a third thing on top (`Hatchet.layerWorker`) to be the "after all
registrations" step, because `Layer.provide(X, Y)` builds `Y` first, so
`Hatchet.layer()` underneath the task layers would start the worker before any
task registered. That's one more layer in every bootstrap and one more thing
to forget — and forgetting it produces the hang described above.

`@effect/workflow` doesn't need that step because a cluster runner announces
itself as `Runner{address, groups, weight}` — no entity types at all — and
unregistered-entity traffic parks on a `Latch` until registration opens it.
Hatchet's protocol has no equivalent. The pattern doesn't transfer.

**Restarting the worker when the task set changes.** Makes late registration
genuinely work, at the cost of a new `workerId`, a drain window for in-flight
runs, and worker churn in the engine UI on every startup registration.
Rejected.

## Follow-up (separate change)

Split dispatch from execution. Everything needed to *trigger* work is
name-based in the SDK and needs no local declaration:

- `admin.runWorkflow(name, input)` → `WorkflowRunRef`
- `runs.runRef(id).output` to await by id
- `scheduled.create(name, { triggerAt, input })`, `crons.create(name, {...})`

`putWorkflow` is called from exactly one place — `worker-internal.registerWorkflow`,
reachable only via `Worker.create` / `registerWorkflows` — and the
`triggerWorkflow` wire message carries no definition fields, so a process that
never constructs a `Worker` cannot clobber the real definition.

With that split, `Hatchet.layer()` becomes a dispatch client that depends on
nothing and can sit at the bottom of any graph, and only the worker entrypoint
needs the complete task set. That is what would let a service define and
dispatch a task from anywhere in the layer graph.
