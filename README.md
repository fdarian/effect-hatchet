# effect-hatchet

> Effect-native bindings for Hatchet, with an in-memory implementation for tests.

A single `Hatchet` tag with two constructors: one that talks to a real [Hatchet](https://hatchet.run) engine, and one that runs your task graph entirely in-process. Define a task once with Effect + Schema; swap the layer to run it under Vitest with no engine.

## Install

```bash
bun add effect-hatchet @hatchet-dev/typescript-sdk effect
```

`@hatchet-dev/typescript-sdk` is a peer dependency. It's only loaded at runtime when you use `Hatchet.layer` — tests using `Hatchet.layerInMemory` won't touch it.

## Quick start

```ts
import { Effect, Schema as S } from "effect"
import { Hatchet, Task } from "effect-hatchet"

const greet = Task.make({
  name: "greet",
  input: S.Struct({ name: S.String }),
  output: S.Struct({ message: S.String }),
  fn: (input) => Effect.succeed({ message: `hello ${input.name}` }),
})

const program = Effect.gen(function* () {
  const hatchet = yield* Hatchet
  yield* hatchet.register(greet)
  yield* hatchet.startWorker()

  const result = yield* greet.run({ name: "world" })
  console.log(result.message) // "hello world"
})

program.pipe(
  Effect.provide(Hatchet.layer()),
  Effect.scoped,
  Effect.runPromise,
)
```

`Hatchet.layer` reads connection config from the standard Hatchet env vars (`HATCHET_CLIENT_TOKEN`, `HATCHET_CLIENT_HOST_PORT`, `HATCHET_CLIENT_API_URL`, `HATCHET_CLIENT_TLS_STRATEGY`) via Effect `Config`.

## Test without a Hatchet engine

Swap `Hatchet.layer` for `Hatchet.layerInMemory` and the same program runs in-process — no worker, no gRPC, no engine.

```ts
import { Effect, Schema as S } from "effect"
import { expect, it } from "vitest"
import { Hatchet } from "effect-hatchet"

it("greets", () =>
  Effect.gen(function* () {
    const hatchet = yield* Hatchet
    yield* hatchet.register(greet)

    const result = yield* greet.run({ name: "world" })
    expect(result.message).toBe("hello world")
  }).pipe(
    Effect.provide(Hatchet.layerInMemory()),
    Effect.scoped,
    Effect.runPromise,
  ))
```

`startWorker()` is a no-op under `layerInMemory`, so the same bootstrap works in both layers. `task.schedule` honors real wall-clock delays via `Effect.sleep` — pair with `TestClock` for time-dependent tests.

## Guides

The order below mirrors the lifecycle: define a task, register it, start the worker, run it, schedule it, handle errors.

### Defining tasks

A `Task` is a unit of work. You define it with `Task.make`, then register it with the engine to make it runnable.

Only `name` and `fn` are required; everything else is optional.

```ts
Task.make({
  name: "send-email",                        // required
  input: S.Struct({                          // optional
    to: S.String,
    subject: S.String,
  }),
  output: S.Struct({ messageId: S.String }), // optional
  fn: (input, ctx) =>                        // required
    Effect.gen(function* () {
      const mailer = yield* Mailer
      const id = yield* mailer.send(input.to, input.subject)
      return { messageId: id }
    }),

  // Passed through to Hatchet (all optional):
  rateLimits: [{ key: "send-email", units: 1 }],
  on: { event: "user:created" },
  durable: true,
})
```

#### Schema optionality

`input` and `output` are independent — supply either, both, or neither:

- Omit `input` → `fn`'s `input` parameter is untyped (`unknown`); `task.run` accepts anything.
- Omit `output` → whatever `fn` returns is passed through verbatim, no encoding.
- Provide either → that side gets compile-time types **and** runtime validation. Input is decoded before `fn` runs; output is encoded before being returned to the caller.

#### `fn` shape

`Effect.Effect<OUTPUT, ERROR, REQUIREMENTS>`. Any `R` your task depends on (services, configs, loggers) is captured at `register` time and provided when the task runs — including under `layerInMemory`. So `yield* Mailer` inside `fn` works as long as the layer providing `Mailer` is in scope when you call `register`.

#### `ctx`

`{ runId: string }` — Hatchet's workflow run id (a UUID under `layerInMemory`).

#### Errors

Any failure in `fn` — typed errors, schema decode errors on `input`, or unexpected throws — is logged and surfaced to the caller as `TaskExecutionFailure` with the original error in `cause`.

### Registering tasks

Defining a task only creates a value; the engine doesn't know about it yet. `hatchet.register(task)` wires it in.

**Required before any `.run` / `.runNoWait` / `.schedule` call.** Calling those on an unregistered task is a defect (`Effect.die`), not a typed failure.

```ts
yield* hatchet.register(taskA)
yield* hatchet.register(taskB)
// or
yield* Effect.forEach([taskA, taskB], (t) => hatchet.register(t))
```

`register` captures the surrounding Effect runtime, so the task's `R` is satisfied by whichever layers are in scope **at registration time**, not at run time. If a task yields `Mailer`, make sure `Mailer.Default` (or equivalent) is provided before you call `register`.

Both layers require this — including `layerInMemory`, where it's how the in-memory engine learns about the task at all.

### Starting the worker

The Hatchet engine queues work; a **worker** pulls work off the queue and runs it. `hatchet.startWorker()` boots that worker for the current process.

- **`Hatchet.layer`: required.** Without it, registered tasks exist client-side but no worker pulls them — `task.run` will block waiting for output that never arrives. The call forks the worker loop and returns; the surrounding scope keeps it alive.
- **`Hatchet.layerInMemory`: no-op.** In-memory runs happen directly in the Effect runtime, so there's nothing to start. The call is safe — that's the point, so the same bootstrap code works under both layers.

Call once, after all registrations:

```ts
yield* hatchet.register(taskA)
yield* hatchet.register(taskB)
yield* hatchet.startWorker()
```

Tasks registered after `startWorker()` aren't picked up by that worker.

### Running tasks

Three call styles, all returning Effects that fail with `TaskExecutionFailure`:

```ts
// Wait for output
const result = yield* greet.run({ name: "world" })

// Fire-and-forget; get a handle you can await later
const handle = yield* greet.runNoWait({ name: "world" })
const later = yield* handle.output

// Enqueue for the future
const scheduled = yield* greet.schedule(
  new Date(Date.now() + 60_000),
  { name: "world" },
)
```

- **`run`** — blocks the Effect until the task produces output.
- **`runNoWait`** — returns immediately with a handle; `yield* handle.output` later if you want to await the result.
- **`schedule`** — enqueues for a future time, returns `{ id }`. Cancel with `hatchet.schedule.delete(id)`.

Input passes through the task's `input` schema (if any) before reaching `fn`. Output passes through the task's `output` schema before reaching the caller. Schema failures surface as `TaskExecutionFailure`.

Under `Hatchet.layer`, these dispatch through the Hatchet engine. Under `Hatchet.layerInMemory`, `run` invokes the task directly, `runNoWait` forks a daemon fiber, and `schedule` honors real wall-clock delays via `Effect.sleep` (use `TestClock` for time-dependent tests).

### Crons and schedules

A **cron** is a recurring trigger driven by a cron expression. A **schedule** is a one-shot enqueue at a specific time (what `task.schedule` creates).

```ts
const cron = yield* hatchet.cron.create({
  workflowName: greet.name,
  name: "daily-greet",
  expression: "0 9 * * *",
  input: { name: "world" },
  additionalMetadata: { tier: "free" }, // optional
})

const all = yield* hatchet.cron.list({ workflowName: greet.name })

yield* hatchet.cron.delete(cron.id)
yield* hatchet.schedule.delete(scheduled.id)
```

- `workflowName` is the target task's `name`. The task must be registered for the cron to fire under `Hatchet.layer`.
- `schedule.delete` swallows missing-ID errors under both layers — safe to call defensively.
- Under `layerInMemory`, crons are stored but don't auto-fire on a schedule — drive them manually from your test if you need to verify firing behavior.

### Errors

Tagged errors you can `Effect.catchTag` on:

| Error                  | Raised by                                     |
| ---------------------- | --------------------------------------------- |
| `TaskExecutionFailure` | `task.run`, `task.runNoWait`, `task.schedule` |
| `CronCreateError`      | `hatchet.cron.create`                         |
| `CronDeleteError`      | `hatchet.cron.delete`                         |
| `CronListError`        | `hatchet.cron.list`                           |
| `ScheduleDeleteError`  | `hatchet.schedule.delete`                     |

`TaskExecutionFailure.cause` carries the original error from your `fn` (typed failure, schema decode error, or unexpected throw).

### Worker affinity

To prefer running enqueued tasks on the same worker that submitted them:

```ts
Hatchet.layer({ runPrefersThisWorker: true })
```

The layer tags the worker with a per-process instance id and requires that label on dispatched runs. Useful when tasks need access to in-process state on the submitting worker.

## Status

Pre-1.0. Surface is shaped against production usage but expect changes as more Hatchet features (multi-step workflows, parent/child runs, richer `ctx`) get surfaced.
