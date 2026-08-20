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
import { Context, Effect, Layer, Schema } from "effect"
import { Hatchet, Task } from "effect-hatchet"

// Any service your task needs. Whatever you `yield*` inside `fn` lands in the
// task's requirements, and surfaces as a requirement of the layer below.
class Greeter extends Context.Service<Greeter>()("Greeter", {
  make: Effect.succeed({
    greet: (name: string) => Effect.succeed(`hello ${name}`),
  }),
}) {
  static readonly layer = Layer.effect(this, this.make)
}

// A task can fail with your own typed errors.
class NameTooLong extends Schema.TaggedError<NameTooLong>()("NameTooLong", {
  name: Schema.String,
}) {}

// Define a task using the `Task.make` api.
const greet = Task.make({
  // Every task needs a name unique to the engine.
  name: "greet",
  // Decoded before `fn` runs. Omit it and `fn` receives the input untyped.
  input: Schema.Struct({ name: Schema.String }),
  // Encoded before the result reaches the caller. Omit it and whatever `fn`
  // returns passes through verbatim.
  output: Schema.Struct({ message: Schema.String }),
  // The body of the task. `ctx` carries Hatchet's workflow run id.
  fn: (input, ctx) =>
    Effect.gen(function* () {
      if (input.name.length > 64) {
        return yield* new NameTooLong({ name: input.name })
      }
      const greeter = yield* Greeter
      yield* Effect.annotateLogs(Effect.log("Greeting"), { runId: ctx.runId })
      return { message: yield* greeter.greet(input.name) }
    }),
  // Everything below is optional and passed straight through to Hatchet.
  rateLimits: [{ key: "greet", units: 1 }],
  concurrency: { expression: "input.name", maxRuns: 1 },
  on: { event: "user:created" },
  durable: true,
})

const program = Effect.gen(function* () {
  // Run and wait for the output. Every failure — your typed error, an input
  // decode error, an unexpected throw — reaches the caller as
  // `TaskExecutionFailure`, with the original in `.cause`.
  const result = yield* greet.run({ name: "world" })
  yield* Effect.log(result.message) // "hello world"

  // Or don't wait: take a handle now, await the output whenever you want it.
  const handle = yield* greet.runNoWait({ name: "world" })
  const later = yield* handle.output

  // Or enqueue it for a specific time.
  const scheduled = yield* greet.schedule(new Date(Date.now() + 60_000), {
    name: "world",
  })

  // The `Hatchet` service itself covers everything that isn't a single run:
  // cancelling schedules, and cron expressions against a registered task name.
  const hatchet = yield* Hatchet
  yield* hatchet.schedule.delete(scheduled.id)
  yield* hatchet.cron.create({
    workflowName: greet.name,
    name: "daily-greet",
    expression: "0 9 * * *",
    input: { name: "world" },
  })
})

// Handing tasks to the layer is what registers them with the engine and boots
// this process's worker — in that order, once, at startup. A worker advertises
// its task set exactly once when it starts, so the set is fixed for the life
// of the process; taking it as data here is what makes a late registration
// unrepresentable rather than a run that enqueues and never gets picked up.
//
// Each task's own requirements become requirements of the layer, so the
// compiler holds you to providing them where the tasks are wired in.
//
// `Hatchet.layer` reads `HATCHET_CLIENT_TOKEN`, `HATCHET_CLIENT_HOST_PORT`,
// `HATCHET_CLIENT_API_URL` and `HATCHET_CLIENT_TLS_STRATEGY` through Effect
// `Config`. Swap it for `Hatchet.layerInMemory({ tasks: [greet] })` and the
// same program runs entirely in-process — no worker, no gRPC, no engine.
program.pipe(
  Effect.provide(
    Hatchet.layer({ tasks: [greet] }).pipe(Layer.provide(Greeter.layer)),
  ),
  Effect.runPromise,
)
```

## Test without a Hatchet engine

Swap `Hatchet.layer` for `Hatchet.layerInMemory` and the same program runs in-process — no worker, no gRPC, no engine.

```ts
import { Effect, Layer } from "effect"
import { expect, it } from "vitest"
import { Hatchet } from "effect-hatchet"

it("greets", () =>
  Effect.gen(function* () {
    const result = yield* greet.run({ name: "world" })
    expect(result.message).toBe("hello world")
  }).pipe(
    Effect.provide(
      Hatchet.layerInMemory({ tasks: [greet] }).pipe(
        Layer.provide(Greeter.layer),
      ),
    ),
    Effect.runPromise,
  ))
```

Nothing about the program changes — only which layer you provide. `layerInMemory` boots no worker, but it holds tasks to the same fixed-at-startup rule as the real engine, so a bootstrap that passes here can't hang in production. `task.schedule` honors real wall-clock delays via `Effect.sleep` — pair with `TestClock` for time-dependent tests.

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
  concurrency: { expression: "input.to", maxRuns: 1 },
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

const firstPage = yield* hatchet.schedule.list({
  statuses: ["SCHEDULED"], // optional
  offset: 0,               // optional
  limit: 50,               // optional
})

yield* hatchet.cron.delete(cron.id)
yield* hatchet.schedule.delete(scheduled.id)
```

- `workflowName` is the target task's `name`. The task must be registered for the cron to fire under `Hatchet.layer`.
- `schedule.delete` swallows missing-ID errors under both layers — safe to call defensively.
- `schedule.list` makes exactly one call and returns one page — it does not accumulate every schedule for you. The result is `{ schedules, pagination? }`: `schedules` is that page's `ScheduledRun[]`, and `pagination` (when the source reports it) is `{ currentPage?, nextPage?, numPages? }`, all 1-indexed. Keep calling with `offset` advanced to `(pagination.nextPage - 1) * limit` while `pagination.nextPage` is defined to walk every page yourself. `statuses` filters by `ScheduledRunStatus` (`PENDING`, `RUNNING`, `SUCCEEDED`, `FAILED`, `CANCELLED`, `QUEUED`, `SCHEDULED`); results are ordered by `triggerAt`.
- Under `layerInMemory`, a schedule whose run hasn't fired yet reports `workflowRunStatus: "SCHEDULED"` — the same default that `statuses: ["SCHEDULED"]` filters on, so matching entries always carry that value rather than leaving the field absent. `offset`/`limit` and the returned `pagination` follow the same 1-indexed semantics as `Hatchet.layer`, so pagination logic written against one layer works against the other.
- Under `layerInMemory`, crons are stored but don't auto-fire on a schedule. Use `hatchet.cron._testFire(cron.id)` in tests to manually fire a registered cron's task; it's in-memory only and dies under `Hatchet.layer`.

### Events

An **event** is a named trigger: pushing one fires every task registered with `on: { event: <key> }` for that key.

```ts
const UserCreated = Event.make({
  key: "user:created",
  payload: S.Struct({ userId: S.String }),
})

const onUserCreated = Task.make({
  name: "on-user-created",
  input: UserCreated.payload,   // schema written once, referenced here
  on: { event: UserCreated },   // typed reference, not a bare string
  fn: (input) => Effect.succeed(`welcomed ${input.userId}`),
})

yield* hatchet.register(onUserCreated)
yield* hatchet.startWorker()

yield* hatchet.event.push(UserCreated, { userId: "user-1" })
```

- `Event.make`'s `key` is the wire key events are pushed and matched on; `payload` is decoded before `fn` runs, same as `Task.make`'s own `input`.
- `on: { event }` and `event.push`'s first argument both accept the typed `Event` or a plain string key — mix and match freely.

<details>
<summary>Using a plain string key also works</summary>

```ts
const onUserCreated = Task.make({
  name: "on-user-created",
  input: S.Struct({ userId: S.String }),
  on: { event: "user:created" },
  fn: (input) => Effect.succeed(`welcomed ${input.userId}`),
})

yield* hatchet.register(onUserCreated)
yield* hatchet.startWorker()

yield* hatchet.event.push("user:created", { userId: "user-1" })
```
</details>

- `event.push` is fire-and-forget under both layers: it returns `{ id }` immediately without waiting for the triggered run(s) to finish, matching real Hatchet's own semantics. Await `onUserCreated.run(...)` directly if you need the result.
- Pushing a key with no registered listeners is a no-op, not an error.
- Unlike crons, event-triggered tasks fire under `layerInMemory` too — no `_testFire`-style workaround needed.

### Errors

Tagged errors you can `Effect.catchTag` on:

| Error                  | Raised by                                     |
| ---------------------- | --------------------------------------------- |
| `TaskExecutionFailure` | `task.run`, `task.runNoWait`, `task.schedule` |
| `CronCreateError`      | `hatchet.cron.create`                         |
| `CronDeleteError`      | `hatchet.cron.delete`                         |
| `CronListError`        | `hatchet.cron.list`                           |
| `ScheduleListError`    | `hatchet.schedule.list`                       |
| `ScheduleDeleteError`  | `hatchet.schedule.delete`                     |
| `EventPushError`       | `hatchet.event.push`                          |

`TaskExecutionFailure.cause` carries the original error from your `fn` (typed failure, schema decode error, or unexpected throw).

### Worker affinity

To prefer running enqueued tasks on the same worker that submitted them:

```ts
Hatchet.layer({ runPrefersThisWorker: true })
```

The layer tags the worker with a per-process instance id and requires that label on dispatched runs. Useful when tasks need access to in-process state on the submitting worker.

## Development

`pnpm test` runs the in-memory suite — no Docker required. `pnpm test:real` runs the same portable tests against a real `hatchet-lite` server, booted in Docker via testcontainers; it requires a running Docker daemon and is not part of the default `pnpm test`.

## Status

Pre-1.0. Surface is shaped against production usage but expect changes as more Hatchet features (multi-step workflows, parent/child runs, richer `ctx`) get surfaced.
