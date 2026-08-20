---
"effect-hatchet": major
---

Task registration moves into the layer. `Hatchet.layer` and `Hatchet.layerInMemory` now take the tasks they should serve, and handle registration and worker startup themselves.

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

- `hatchet.register(task)` and `hatchet.startWorker()` are removed from the `Hatchet` interface. Pass tasks to the layer instead; it registers them and then starts the worker, in that order, once.
- `Hatchet.layerInMemory` takes the same `{ tasks }` option and is now a function of options rather than a bare layer.
- `Effect.scoped` is no longer needed at the call site. The layer owns the scope that registration and the worker fiber live in.
- A task's own requirements become requirements of the layer, so providing them is now checked by the compiler instead of documented in prose.

Why: a Hatchet worker advertises its task set to the engine exactly once when it starts, and neither reconnects nor heartbeats carry that list again. Registering after `startWorker()` therefore produced a run that enqueued successfully and then waited forever for a worker that never claimed it. Taking the task set as data makes that state unrepresentable.

`layerInMemory` enforces the same fixed-at-startup rule even though nothing in-process requires it, so a bootstrap that passes in tests cannot hang against a real engine.
