---
"effect-hatchet": minor
---

`Hatchet.register` no longer requires `Scope`. Fibers that run task handlers, and the worker started by `startWorker`, now live until the Hatchet layer is released, at which point they are interrupted.
