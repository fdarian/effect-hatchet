---
"effect-hatchet": patch
---

Fail cancelled `task.run` and `handle.output` with `TaskExecutionFailure` whose cause is `RunCancelled({ runId })` in both layers, instead of resolving `{}` on real Hatchet or propagating interruption in memory.

Poll live run status with bounded exponential backoff after SDK result resolution to account for delayed terminal status updates. Fail safely on failed runs, lookup errors, or a 30-second status deadline.
