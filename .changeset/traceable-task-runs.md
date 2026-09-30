---
"effect-hatchet": minor
---

Expose `runId` alongside the existing `output` Effect on `task.runNoWait(input)` handles, so callers can log and trace enqueued Hatchet runs. The in-memory implementation provides unique synthetic run IDs.
