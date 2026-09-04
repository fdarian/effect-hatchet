---
"effect-hatchet": minor
---

`layerInMemory`'s `schedule.list` pagination now matches the real Hatchet server exactly: no floor on `numPages`, and `nextPage` is always present — clamped back to `currentPage` on the last page instead of omitted.

Consumers that end pagination on `nextPage == null` will now loop forever against the mock. Terminate on `nextPage > currentPage`, plus a guard on an empty `schedules` page.
