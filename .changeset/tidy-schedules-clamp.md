---
"effect-hatchet": minor
---

`Hatchet.layerInMemory()`'s `schedule.list` now mirrors the real server's pagination contract exactly: `numPages` has no floor (zero results yield `numPages: 0`, not `1`), `currentPage` is computed as `1 + ceil(offset / limit)`, and `nextPage` is always populated — clamped back to `currentPage` on the last page instead of being omitted.

This is a deliberate fidelity fix, not a nicety: any consumer that terminates pagination by checking `nextPage == null` was already broken against the real Hatchet server (which never omits `nextPage`) and will now loop forever against the mock too, surfacing that latent bug in tests instead of masking it. Terminate on `nextPage <= currentPage` instead (and also stop on an empty `schedules` page, since the zero-result case never satisfies the clamp).
