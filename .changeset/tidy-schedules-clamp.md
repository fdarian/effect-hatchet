---
"effect-hatchet": minor
---

`layerInMemory`'s `schedule.list` pagination now matches the real Hatchet server: `nextPage` is always present (clamped to `currentPage` on the last page, not omitted) and `numPages` is no longer floored at 1 — so loops that terminate on `nextPage == null` will now hang.
