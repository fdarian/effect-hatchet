# effect-hatchet

## 2.0.0

### Major Changes

- 9135f37: Require Effect v4. The `effect` peer dependency is now `^4.0.0-rc.111`.

## 1.1.0

### Minor Changes

- 472c5be: Add `Event.make` for typed event definitions, referenceable from `on: { event }` and `hatchet.event.push`.
- 5b93227: Add task concurrency, schedule listing, and cron test-firing; fix in-memory schedule cancellation.
- 472c5be: Add `hatchet.event.push`; event-triggered tasks (`on: { event }`) now fire under both `Hatchet.layer` and `Hatchet.layerInMemory()`.

## 1.0.1

### Patch Changes

- 7f84a8a: Export missing Task and error types
