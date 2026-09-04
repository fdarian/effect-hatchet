import { it } from "@effect/vitest";
import { Cause, Deferred, Effect, Exit, Layer, Schema as S } from "effect";
import { TestClock } from "effect/testing";
import { expect } from "vitest";
import { Task, TaskExecutionFailure } from "../src/core/task.js";
import { Hatchet } from "../src/index.js";
import { registerSharedHatchetTests } from "./shared-suite.js";

const HatchetTest = Hatchet.layerInMemory();

it.layer(HatchetTest)("Hatchet (in-memory)", (it) => {
	registerSharedHatchetTests(it);

	// -------------------------------------------------------------------------
	// schedule.list returns a page of tracked schedules, optionally filtered by
	// status, with pagination metadata the caller can use to keep going
	//
	// In-memory only: asserts on `numPages`/whole-tenant contents, which only
	// hold for a fresh isolated in-memory Map.
	// -------------------------------------------------------------------------

	it.effect(
		"schedule.list returns a page filtered by status, with pagination metadata",
		() =>
			Effect.gen(function* () {
				const noop = Task.make({
					name: "noop-scheduled-list",
					fn: () => Effect.succeed(null),
				});

				const hatchet = yield* Hatchet;
				yield* hatchet.register(noop);

				const scheduled = yield* noop.schedule(
					new Date(Date.now() + 60_000),
					{},
				);

				const all = yield* hatchet.schedule.list();
				const found = all.schedules.find((entry) => entry.id === scheduled.id);
				expect(found).toBeDefined();
				expect(found?.workflowRunStatus).toBe("SCHEDULED");
				expect(all.pagination?.currentPage).toBe(1);
				expect(all.pagination?.numPages).toBe(1);
				// nextPage is clamped to currentPage on the last page, not
				// omitted -- mirrors the real server exactly (see the comment
				// above the pagination math in src/impl/in-memory.ts).
				expect(all.pagination?.nextPage).toBe(1);

				const pending = yield* hatchet.schedule.list({
					statuses: ["SCHEDULED"],
				});
				const foundPending = pending.schedules.find(
					(entry) => entry.id === scheduled.id,
				);
				expect(foundPending).toBeDefined();
				expect(foundPending?.workflowRunStatus).toBe("SCHEDULED");

				const succeeded = yield* hatchet.schedule.list({
					statuses: ["SUCCEEDED"],
				});
				expect(
					succeeded.schedules.some((entry) => entry.id === scheduled.id),
				).toBe(false);

				yield* hatchet.schedule.delete(scheduled.id);
				const afterDelete = yield* hatchet.schedule.list();
				expect(
					afterDelete.schedules.some((entry) => entry.id === scheduled.id),
				).toBe(false);
			}),
	);

	// -------------------------------------------------------------------------
	// schedule.list lets the caller drive pagination via offset/limit —
	// it doesn't accumulate pages itself. `nextPage` is clamped to
	// `currentPage` on the last page rather than omitted, so the walk below
	// terminates on `nextPage > currentPage` (not `nextPage !== undefined`,
	// which would never turn false).
	//
	// In-memory only: same whole-tenant-contents caveat as above.
	// -------------------------------------------------------------------------

	it.effect("schedule.list pages by offset/limit, driven by the caller", () =>
		Effect.gen(function* () {
			const noop = Task.make({
				name: "noop-scheduled-paging",
				fn: () => Effect.succeed(null),
			});

			const hatchet = yield* Hatchet;
			yield* hatchet.register(noop);

			const ids: string[] = [];
			for (let i = 0; i < 3; i++) {
				const scheduled = yield* noop.schedule(
					new Date(Date.now() + 60_000 + i * 1_000),
					{},
				);
				ids.push(scheduled.id);
			}

			const limit = 1;
			let page = yield* hatchet.schedule.list({ offset: 0, limit });
			expect(page.schedules.length).toBe(1);
			expect(page.pagination?.currentPage).toBe(1);
			expect(page.pagination?.numPages).toBe(3);

			const collected = page.schedules.map((entry) => entry.id);
			while (
				page.schedules.length > 0 &&
				page.pagination?.nextPage !== undefined &&
				page.pagination.currentPage !== undefined &&
				page.pagination.nextPage > page.pagination.currentPage
			) {
				const offset = (page.pagination.nextPage - 1) * limit;
				page = yield* hatchet.schedule.list({ offset, limit });
				expect(page.schedules.length).toBeLessThanOrEqual(1);
				collected.push(...page.schedules.map((entry) => entry.id));
			}

			expect(collected.sort()).toEqual([...ids].sort());
		}),
	);

	// -------------------------------------------------------------------------
	// schedule.list's pagination arithmetic matches the real server exactly,
	// warts included: `numPages` has no floor (zero results -> 0 pages, not
	// 1), and `nextPage` is always populated -- clamped to `currentPage` on
	// the last page, never omitted. See the comment above the pagination
	// math in src/impl/in-memory.ts for the server-source reference.
	//
	// Each case below provides its own fresh in-memory instance via
	// `Layer.fresh` (plain `Effect.provide` would reuse the file's shared
	// `it.layer` instance, since it's the same Layer reference) so the
	// schedule counts line up with the assertions exactly, unaffected by
	// schedules created in other tests in this file.
	// -------------------------------------------------------------------------

	it.effect("schedule.list pagination shape for a single short page", () =>
		Effect.gen(function* () {
			const noop = Task.make({
				name: "noop-scheduled-single-page",
				fn: () => Effect.succeed(null),
			});

			const hatchet = yield* Hatchet;
			yield* hatchet.register(noop);
			for (let i = 0; i < 3; i++) {
				yield* noop.schedule(new Date(Date.now() + 60_000 + i * 1_000), {});
			}

			const page = yield* hatchet.schedule.list({ offset: 0, limit: 50 });
			expect(page.schedules.length).toBe(3);
			expect(page.pagination?.numPages).toBe(1);
			expect(page.pagination?.currentPage).toBe(1);
			expect(page.pagination?.nextPage).toBe(1);
		}).pipe(Effect.provide(Layer.fresh(Hatchet.layerInMemory()))),
	);

	it.effect("schedule.list pagination shape for zero results", () =>
		Effect.gen(function* () {
			const hatchet = yield* Hatchet;

			const page = yield* hatchet.schedule.list({ offset: 0, limit: 50 });
			expect(page.schedules.length).toBe(0);
			expect(page.pagination?.numPages).toBe(0);
			expect(page.pagination?.currentPage).toBe(1);
			// The clamp (nextPage === currentPage) never fires when there's no
			// last page to land on, so nextPage just keeps counting up -- 2
			// here, 3 on the next call at the same offset, and so on. That's
			// the real server's behavior, reproduced deliberately.
			expect(page.pagination?.nextPage).toBe(2);
		}).pipe(Effect.provide(Layer.fresh(Hatchet.layerInMemory()))),
	);

	it.effect(
		"schedule.list walks every page of 120 schedules, terminating on the nextPage clamp",
		() =>
			Effect.gen(function* () {
				const noop = Task.make({
					name: "noop-scheduled-full-walk",
					fn: () => Effect.succeed(null),
				});

				const hatchet = yield* Hatchet;
				yield* hatchet.register(noop);

				const ids: string[] = [];
				for (let i = 0; i < 120; i++) {
					const scheduled = yield* noop.schedule(
						new Date(Date.now() + 60_000 + i * 1_000),
						{},
					);
					ids.push(scheduled.id);
				}

				const limit = 50;
				const expectedByOffset = new Map<number, [number, number, number]>([
					[0, [3, 1, 2]],
					[50, [3, 2, 3]],
					[100, [3, 3, 3]],
				]);

				const collected: string[] = [];
				let offset = 0;
				let page = yield* hatchet.schedule.list({ offset, limit });

				while (true) {
					const expected = expectedByOffset.get(offset);
					if (expected === undefined) {
						throw new Error(
							`no expected pagination fixture for offset ${offset}`,
						);
					}
					const [numPages, currentPage, nextPage] = expected;
					expect(page.pagination?.numPages).toBe(numPages);
					expect(page.pagination?.currentPage).toBe(currentPage);
					expect(page.pagination?.nextPage).toBe(nextPage);
					collected.push(...page.schedules.map((entry) => entry.id));

					if (
						page.schedules.length === 0 ||
						page.pagination?.nextPage === undefined ||
						page.pagination.currentPage === undefined ||
						page.pagination.nextPage <= page.pagination.currentPage
					) {
						break;
					}
					offset = (page.pagination.nextPage - 1) * limit;
					page = yield* hatchet.schedule.list({ offset, limit });
				}

				expect(collected.sort()).toEqual([...ids].sort());
			}).pipe(Effect.provide(Layer.fresh(Hatchet.layerInMemory()))),
	);

	// -------------------------------------------------------------------------
	// fn failure surfaces as TaskExecutionFailure with cause populated
	//
	// In-memory only: `cause instanceof MyError` cannot survive the round trip
	// through a real server.
	// -------------------------------------------------------------------------

	it.effect("fn failure surfaces as TaskExecutionFailure with cause", () =>
		Effect.gen(function* () {
			class MyError extends S.TaggedError<MyError>()("MyError", {
				reason: S.String,
			}) {}

			const failing = Task.make({
				name: "failing-fn",
				fn: () => Effect.fail(new MyError({ reason: "boom" })),
			});

			const hatchet = yield* Hatchet;
			yield* hatchet.register(failing);
			const exit = yield* Effect.exit(failing.run({}));

			expect(Exit.isFailure(exit)).toBe(true);
			if (Exit.isFailure(exit)) {
				const failures = exit.cause.reasons
					.filter(Cause.isFailReason)
					.map((r) => r.error);
				expect(failures.length).toBe(1);
				const failure = failures[0];
				expect(failure).toBeInstanceOf(TaskExecutionFailure);
				expect((failure as TaskExecutionFailure).cause).toBeInstanceOf(MyError);
			}
		}),
	);

	// -------------------------------------------------------------------------
	// cron._testFire manually fires a registered cron's task
	//
	// In-memory only: dies under `Hatchet.layer` ("only available under
	// layerInMemory").
	// -------------------------------------------------------------------------

	it.effect("cron._testFire fires the cron's registered task", () =>
		Effect.gen(function* () {
			const deferred = yield* Deferred.make<true>();

			const greet = Task.make({
				name: "greet-test-fire",
				fn: () =>
					Deferred.succeed(deferred, true as const).pipe(Effect.as("done")),
			});

			const hatchet = yield* Hatchet;
			yield* hatchet.register(greet);

			const cron = yield* hatchet.cron.create({
				workflowName: greet.name,
				name: "test-fire-cron",
				expression: "0 9 * * *",
				input: {},
			});

			const before = yield* Deferred.poll(deferred);
			expect(before._tag).toBe("None");

			yield* hatchet.cron._testFire(cron.id);

			const result = yield* Deferred.await(deferred);
			expect(result).toBe(true);
		}),
	);

	it.effect("cron._testFire on an unregistered cron ID is a defect", () =>
		Effect.gen(function* () {
			const hatchet = yield* Hatchet;
			const exit = yield* Effect.exit(hatchet.cron._testFire("no-such-cron"));

			expect(Exit.isFailure(exit)).toBe(true);
			if (Exit.isFailure(exit)) {
				const defects = exit.cause.reasons
					.filter(Cause.isDieReason)
					.map((r) => r.defect);
				expect(defects.length).toBe(1);
				expect(String(defects[0])).toContain("Missing local cron");
			}
		}),
	);

	// -------------------------------------------------------------------------
	// startWorker() is a no-op under layerInMemory
	//
	// In-memory only: the premise ("is a no-op") is false for live.
	// -------------------------------------------------------------------------

	it.effect("startWorker is a no-op under layerInMemory", () =>
		Effect.gen(function* () {
			const noop = Task.make({
				name: "noop-worker",
				fn: () => Effect.succeed("ok"),
			});

			const hatchet = yield* Hatchet;
			yield* hatchet.register(noop);
			yield* hatchet.startWorker(); // must not throw or error
			const result = yield* noop.run({});
			expect(result).toBe("ok");
		}),
	);

	// -------------------------------------------------------------------------
	// schedule fires after delay (TestClock)
	//
	// In-memory only: live scheduling is driven by real server wall-clock,
	// TestClock.adjust can't move it.
	// -------------------------------------------------------------------------

	it.effect("schedule fires after delay using TestClock", () =>
		Effect.gen(function* () {
			const deferred = yield* Deferred.make<true>();

			const delayed = Task.make({
				name: "delayed-task",
				fn: () =>
					Effect.gen(function* () {
						yield* Deferred.succeed(deferred, true as const);
						return "done";
					}),
			});

			const hatchet = yield* Hatchet;
			yield* hatchet.register(delayed);

			const enqueueAt = new Date(Date.now() + 60_000);
			yield* delayed.schedule(enqueueAt, {});

			// Task should not have fired yet
			const before = yield* Deferred.poll(deferred);
			expect(before._tag).toBe("None");

			// Advance clock by 1 minute — triggers the sleep inside schedule
			yield* TestClock.adjust("1 minutes");

			// Now the deferred should be resolved
			const result = yield* Deferred.await(deferred);
			expect(result).toBe(true);
		}),
	);

	// -------------------------------------------------------------------------
	// deleting a schedule cancels it — it must not fire (TestClock)
	//
	// In-memory only: same TestClock caveat as above.
	// -------------------------------------------------------------------------

	it.effect("schedule.delete cancels a pending schedule before it fires", () =>
		Effect.gen(function* () {
			const deferred = yield* Deferred.make<true>();

			const delayed = Task.make({
				name: "delayed-task-cancelled",
				fn: () =>
					Effect.gen(function* () {
						yield* Deferred.succeed(deferred, true as const);
						return "done";
					}),
			});

			const hatchet = yield* Hatchet;
			yield* hatchet.register(delayed);

			const enqueueAt = new Date(Date.now() + 60_000);
			const scheduled = yield* delayed.schedule(enqueueAt, {});

			// Cancel before the trigger time elapses
			yield* hatchet.schedule.delete(scheduled.id);

			// Advance the clock well past the trigger time
			yield* TestClock.adjust("1 minutes");

			// The task must never have fired
			const after = yield* Deferred.poll(deferred);
			expect(after._tag).toBe("None");
		}),
	);
});
