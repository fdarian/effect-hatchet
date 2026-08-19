import {
	Cause,
	Deferred,
	Effect,
	Exit,
	Schema as S,
	type Scope,
	TestClock,
	TestContext,
} from "effect";
import { expect, it } from "vitest";
import { Task, TaskExecutionFailure } from "../src/core/task.js";
import { Hatchet } from "../src/index.js";

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

const run = <A>(effect: Effect.Effect<A, unknown, never>) =>
	Effect.runPromise(effect);

const runExit = <A>(effect: Effect.Effect<A, unknown, never>) =>
	Effect.runPromiseExit(effect);

const withHatchet = <A, E>(
	effect: Effect.Effect<A, E, Hatchet | Scope.Scope>,
): Effect.Effect<A, E, never> =>
	effect.pipe(Effect.provide(Hatchet.layerInMemory()), Effect.scoped);

// ---------------------------------------------------------------------------
// Basic: input + output schemas
// ---------------------------------------------------------------------------

it("registers and runs a task with input and output schemas", async () => {
	const greet = Task.make({
		name: "greet",
		input: S.Struct({ name: S.String }),
		output: S.Struct({ message: S.String }),
		fn: (input) => Effect.succeed({ message: `hello ${input.name}` }),
	});

	const result = await run(
		withHatchet(
			Effect.gen(function* () {
				const hatchet = yield* Hatchet;
				yield* hatchet.register(greet);
				return yield* greet.run({ name: "world" });
			}),
		),
	);

	expect(result.message).toBe("hello world");
});

// ---------------------------------------------------------------------------
// Basic: no input schema (input is unknown)
// ---------------------------------------------------------------------------

it("registers and runs a task with no input schema", async () => {
	const echo = Task.make({
		name: "echo-no-input",
		fn: (input) => Effect.succeed({ received: input }),
	});

	const result = await run(
		withHatchet(
			Effect.gen(function* () {
				const hatchet = yield* Hatchet;
				yield* hatchet.register(echo);
				return yield* echo.run({ anything: true });
			}),
		),
	);

	expect(result).toEqual({ received: { anything: true } });
});

// ---------------------------------------------------------------------------
// Basic: no output schema (passes through verbatim)
// ---------------------------------------------------------------------------

it("registers and runs a task with no output schema", async () => {
	const compute = Task.make({
		name: "compute-no-output",
		input: S.Struct({ x: S.Number }),
		fn: (input) => Effect.succeed({ doubled: input.x * 2 }),
	});

	const result = await run(
		withHatchet(
			Effect.gen(function* () {
				const hatchet = yield* Hatchet;
				yield* hatchet.register(compute);
				return yield* compute.run({ x: 7 });
			}),
		),
	);

	expect(result).toEqual({ doubled: 14 });
});

// ---------------------------------------------------------------------------
// concurrency option is accepted and ignored under layerInMemory
// ---------------------------------------------------------------------------

it("registers and runs a task with a concurrency option", async () => {
	const limited = Task.make({
		name: "limited-concurrency",
		input: S.Struct({ x: S.Number }),
		output: S.Struct({ doubled: S.Number }),
		fn: (input) => Effect.succeed({ doubled: input.x * 2 }),
		concurrency: { expression: "input.x", maxRuns: 1 },
	});

	const result = await run(
		withHatchet(
			Effect.gen(function* () {
				const hatchet = yield* Hatchet;
				yield* hatchet.register(limited);
				return yield* limited.run({ x: 5 });
			}),
		),
	);

	expect(result.doubled).toBe(10);
});

// ---------------------------------------------------------------------------
// runNoWait returns a handle whose output resolves
// ---------------------------------------------------------------------------

it("runNoWait returns a handle whose output resolves", async () => {
	const add = Task.make({
		name: "add",
		input: S.Struct({ a: S.Number, b: S.Number }),
		output: S.Struct({ sum: S.Number }),
		fn: (input) => Effect.succeed({ sum: input.a + input.b }),
	});

	const result = await run(
		withHatchet(
			Effect.gen(function* () {
				const hatchet = yield* Hatchet;
				yield* hatchet.register(add);
				const handle = yield* add.runNoWait({ a: 3, b: 4 });
				return yield* handle.output;
			}),
		),
	);

	expect(result.sum).toBe(7);
});

// ---------------------------------------------------------------------------
// schedule returns { id }; schedule.delete works
// ---------------------------------------------------------------------------

it("schedule returns an id and schedule.delete is idempotent", async () => {
	const noop = Task.make({
		name: "noop-scheduled",
		fn: () => Effect.succeed(null),
	});

	await run(
		withHatchet(
			Effect.gen(function* () {
				const hatchet = yield* Hatchet;
				yield* hatchet.register(noop);

				const scheduled = yield* noop.schedule(
					new Date(Date.now() + 60_000),
					{},
				);
				expect(typeof scheduled.id).toBe("string");
				expect(scheduled.id.length).toBeGreaterThan(0);

				// idempotent delete
				yield* hatchet.schedule.delete(scheduled.id);
				yield* hatchet.schedule.delete(scheduled.id); // second call must not fail
			}),
		),
	);
});

// ---------------------------------------------------------------------------
// schedule.list returns a page of tracked schedules, optionally filtered by
// status, with pagination metadata the caller can use to keep going
// ---------------------------------------------------------------------------

it("schedule.list returns a page filtered by status, with pagination metadata", async () => {
	const noop = Task.make({
		name: "noop-scheduled-list",
		fn: () => Effect.succeed(null),
	});

	await run(
		withHatchet(
			Effect.gen(function* () {
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
				expect(all.pagination?.nextPage).toBeUndefined();

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
		),
	);
});

// ---------------------------------------------------------------------------
// schedule.list lets the caller drive pagination via offset/limit and
// pagination.nextPage — it doesn't accumulate pages itself
// ---------------------------------------------------------------------------

it("schedule.list pages by offset/limit, driven by the caller", async () => {
	const noop = Task.make({
		name: "noop-scheduled-paging",
		fn: () => Effect.succeed(null),
	});

	await run(
		withHatchet(
			Effect.gen(function* () {
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
				while (page.pagination?.nextPage !== undefined) {
					const offset = (page.pagination.nextPage - 1) * limit;
					page = yield* hatchet.schedule.list({ offset, limit });
					expect(page.schedules.length).toBeLessThanOrEqual(1);
					collected.push(...page.schedules.map((entry) => entry.id));
				}

				expect(collected.sort()).toEqual([...ids].sort());
			}),
		),
	);
});

// ---------------------------------------------------------------------------
// Schema decode error surfaces as TaskExecutionFailure
// ---------------------------------------------------------------------------

it("input schema decode error surfaces as TaskExecutionFailure", async () => {
	const typed = Task.make({
		name: "typed-input",
		input: S.Struct({ count: S.Number }),
		fn: (input) => Effect.succeed({ doubled: input.count * 2 }),
	});

	const exit = await runExit(
		withHatchet(
			Effect.gen(function* () {
				const hatchet = yield* Hatchet;
				yield* hatchet.register(typed);
				return yield* typed.run({
					count: "not-a-number" as unknown as number,
				});
			}),
		),
	);

	expect(Exit.isFailure(exit)).toBe(true);
	if (Exit.isFailure(exit)) {
		const failures = [...Cause.failures(exit.cause)];
		expect(failures.length).toBe(1);
		expect(failures[0]).toBeInstanceOf(TaskExecutionFailure);
	}
});

// ---------------------------------------------------------------------------
// fn failure surfaces as TaskExecutionFailure with cause populated
// ---------------------------------------------------------------------------

it("fn failure surfaces as TaskExecutionFailure with cause", async () => {
	class MyError extends S.TaggedError<MyError>()("MyError", {
		reason: S.String,
	}) {}

	const failing = Task.make({
		name: "failing-fn",
		fn: () => Effect.fail(new MyError({ reason: "boom" })),
	});

	const exit = await runExit(
		withHatchet(
			Effect.gen(function* () {
				const hatchet = yield* Hatchet;
				yield* hatchet.register(failing);
				return yield* failing.run({});
			}),
		),
	);

	expect(Exit.isFailure(exit)).toBe(true);
	if (Exit.isFailure(exit)) {
		const failures = [...Cause.failures(exit.cause)];
		expect(failures.length).toBe(1);
		const failure = failures[0];
		expect(failure).toBeInstanceOf(TaskExecutionFailure);
		expect((failure as TaskExecutionFailure).cause).toBeInstanceOf(MyError);
	}
});

// ---------------------------------------------------------------------------
// Unregistered task is a defect (die), not a typed failure
// ---------------------------------------------------------------------------

it("running an unregistered task is a defect, not a typed failure", async () => {
	const ghost = Task.make({
		name: "ghost",
		fn: () => Effect.succeed("never"),
	});

	const exit = await runExit(
		withHatchet(
			Effect.gen(function* () {
				return yield* ghost.run({});
			}),
		),
	);

	expect(Exit.isFailure(exit)).toBe(true);
	if (Exit.isFailure(exit)) {
		// Must be a defect, not a typed failure
		const defects = [...Cause.defects(exit.cause)];
		expect(defects.length).toBe(1);
		expect(String(defects[0])).toContain("Missing task");
		// Must have NO typed failures
		const failures = [...Cause.failures(exit.cause)];
		expect(failures.length).toBe(0);
	}
});

// ---------------------------------------------------------------------------
// cron round-trip: create → list → delete
// ---------------------------------------------------------------------------

it("cron create → list → delete round-trip", async () => {
	const greet = Task.make({
		name: "greet-cron",
		input: S.Struct({ name: S.String }),
		fn: (input) => Effect.succeed({ message: `hello ${input.name}` }),
	});

	await run(
		withHatchet(
			Effect.gen(function* () {
				const hatchet = yield* Hatchet;
				yield* hatchet.register(greet);

				const cron = yield* hatchet.cron.create({
					workflowName: greet.name,
					name: "daily-greet",
					expression: "0 9 * * *",
					input: { name: "world" },
					additionalMetadata: { tier: "free" },
				});
				expect(typeof cron.id).toBe("string");

				const listed = yield* hatchet.cron.list({
					workflowName: greet.name,
				});
				expect(listed.length).toBe(1);
				expect(listed[0]?.id).toBe(cron.id);
				expect(listed[0]?.expression).toBe("0 9 * * *");
				expect(listed[0]?.workflowName).toBe(greet.name);

				yield* hatchet.cron.delete(cron.id);

				const afterDelete = yield* hatchet.cron.list({
					workflowName: greet.name,
				});
				expect(afterDelete.length).toBe(0);
			}),
		),
	);
});

// ---------------------------------------------------------------------------
// cron._testFire manually fires a registered cron's task
// ---------------------------------------------------------------------------

it("cron._testFire fires the cron's registered task", async () => {
	const result = await run(
		withHatchet(
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

				return yield* Deferred.await(deferred);
			}),
		),
	);

	expect(result).toBe(true);
});

it("cron._testFire on an unregistered cron ID is a defect", async () => {
	const exit = await runExit(
		withHatchet(
			Effect.gen(function* () {
				const hatchet = yield* Hatchet;
				return yield* hatchet.cron._testFire("no-such-cron");
			}),
		),
	);

	expect(Exit.isFailure(exit)).toBe(true);
	if (Exit.isFailure(exit)) {
		const defects = [...Cause.defects(exit.cause)];
		expect(defects.length).toBe(1);
		expect(String(defects[0])).toContain("Missing local cron");
	}
});

// ---------------------------------------------------------------------------
// R-requirement satisfied by layers in scope at register-time
// ---------------------------------------------------------------------------

class Mailer extends Effect.Service<Mailer>()("Mailer", {
	succeed: {
		send: (to: string) => Effect.succeed(`id-for-${to}`),
	},
}) {}

it("task R-requirement is satisfied by layers at register-time", async () => {
	const sendEmail = Task.make({
		name: "send-email",
		input: S.Struct({ to: S.String }),
		output: S.Struct({ messageId: S.String }),
		fn: (input) =>
			Effect.gen(function* () {
				const mailer = yield* Mailer;
				const id = yield* mailer.send(input.to);
				return { messageId: id };
			}),
	});

	const result = await run(
		Effect.gen(function* () {
			const hatchet = yield* Hatchet;
			yield* hatchet.register(sendEmail);
			return yield* sendEmail.run({ to: "alice@example.com" });
		}).pipe(
			Effect.provide(Mailer.Default),
			Effect.provide(Hatchet.layerInMemory()),
			Effect.scoped,
		),
	);

	expect(result.messageId).toBe("id-for-alice@example.com");
});

// ---------------------------------------------------------------------------
// startWorker() is a no-op under layerInMemory
// ---------------------------------------------------------------------------

it("startWorker is a no-op under layerInMemory", async () => {
	const noop = Task.make({
		name: "noop-worker",
		fn: () => Effect.succeed("ok"),
	});

	await run(
		withHatchet(
			Effect.gen(function* () {
				const hatchet = yield* Hatchet;
				yield* hatchet.register(noop);
				yield* hatchet.startWorker(); // must not throw or error
				const result = yield* noop.run({});
				expect(result).toBe("ok");
			}),
		),
	);
});

// ---------------------------------------------------------------------------
// schedule fires after delay (TestClock)
// ---------------------------------------------------------------------------

it("schedule fires after delay using TestClock", async () => {
	await Effect.runPromise(
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

			yield* Effect.gen(function* () {
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
			}).pipe(Effect.provide(Hatchet.layerInMemory()), Effect.scoped);
		}).pipe(Effect.provide(TestContext.TestContext)),
	);
});

// ---------------------------------------------------------------------------
// deleting a schedule cancels it — it must not fire (TestClock)
// ---------------------------------------------------------------------------

it("schedule.delete cancels a pending schedule before it fires", async () => {
	await Effect.runPromise(
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

			yield* Effect.gen(function* () {
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
			}).pipe(Effect.provide(Hatchet.layerInMemory()), Effect.scoped);
		}).pipe(Effect.provide(TestContext.TestContext)),
	);
});
