import type { Vitest } from "@effect/vitest";
import {
	Cause,
	Context,
	Deferred,
	Effect,
	Exit,
	Fiber,
	Layer,
	Ref,
	Schema as S,
	Schedule,
	Stream,
} from "effect";
import { TestClock } from "effect/testing";
import { expect } from "vitest";
import { Event } from "../src/core/event.js";
import type { RunStatus } from "../src/core/runs.js";
import { Task, TaskExecutionFailure } from "../src/core/task.js";
import { Hatchet, RunCancelled } from "../src/index.js";

class Mailer extends Context.Service<Mailer>()("Mailer", {
	make: Effect.succeed({
		send: (to: string) => Effect.succeed(`id-for-${to}`),
	}),
}) {
	static readonly layer = Layer.effect(this, this.make);
}

/**
 * Tests that hold under both `Hatchet.layerInMemory()` and
 * `layerRealHatchet` — registered here once and run against `it` scoped to
 * each layer in `tests/hatchet.test.ts` and `tests/hatchet.real.test.ts`.
 */
export function registerSharedHatchetTests(it: Vitest.MethodsNonLive<Hatchet>) {
	const awaitStatus = (hatchet: Hatchet, runId: string, expected: RunStatus) =>
		hatchet.runs.getStatus(runId).pipe(
			Effect.flatMap((status) =>
				status === expected ? Effect.succeed(status) : Effect.fail(status),
			),
			Effect.retry(Schedule.spaced("50 millis")),
			Effect.timeout("10 seconds"),
			TestClock.withLive,
		);

	it.effect(
		"runs tracks RUNNING to COMPLETED and cancellation of finished runs is a no-op",
		() =>
			Effect.gen(function* () {
				const started = yield* Deferred.make<void>();
				const finish = yield* Deferred.make<void>();
				const task = Task.make({
					name: "run-status-completed",
					fn: () =>
						Deferred.succeed(started, undefined).pipe(
							Effect.andThen(Deferred.await(finish)),
							Effect.as({ done: true }),
						),
				});
				const hatchet = yield* Hatchet;
				yield* hatchet.register(task);
				yield* hatchet.startWorker();
				const handle = yield* task.runNoWait({});
				yield* Deferred.await(started);
				yield* awaitStatus(hatchet, handle.runId, "RUNNING");
				yield* Deferred.succeed(finish, undefined);
				yield* TestClock.withLive(handle.output);
				yield* awaitStatus(hatchet, handle.runId, "COMPLETED");
				yield* hatchet.runs.cancel(handle.runId);
				expect(yield* hatchet.runs.getStatus(handle.runId)).toBe("COMPLETED");
			}),
		{ timeout: 15_000 },
	);

	it.effect(
		"runs tracks RUNNING to FAILED",
		() =>
			Effect.gen(function* () {
				const started = yield* Deferred.make<void>();
				const fail = yield* Deferred.make<void>();
				const task = Task.make({
					name: "run-status-failed",
					retries: 0,
					fn: () =>
						Deferred.succeed(started, undefined).pipe(
							Effect.andThen(Deferred.await(fail)),
							Effect.andThen(Effect.fail("boom")),
						),
				});
				const hatchet = yield* Hatchet;
				yield* hatchet.register(task);
				yield* hatchet.startWorker();
				const handle = yield* task.runNoWait({});
				const output = yield* Effect.forkChild(
					TestClock.withLive(Effect.exit(handle.output)),
				);
				yield* Deferred.await(started);
				yield* awaitStatus(hatchet, handle.runId, "RUNNING");
				yield* Deferred.succeed(fail, undefined);
				expect(Exit.isFailure(yield* Fiber.join(output))).toBe(true);
				yield* awaitStatus(hatchet, handle.runId, "FAILED");
			}),
		{ timeout: 15_000 },
	);

	it.effect(
		"runs.cancel aborts the signal and interrupts the Effect with finalizers",
		() =>
			Effect.gen(function* () {
				const started = yield* Deferred.make<AbortSignal>();
				const finalized = yield* Deferred.make<boolean>();
				const task = Task.make({
					name: "run-cancel-finalizer",
					fn: (_input, ctx) =>
						Deferred.succeed(started, ctx.signal).pipe(
							Effect.andThen(Effect.never),
							Effect.ensuring(
								Effect.suspend(() =>
									Deferred.succeed(finalized, ctx.signal.aborted),
								),
							),
						),
				});
				const hatchet = yield* Hatchet;
				yield* hatchet.register(task);
				yield* hatchet.startWorker();
				const handle = yield* task.runNoWait({});
				const output = yield* Effect.forkChild(
					TestClock.withLive(Effect.exit(handle.output)),
				);
				const signal = yield* Deferred.await(started);
				expect(signal.aborted).toBe(false);
				yield* awaitStatus(hatchet, handle.runId, "RUNNING");
				yield* hatchet.runs.cancel(handle.runId);
				expect(yield* Deferred.await(finalized)).toBe(true);
				expect(signal.aborted).toBe(true);
				const exit = yield* Fiber.join(output);
				expect(Exit.isFailure(exit)).toBe(true);
				if (Exit.isFailure(exit)) {
					const failures = exit.cause.reasons.filter(Cause.isFailReason);
					expect(failures).toHaveLength(1);
					const failure = failures[0]?.error;
					expect(failure).toBeInstanceOf(TaskExecutionFailure);
					if (failure instanceof TaskExecutionFailure) {
						expect(failure.cause).toBeInstanceOf(RunCancelled);
						expect(failure.cause).toEqual(
							new RunCancelled({ runId: handle.runId }),
						);
					}
				}
				yield* awaitStatus(hatchet, handle.runId, "CANCELLED");
			}),
		{ timeout: 15_000 },
	);

	it.effect(
		"awaited run fails with RunCancelled after cancellation",
		() =>
			Effect.gen(function* () {
				const started = yield* Deferred.make<string>();
				const finalized = yield* Deferred.make<void>();
				const task = Task.make({
					name: "awaited-run-cancelled",
					fn: (_input, ctx) =>
						Deferred.succeed(started, ctx.runId).pipe(
							Effect.andThen(Effect.never),
							Effect.ensuring(Deferred.succeed(finalized, undefined)),
						),
				});
				const hatchet = yield* Hatchet;
				yield* hatchet.register(task);
				yield* hatchet.startWorker();
				const running = yield* Effect.forkChild(
					TestClock.withLive(Effect.exit(task.run({}))),
				);
				const runId = yield* Deferred.await(started);
				yield* awaitStatus(hatchet, runId, "RUNNING");
				yield* hatchet.runs.cancel(runId);
				yield* Deferred.await(finalized);
				const exit = yield* Fiber.join(running);
				expect(exit).toMatchObject({ _tag: "Failure" });
				if (Exit.isFailure(exit)) {
					const failures = exit.cause.reasons.filter(Cause.isFailReason);
					expect(failures).toHaveLength(1);
					const failure = failures[0]?.error;
					expect(failure).toBeInstanceOf(TaskExecutionFailure);
					if (failure instanceof TaskExecutionFailure) {
						expect(failure.cause).toEqual(new RunCancelled({ runId }));
					}
				}
				yield* awaitStatus(hatchet, runId, "CANCELLED");
			}),
		{ timeout: 15_000 },
	);

	it.effect(
		"putStream delivers live chunks and the subscription completes with the run",
		() =>
			Effect.gen(function* () {
				const emit = yield* Deferred.make<void>();
				const received = yield* Deferred.make<void>();
				const task = Task.make({
					name: "run-stream-chunks",
					fn: (_input, ctx) =>
						Deferred.await(emit).pipe(
							Effect.andThen(ctx.putStream("hello")),
							Effect.andThen(ctx.putStream(" world")),
							Effect.andThen(Deferred.await(received)),
							Effect.as({ done: true }),
						),
				});
				const hatchet = yield* Hatchet;
				yield* hatchet.register(task);
				yield* hatchet.startWorker();
				const handle = yield* task.runNoWait({});
				yield* awaitStatus(hatchet, handle.runId, "RUNNING");
				const chunks: string[] = [];
				const subscriber = yield* Effect.forkChild(
					hatchet.runs.subscribeToStream(handle.runId).pipe(
						Stream.runForEach((chunk) =>
							Effect.gen(function* () {
								chunks.push(chunk);
								if (chunks.length === 2)
									yield* Deferred.succeed(received, undefined);
							}),
						),
					),
				);
				// The SDK exposes no subscription-ready handshake; allow its network listener to connect.
				yield* Effect.sleep("1 second").pipe(TestClock.withLive);
				yield* Deferred.succeed(emit, undefined);
				yield* TestClock.withLive(handle.output);
				yield* Fiber.join(subscriber);
				expect(chunks).toEqual(["hello", " world"]);
			}),
		{ timeout: 15_000 },
	);
	// -------------------------------------------------------------------------
	// Basic: input + output schemas
	// -------------------------------------------------------------------------

	it.effect("registers and runs a task with input and output schemas", () =>
		Effect.gen(function* () {
			const greet = Task.make({
				name: "greet",
				input: S.Struct({ name: S.String }),
				output: S.Struct({ message: S.String }),
				fn: (input) => Effect.succeed({ message: `hello ${input.name}` }),
			});

			const hatchet = yield* Hatchet;
			yield* hatchet.register(greet);
			yield* hatchet.startWorker();
			const result = yield* TestClock.withLive(greet.run({ name: "world" }));

			expect(result.message).toBe("hello world");
		}),
	);

	// -------------------------------------------------------------------------
	// Basic: no input schema (input is unknown)
	// -------------------------------------------------------------------------

	it.effect("registers and runs a task with no input schema", () =>
		Effect.gen(function* () {
			const echo = Task.make({
				name: "echo-no-input",
				fn: (input) => Effect.succeed({ received: input }),
			});

			const hatchet = yield* Hatchet;
			yield* hatchet.register(echo);
			yield* hatchet.startWorker();
			const result = yield* TestClock.withLive(echo.run({ anything: true }));

			expect(result).toEqual({ received: { anything: true } });
		}),
	);

	// -------------------------------------------------------------------------
	// Basic: no output schema (passes through verbatim)
	// -------------------------------------------------------------------------

	it.effect("registers and runs a task with no output schema", () =>
		Effect.gen(function* () {
			const compute = Task.make({
				name: "compute-no-output",
				input: S.Struct({ x: S.Number }),
				fn: (input) => Effect.succeed({ doubled: input.x * 2 }),
			});

			const hatchet = yield* Hatchet;
			yield* hatchet.register(compute);
			yield* hatchet.startWorker();
			const result = yield* TestClock.withLive(compute.run({ x: 7 }));

			expect(result).toEqual({ doubled: 14 });
		}),
	);

	// -------------------------------------------------------------------------
	// concurrency option is accepted, forwarded to the SDK, and enforced
	// -------------------------------------------------------------------------

	it.effect("registers and runs a task with a concurrency option", () =>
		Effect.gen(function* () {
			// The concurrency key expression must evaluate to a string — the
			// real server rejects a bare numeric field ("expected string
			// output for concurrency key, got int"), so the key comes from
			// its own string input field rather than `input.x`.
			const limited = Task.make({
				name: "limited-concurrency",
				input: S.Struct({ x: S.Number, group: S.String }),
				output: S.Struct({ doubled: S.Number }),
				fn: (input) => Effect.succeed({ doubled: input.x * 2 }),
				concurrency: { expression: "input.group", maxRuns: 1 },
			});

			const hatchet = yield* Hatchet;
			yield* hatchet.register(limited);
			yield* hatchet.startWorker();
			const result = yield* TestClock.withLive(
				limited.run({ x: 5, group: "test-group" }),
			);

			expect(result.doubled).toBe(10);
		}),
	);

	// -------------------------------------------------------------------------
	// runNoWait returns a handle whose output resolves
	// -------------------------------------------------------------------------

	it.effect(
		"runNoWait returns unique run IDs and handles whose output resolves",
		() =>
			Effect.gen(function* () {
				const add = Task.make({
					name: "add",
					input: S.Struct({ a: S.Number, b: S.Number }),
					output: S.Struct({ sum: S.Number }),
					fn: (input) => Effect.succeed({ sum: input.a + input.b }),
				});

				const hatchet = yield* Hatchet;
				yield* hatchet.register(add);
				yield* hatchet.startWorker();
				const enqueue = add.runNoWait({ a: 3, b: 4 });
				const handle = yield* enqueue;
				const secondHandle = yield* enqueue;

				expect(typeof handle.runId).toBe("string");
				expect(handle.runId.length).toBeGreaterThan(0);
				expect(typeof secondHandle.runId).toBe("string");
				expect(secondHandle.runId.length).toBeGreaterThan(0);
				expect(secondHandle.runId).not.toBe(handle.runId);

				const result = yield* TestClock.withLive(handle.output);
				const secondResult = yield* TestClock.withLive(secondHandle.output);

				expect(result.sum).toBe(7);
				expect(secondResult.sum).toBe(7);
				yield* awaitStatus(hatchet, handle.runId, "COMPLETED");
			}),
	);

	it.effect(
		"runNoWait without an output schema exposes the task context run ID",
		() =>
			Effect.gen(function* () {
				const identify = Task.make({
					name: "identify-run-no-output-schema",
					fn: (_input, ctx) => Effect.succeed({ runId: ctx.runId }),
				});

				const hatchet = yield* Hatchet;
				yield* hatchet.register(identify);
				yield* hatchet.startWorker();
				const handle = yield* identify.runNoWait({});
				const result = yield* TestClock.withLive(handle.output);

				expect(typeof handle.runId).toBe("string");
				expect(handle.runId.length).toBeGreaterThan(0);
				expect(result.runId).toBe(handle.runId);
			}),
	);

	// -------------------------------------------------------------------------
	// schedule returns { id }; schedule.delete works
	// -------------------------------------------------------------------------

	it.effect("schedule returns an id and schedule.delete is idempotent", () =>
		Effect.gen(function* () {
			const noop = Task.make({
				name: "noop-scheduled",
				fn: () => Effect.succeed(null),
			});

			const hatchet = yield* Hatchet;
			yield* hatchet.register(noop);
			yield* hatchet.startWorker();

			const scheduled = yield* noop.schedule(new Date(Date.now() + 60_000), {});
			expect(typeof scheduled.id).toBe("string");
			expect(scheduled.id.length).toBeGreaterThan(0);

			// idempotent delete
			yield* hatchet.schedule.delete(scheduled.id);
			yield* hatchet.schedule.delete(scheduled.id); // second call must not fail
		}),
	);

	// -------------------------------------------------------------------------
	// Schema decode error surfaces as TaskExecutionFailure
	// -------------------------------------------------------------------------

	it.effect("input schema decode error surfaces as TaskExecutionFailure", () =>
		Effect.gen(function* () {
			const typed = Task.make({
				name: "typed-input",
				input: S.Struct({ count: S.Number }),
				fn: (input) => Effect.succeed({ doubled: input.count * 2 }),
			});

			const hatchet = yield* Hatchet;
			yield* hatchet.register(typed);
			yield* hatchet.startWorker();
			const exit = yield* Effect.exit(
				typed.run({ count: "not-a-number" as unknown as number }),
			);

			expect(Exit.isFailure(exit)).toBe(true);
			if (Exit.isFailure(exit)) {
				const failures = exit.cause.reasons
					.filter(Cause.isFailReason)
					.map((r) => r.error);
				expect(failures.length).toBe(1);
				expect(failures[0]).toBeInstanceOf(TaskExecutionFailure);
			}
		}),
	);

	// -------------------------------------------------------------------------
	// Unregistered task is a defect (die), not a typed failure
	// -------------------------------------------------------------------------

	it.effect(
		"running an unregistered task is a defect, not a typed failure",
		() =>
			Effect.gen(function* () {
				const ghost = Task.make({
					name: "ghost",
					fn: () => Effect.succeed("never"),
				});

				const exit = yield* Effect.exit(ghost.run({}));

				expect(Exit.isFailure(exit)).toBe(true);
				if (Exit.isFailure(exit)) {
					// Must be a defect, not a typed failure
					const defects = exit.cause.reasons
						.filter(Cause.isDieReason)
						.map((r) => r.defect);
					expect(defects.length).toBe(1);
					expect(String(defects[0])).toContain("Missing task");
					// Must have NO typed failures
					const failures = exit.cause.reasons
						.filter(Cause.isFailReason)
						.map((r) => r.error);
					expect(failures.length).toBe(0);
				}
			}),
	);

	// -------------------------------------------------------------------------
	// cron round-trip: create → list → delete
	// -------------------------------------------------------------------------

	it.effect("cron create → list → delete round-trip", () =>
		Effect.gen(function* () {
			const greet = Task.make({
				name: "greet-cron",
				input: S.Struct({ name: S.String }),
				fn: (input) => Effect.succeed({ message: `hello ${input.name}` }),
			});

			const hatchet = yield* Hatchet;
			yield* hatchet.register(greet);
			yield* hatchet.startWorker();

			const cron = yield* hatchet.cron.create({
				workflowName: greet.name,
				name: "daily-greet",
				expression: "0 9 * * *",
				input: { name: "world" },
				additionalMetadata: { tier: "free" },
			});
			expect(typeof cron.id).toBe("string");

			const listed = yield* hatchet.cron.list({ workflowName: greet.name });
			const found = listed.find((entry) => entry.id === cron.id);
			expect(found).toBeDefined();
			expect(found?.expression).toBe("0 9 * * *");
			expect(found?.workflowName).toBe(greet.name);

			yield* hatchet.cron.delete(cron.id);

			const afterDelete = yield* hatchet.cron.list({
				workflowName: greet.name,
			});
			expect(afterDelete.some((entry) => entry.id === cron.id)).toBe(false);
		}),
	);

	// -------------------------------------------------------------------------
	// event.push fires every task registered with `on.event` for that key
	//
	// Push is fire-and-forget under both layers (matching real Hatchet), so
	// this polls a Ref the task writes to instead of awaiting the run.
	// -------------------------------------------------------------------------

	it.effect(
		"event.push fires every task registered for that event key",
		() =>
			Effect.gen(function* () {
				const received = yield* Ref.make<string | undefined>(undefined);

				const onUserCreated = Task.make({
					name: "on-user-created",
					input: S.Struct({ userId: S.String }),
					on: { event: "user:created" },
					fn: (input) => Ref.set(received, input.userId),
				});

				const hatchet = yield* Hatchet;
				yield* hatchet.register(onUserCreated);
				yield* hatchet.startWorker();

				yield* hatchet.event.push("user:created", { userId: "user-1" });

				// This suite runs under `it.effect`, whose default TestClock never
				// advances on its own — a Clock-driven retry/timeout would hang
				// until vitest's own outer timeout kills it. Escape to the live
				// clock so the poll actually progresses in real wall-clock time.
				const userId = yield* Ref.get(received).pipe(
					Effect.filterOrFail(
						(value): value is string => value !== undefined,
						() => "not-fired-yet" as const,
					),
					Effect.retry(Schedule.spaced("50 millis")),
					Effect.timeoutOrElse({
						duration: "10 seconds",
						orElse: () =>
							Effect.fail(
								new Error("event-triggered task did not fire in time"),
							),
					}),
					TestClock.withLive,
				);

				expect(userId).toBe("user-1");
			}),
		{ timeout: 15_000 },
	);

	// -------------------------------------------------------------------------
	// A typed `Event` reference works end-to-end: `on: { event: <Event> }`
	// and `hatchet.event.push(<Event>, ...)` both resolve to its wire key,
	// and the task's `input` is written once via `OrderPlaced.payload`.
	//
	// Deliberately uses a different key/task name than the plain-string event
	// test above: `it.layer` shares one Hatchet instance (and, under the real
	// layer, one long-lived worker) across every test in the file, so reusing
	// a task name would let this test's run get dispatched to a stale worker
	// still holding the previous test's closure.
	// -------------------------------------------------------------------------

	it.effect(
		"event.push and on.event accept a typed Event reference",
		() =>
			Effect.gen(function* () {
				const received = yield* Ref.make<string | undefined>(undefined);

				const OrderPlaced = Event.make({
					key: "order:placed",
					payload: S.Struct({ orderId: S.String }),
				});

				const onOrderPlaced = Task.make({
					name: "on-order-placed",
					input: OrderPlaced.payload,
					on: { event: OrderPlaced },
					fn: (input) => Ref.set(received, input.orderId),
				});

				const hatchet = yield* Hatchet;
				yield* hatchet.register(onOrderPlaced);
				yield* hatchet.startWorker();

				yield* hatchet.event.push(OrderPlaced, { orderId: "order-1" });

				// See the note above: escape to the live clock so the poll
				// actually progresses in real wall-clock time.
				const orderId = yield* Ref.get(received).pipe(
					Effect.filterOrFail(
						(value): value is string => value !== undefined,
						() => "not-fired-yet" as const,
					),
					Effect.retry(Schedule.spaced("50 millis")),
					Effect.timeoutOrElse({
						duration: "10 seconds",
						orElse: () =>
							Effect.fail(
								new Error("event-triggered task did not fire in time"),
							),
					}),
					TestClock.withLive,
				);

				expect(orderId).toBe("order-1");
			}),
		{ timeout: 15_000 },
	);

	// -------------------------------------------------------------------------
	// event.push with no registered listeners is a no-op, not an error
	// -------------------------------------------------------------------------

	it.effect("event.push with no registered listeners is a no-op", () =>
		Effect.gen(function* () {
			const hatchet = yield* Hatchet;
			yield* hatchet.event.push("nobody:listening", { anything: true });
		}),
	);

	// -------------------------------------------------------------------------
	// R-requirement satisfied by layers in scope at register-time
	// -------------------------------------------------------------------------

	it.effect("task R-requirement is satisfied by layers at register-time", () =>
		Effect.gen(function* () {
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

			const hatchet = yield* Hatchet;
			yield* hatchet.register(sendEmail);
			yield* hatchet.startWorker();
			const result = yield* TestClock.withLive(
				sendEmail.run({ to: "alice@example.com" }),
			);

			expect(result.messageId).toBe("id-for-alice@example.com");
		}).pipe(Effect.provide(Mailer.layer)),
	);
}
