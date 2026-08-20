import type { Vitest } from "@effect/vitest";
import { expect } from "@effect/vitest";
import {
	Cause,
	Effect,
	Exit,
	Ref,
	Schema as S,
	Schedule,
	TestServices,
} from "effect";
import { Task, TaskExecutionFailure } from "../src/core/task.js";
import { Hatchet } from "../src/index.js";

class Mailer extends Effect.Service<Mailer>()("Mailer", {
	succeed: {
		send: (to: string) => Effect.succeed(`id-for-${to}`),
	},
}) {}

/**
 * Tests that hold under both `Hatchet.layerInMemory()` and
 * `layerRealHatchet` — registered here once and run against `it` scoped to
 * each layer in `tests/hatchet.test.ts` and `tests/hatchet.real.test.ts`.
 */
export function registerSharedHatchetTests(it: Vitest.MethodsNonLive<Hatchet>) {
	// -------------------------------------------------------------------------
	// Basic: input + output schemas
	// -------------------------------------------------------------------------

	it.scoped("registers and runs a task with input and output schemas", () =>
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
			const result = yield* greet.run({ name: "world" });

			expect(result.message).toBe("hello world");
		}),
	);

	// -------------------------------------------------------------------------
	// Basic: no input schema (input is unknown)
	// -------------------------------------------------------------------------

	it.scoped("registers and runs a task with no input schema", () =>
		Effect.gen(function* () {
			const echo = Task.make({
				name: "echo-no-input",
				fn: (input) => Effect.succeed({ received: input }),
			});

			const hatchet = yield* Hatchet;
			yield* hatchet.register(echo);
			yield* hatchet.startWorker();
			const result = yield* echo.run({ anything: true });

			expect(result).toEqual({ received: { anything: true } });
		}),
	);

	// -------------------------------------------------------------------------
	// Basic: no output schema (passes through verbatim)
	// -------------------------------------------------------------------------

	it.scoped("registers and runs a task with no output schema", () =>
		Effect.gen(function* () {
			const compute = Task.make({
				name: "compute-no-output",
				input: S.Struct({ x: S.Number }),
				fn: (input) => Effect.succeed({ doubled: input.x * 2 }),
			});

			const hatchet = yield* Hatchet;
			yield* hatchet.register(compute);
			yield* hatchet.startWorker();
			const result = yield* compute.run({ x: 7 });

			expect(result).toEqual({ doubled: 14 });
		}),
	);

	// -------------------------------------------------------------------------
	// concurrency option is accepted, forwarded to the SDK, and enforced
	// -------------------------------------------------------------------------

	it.scoped("registers and runs a task with a concurrency option", () =>
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
			const result = yield* limited.run({ x: 5, group: "test-group" });

			expect(result.doubled).toBe(10);
		}),
	);

	// -------------------------------------------------------------------------
	// runNoWait returns a handle whose output resolves
	// -------------------------------------------------------------------------

	it.scoped("runNoWait returns a handle whose output resolves", () =>
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
			const handle = yield* add.runNoWait({ a: 3, b: 4 });
			const result = yield* handle.output;

			expect(result.sum).toBe(7);
		}),
	);

	// -------------------------------------------------------------------------
	// schedule returns { id }; schedule.delete works
	// -------------------------------------------------------------------------

	it.scoped("schedule returns an id and schedule.delete is idempotent", () =>
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

	it.scoped("input schema decode error surfaces as TaskExecutionFailure", () =>
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
				const failures = [...Cause.failures(exit.cause)];
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
					const defects = [...Cause.defects(exit.cause)];
					expect(defects.length).toBe(1);
					expect(String(defects[0])).toContain("Missing task");
					// Must have NO typed failures
					const failures = [...Cause.failures(exit.cause)];
					expect(failures.length).toBe(0);
				}
			}),
	);

	// -------------------------------------------------------------------------
	// cron round-trip: create → list → delete
	// -------------------------------------------------------------------------

	it.scoped("cron create → list → delete round-trip", () =>
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

	it.scoped(
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

				// This suite runs under `it.scoped`, whose default TestClock never
				// advances on its own — a Clock-driven retry/timeout would hang
				// until vitest's own outer timeout kills it. Escape to the live
				// clock so the poll actually progresses in real wall-clock time.
				const userId = yield* Ref.get(received).pipe(
					Effect.filterOrFail(
						(value): value is string => value !== undefined,
						() => "not-fired-yet" as const,
					),
					Effect.retry(Schedule.spaced("50 millis")),
					Effect.timeoutFail({
						duration: "10 seconds",
						onTimeout: () =>
							new Error("event-triggered task did not fire in time"),
					}),
					TestServices.provideLive,
				);

				expect(userId).toBe("user-1");
			}),
		{ timeout: 15_000 },
	);

	// -------------------------------------------------------------------------
	// event.push with no registered listeners is a no-op, not an error
	// -------------------------------------------------------------------------

	it.scoped("event.push with no registered listeners is a no-op", () =>
		Effect.gen(function* () {
			const hatchet = yield* Hatchet;
			yield* hatchet.event.push("nobody:listening", { anything: true });
		}),
	);

	// -------------------------------------------------------------------------
	// R-requirement satisfied by layers in scope at register-time
	// -------------------------------------------------------------------------

	it.scoped("task R-requirement is satisfied by layers at register-time", () =>
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
			const result = yield* sendEmail.run({ to: "alice@example.com" });

			expect(result.messageId).toBe("id-for-alice@example.com");
		}).pipe(Effect.provide(Mailer.Default)),
	);
}
