import { it } from "@effect/vitest";
import { ConfigProvider, Deferred, Effect, Exit, Fiber, Layer } from "effect";
import { TestClock } from "effect/testing";
import { beforeEach, expect, vi } from "vitest";
import {
	Hatchet,
	RunCancelled,
	type RunStatus,
	RunsError,
	Task,
	TaskExecutionFailure,
} from "../src/index.js";

type SdkContext = {
	workflowRunId(): string;
	abortController: AbortController;
	putStream(data: string): Promise<void>;
};

type Registration = {
	name: string;
	fn: (input: unknown, ctx: SdkContext) => Promise<unknown>;
	executionTimeout?: string;
	scheduleTimeout?: string;
	retries?: number;
};

const sdk = vi.hoisted(() => {
	const runNoWait =
		vi.fn<
			() => Promise<{
				getWorkflowRunId: () => Promise<string>;
				output: Promise<Record<string, unknown>>;
			}>
		>();
	return {
		task: vi.fn((params: Registration) => ({ ...params, runNoWait })),
		durableTask: vi.fn((params: Registration) => ({ ...params, runNoWait })),
		runNoWait,
		runs: { get_status: vi.fn<(runId: string) => Promise<RunStatus>>() },
	};
});

vi.mock("@hatchet-dev/typescript-sdk", () => ({
	HatchetClient: { init: () => sdk },
}));

beforeEach(() => vi.clearAllMocks());

const layer = Hatchet.layer().pipe(
	Layer.provide(
		ConfigProvider.layer(
			ConfigProvider.fromUnknown({
				HATCHET_CLIENT_TOKEN: "test-token-not-a-secret",
			}),
		),
	),
	Layer.orDie,
);

it.layer(layer)("Hatchet (mock SDK boundary)", (it) => {
	it.effect(
		"run and runNoWait reject empty SDK outputs for cancelled runs",
		() =>
			Effect.gen(function* () {
				const hatchet = yield* Hatchet;
				const task = Task.make({
					name: "sdk-cancelled-output",
					fn: () => Effect.succeed({}),
				});
				yield* hatchet.register(task);
				for (const noWait of [false, true]) {
					const runId = `sdk-cancelled-${noWait}`;
					sdk.runNoWait.mockResolvedValue({
						getWorkflowRunId: () => Promise.resolve(runId),
						output: Promise.resolve({}),
					});
					sdk.runs.get_status.mockResolvedValueOnce("CANCELLED");
					const output = noWait
						? (yield* task.runNoWait({})).output
						: task.run({});
					const failure = yield* Effect.flip(output);
					expect(failure).toBeInstanceOf(TaskExecutionFailure);
					expect(failure.cause).toEqual(new RunCancelled({ runId }));
					expect(sdk.runs.get_status).toHaveBeenLastCalledWith(runId);
				}
				expect(sdk.runs.get_status).toHaveBeenCalledTimes(2);
			}),
	);

	it.effect(
		"run and runNoWait wrap post-result status failures in TaskExecutionFailure",
		() =>
			Effect.gen(function* () {
				const hatchet = yield* Hatchet;
				const task = Task.make({
					name: "sdk-output-status-error",
					fn: () => Effect.succeed({}),
				});
				yield* hatchet.register(task);
				const cause = new Error("status unavailable");
				for (const noWait of [false, true]) {
					sdk.runNoWait.mockResolvedValue({
						getWorkflowRunId: () => Promise.resolve("sdk-status-error"),
						output: Promise.resolve({}),
					});
					sdk.runs.get_status.mockRejectedValueOnce(cause);
					const output = noWait
						? (yield* task.runNoWait({})).output
						: task.run({});
					const failure = yield* Effect.flip(output);
					expect(failure).toBeInstanceOf(TaskExecutionFailure);
					expect(failure.cause).toBe(cause);
				}
				expect(sdk.runs.get_status).toHaveBeenCalledTimes(2);
			}),
	);

	it.effect("polls non-terminal statuses with capped exponential delays", () =>
		Effect.gen(function* () {
			const hatchet = yield* Hatchet;
			const task = Task.make({
				name: "sdk-delayed-status",
				fn: () => Effect.succeed({ done: true }),
			});
			yield* hatchet.register(task);
			for (const terminal of ["CANCELLED", "COMPLETED"] as const) {
				const runId = `sdk-delayed-${terminal}`;
				sdk.runNoWait.mockResolvedValue({
					getWorkflowRunId: () => Promise.resolve(runId),
					output: Promise.resolve({ done: true }),
				});
				sdk.runs.get_status
					.mockResolvedValueOnce("QUEUED")
					.mockResolvedValueOnce("RUNNING")
					.mockResolvedValueOnce("RUNNING")
					.mockResolvedValueOnce("RUNNING")
					.mockResolvedValueOnce("RUNNING")
					.mockResolvedValueOnce(terminal);
				const handle = yield* task.runNoWait({});
				const running = yield* Effect.forkChild(Effect.exit(handle.output));
				for (const delay of [100, 200, 400, 800, 1000]) {
					yield* TestClock.adjust(delay);
				}
				const result = yield* Fiber.join(running);
				if (terminal === "COMPLETED") {
					expect(result).toMatchObject({
						_tag: "Success",
						value: { done: true },
					});
				} else {
					const failure = yield* Effect.flip(result);
					expect(failure.cause).toEqual(new RunCancelled({ runId }));
				}
			}
			expect(sdk.runs.get_status).toHaveBeenCalledTimes(12);
		}),
	);

	it.effect("SDK result failures do not trigger a status check", () =>
		Effect.gen(function* () {
			const hatchet = yield* Hatchet;
			const task = Task.make({
				name: "sdk-result-failed",
				fn: () => Effect.succeed({}),
			});
			yield* hatchet.register(task);
			const cause = new Error("result failed");
			for (const noWait of [false, true]) {
				sdk.runNoWait.mockImplementationOnce(async () => ({
					getWorkflowRunId: () => Promise.resolve("sdk-failed"),
					get output() {
						return Promise.reject(cause);
					},
				}));
				const output = noWait
					? (yield* task.runNoWait({})).output
					: task.run({});
				const failure = yield* Effect.flip(output);
				expect(failure).toBeInstanceOf(TaskExecutionFailure);
				expect(failure.cause).toBe(cause);
			}
			expect(sdk.runs.get_status).not.toHaveBeenCalled();
		}),
	);

	it.effect("successful SDK outputs are preserved after one status check", () =>
		Effect.gen(function* () {
			const hatchet = yield* Hatchet;
			const task = Task.make({
				name: "sdk-output-completed",
				fn: () => Effect.succeed({ done: true }),
			});
			yield* hatchet.register(task);
			for (const noWait of [false, true]) {
				sdk.runNoWait.mockResolvedValue({
					getWorkflowRunId: () => Promise.resolve("sdk-completed"),
					output: Promise.resolve({ done: true }),
				});
				sdk.runs.get_status.mockResolvedValueOnce("COMPLETED");
				const output = noWait
					? (yield* task.runNoWait({})).output
					: task.run({});
				expect(yield* output).toEqual({ done: true });
			}
			expect(sdk.runs.get_status).toHaveBeenCalledTimes(2);
		}),
	);

	it.effect(
		"polls non-terminal statuses until cancellation or completion",
		() =>
			Effect.gen(function* () {
				const hatchet = yield* Hatchet;
				const task = Task.make({
					name: "sdk-delayed-status",
					fn: () => Effect.succeed({ done: true }),
				});
				yield* hatchet.register(task);
				for (const noWait of [false, true]) {
					for (const status of ["CANCELLED", "COMPLETED"] as const) {
						const runId = `sdk-delayed-${noWait}-${status}`;
						sdk.runNoWait.mockResolvedValue({
							getWorkflowRunId: () => Promise.resolve(runId),
							output: Promise.resolve({ done: true }),
						});
						sdk.runs.get_status
							.mockResolvedValueOnce("QUEUED")
							.mockResolvedValueOnce("RUNNING")
							.mockResolvedValueOnce(status);
						const output = noWait
							? (yield* task.runNoWait({})).output
							: task.run({});
						const running = yield* Effect.forkChild(Effect.exit(output));
						yield* TestClock.adjust("1 second");
						const exit = yield* Fiber.join(running);
						if (status === "COMPLETED") {
							expect(exit).toEqual(Exit.succeed({ done: true }));
						} else {
							const failure = yield* Effect.flip(exit);
							expect(failure.cause).toEqual(new RunCancelled({ runId }));
						}
					}
				}
				expect(sdk.runs.get_status).toHaveBeenCalledTimes(12);
			}),
	);

	it.effect("rejects resolved SDK outputs when terminal status is FAILED", () =>
		Effect.gen(function* () {
			const hatchet = yield* Hatchet;
			const task = Task.make({
				name: "sdk-terminal-failed",
				fn: () => Effect.succeed({}),
			});
			yield* hatchet.register(task);
			for (const noWait of [false, true]) {
				sdk.runNoWait.mockResolvedValue({
					getWorkflowRunId: () => Promise.resolve("sdk-terminal-failed"),
					output: Promise.resolve({}),
				});
				sdk.runs.get_status.mockResolvedValueOnce("FAILED");
				const output = noWait
					? (yield* task.runNoWait({})).output
					: task.run({});
				const failure = yield* Effect.flip(output);
				expect(failure).toBeInstanceOf(TaskExecutionFailure);
				expect(failure.cause).toBeInstanceOf(RunsError);
				if (failure.cause instanceof RunsError) {
					expect(failure.cause.cause).toEqual(
						new Error("Run 'sdk-terminal-failed' finished with status FAILED"),
					);
				}
			}
			expect(sdk.runs.get_status).toHaveBeenCalledTimes(2);
		}),
	);

	it.effect("fails after 30 seconds without a terminal status", () =>
		Effect.gen(function* () {
			const hatchet = yield* Hatchet;
			const task = Task.make({
				name: "sdk-status-timeout",
				fn: () => Effect.succeed({}),
			});
			yield* hatchet.register(task);
			for (const noWait of [false, true]) {
				sdk.runNoWait.mockResolvedValue({
					getWorkflowRunId: () => Promise.resolve("sdk-status-timeout"),
					output: Promise.resolve({}),
				});
				sdk.runs.get_status.mockResolvedValue("RUNNING");
				const output = noWait
					? (yield* task.runNoWait({})).output
					: task.run({});
				const running = yield* Effect.forkChild(Effect.flip(output));
				yield* TestClock.adjust("30 seconds");
				const failure = yield* Fiber.join(running);
				expect(failure).toBeInstanceOf(TaskExecutionFailure);
				expect(failure.cause).toBeInstanceOf(RunsError);
				if (failure.cause instanceof RunsError) {
					expect(failure.cause.cause).toEqual(
						new Error(
							"Run 'sdk-status-timeout' status never became terminal within 30 seconds",
						),
					);
				}
			}
		}),
	);

	it.effect("forwards execution options to regular and durable SDK tasks", () =>
		Effect.gen(function* () {
			const hatchet = yield* Hatchet;
			for (const durable of [false, true]) {
				yield* hatchet.register(
					Task.make({
						name: `options-${durable}`,
						durable,
						executionTimeout: "6h",
						scheduleTimeout: "10m",
						retries: 2,
						fn: () => Effect.succeed({ done: true }),
					}),
				);
				const registration = (durable ? sdk.durableTask : sdk.task).mock
					.calls[0]?.[0];
				expect(registration).toMatchObject({
					executionTimeout: "6h",
					scheduleTimeout: "10m",
					retries: 2,
				});
			}
		}),
	);

	it.effect(
		"preserves the default timeout and omits unset retry/schedule options",
		() =>
			Effect.gen(function* () {
				const hatchet = yield* Hatchet;
				for (const durable of [false, true]) {
					yield* hatchet.register(
						Task.make({
							name: `defaults-${durable}`,
							durable,
							fn: () => Effect.succeed({ done: true }),
						}),
					);
					const registration = (durable ? sdk.durableTask : sdk.task).mock
						.calls[0]?.[0];
					expect(registration).toHaveProperty("executionTimeout", "3h");
					expect(registration).not.toHaveProperty("scheduleTimeout");
					expect(registration).not.toHaveProperty("retries");
				}
			}),
	);

	it.effect(
		"the SDK abort signal interrupts the task runtime and runs finalizers",
		() =>
			Effect.gen(function* () {
				const started = yield* Deferred.make<AbortSignal>();
				const finalized = yield* Deferred.make<boolean>();
				const task = Task.make({
					name: "sdk-cancellation",
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
				const registration = sdk.task.mock.calls[0]?.[0];
				if (registration === undefined)
					return yield* Effect.die("Missing SDK registration");
				const controller = new AbortController();
				const running = yield* Effect.forkChild(
					Effect.exit(
						Effect.tryPromise(() =>
							registration.fn(
								{},
								{
									workflowRunId: () => "sdk-run-id",
									abortController: controller,
									putStream: async () => {},
								},
							),
						),
					),
				);
				expect(yield* Deferred.await(started)).toBe(controller.signal);
				controller.abort();
				expect(yield* Deferred.await(finalized)).toBe(true);
				expect(Exit.isFailure(yield* Fiber.join(running))).toBe(true);
			}),
	);

	it.effect(
		"putStream forwards chunks and wraps SDK failures in TaskStreamError",
		() =>
			Effect.gen(function* () {
				const cause = new Error("stream transport failed");
				const chunks: string[] = [];
				const task = Task.make({
					name: "sdk-stream-error",
					fn: (_input, ctx) =>
						ctx
							.putStream("chunk")
							.pipe(
								Effect.catchTag("TaskStreamError", (error) =>
									Effect.succeed({ cause: error.cause }),
								),
							),
				});
				const hatchet = yield* Hatchet;
				yield* hatchet.register(task);
				const registration = sdk.task.mock.calls[0]?.[0];
				if (registration === undefined)
					return yield* Effect.die("Missing SDK registration");
				const result = yield* Effect.tryPromise(() =>
					registration.fn(
						{},
						{
							workflowRunId: () => "sdk-run-id",
							abortController: new AbortController(),
							putStream: (data) => {
								chunks.push(data);
								return Promise.reject(cause);
							},
						},
					),
				);
				expect(chunks).toEqual(["chunk"]);
				expect(result).toEqual({ cause });
			}),
	);
});
