import { it } from "@effect/vitest";
import { ConfigProvider, Deferred, Effect, Exit, Fiber, Layer } from "effect";
import { beforeEach, expect, vi } from "vitest";
import { Hatchet, Task } from "../src/index.js";

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

const sdk = vi.hoisted(() => ({
	task: vi.fn((params: Registration) => params),
	durableTask: vi.fn((params: Registration) => params),
}));

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
