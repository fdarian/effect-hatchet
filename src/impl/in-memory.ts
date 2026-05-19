import { Duration, Effect, Fiber, Layer, Schema, type Scope } from "effect";
import type { CronTrigger } from "../core/cron.js";
import { type Hatchet, HatchetTag } from "../core/hatchet.js";
import type {
	PossibleOutput,
	Task,
	TaskContext,
	TaskName,
} from "../core/task.js";
import { TaskExecutionFailure } from "../core/task.js";

export const make = Effect.gen(function* () {
	const runners = new Map<
		TaskName,
		(
			input: unknown,
			ctx: TaskContext,
		) => Effect.Effect<PossibleOutput, TaskExecutionFailure>
	>();

	type LocalCronEntry = {
		workflowName: string;
		name: string;
		expression: string;
		input: Record<string, unknown>;
		additionalMetadata: Record<string, string> | undefined;
	};

	const localCrons = yield* Effect.sync(
		() => new Map<string, LocalCronEntry>(),
	);
	const localCronCounter = yield* Effect.sync(() => ({ value: 0 }));

	const localSchedules = yield* Effect.sync(
		() => new Map<string, { workflowName: string }>(),
	);
	const localScheduleCounter = yield* Effect.sync(() => ({ value: 0 }));

	return {
		_internal: {
			run: (name, input) => {
				const runner = runners.get(name);
				if (runner == null) {
					return Effect.die(
						`Missing task: '${name}', make sure you have registered the task`,
					);
				}
				const ctx: TaskContext = { runId: crypto.randomUUID() };
				return runner(input, ctx).pipe(
					Effect.mapError(
						(error) => new TaskExecutionFailure({ cause: error }),
					),
				);
			},
			runNoWait: (name, input) => {
				const runner = runners.get(name);
				if (runner == null) {
					return Effect.die(
						`Missing task: '${name}', make sure you have registered the task`,
					);
				}
				const ctx: TaskContext = { runId: crypto.randomUUID() };
				return Effect.gen(function* () {
					const fiber = yield* Effect.forkDaemon(runner(input, ctx));
					return { output: Fiber.join(fiber) };
				});
			},
			schedule: (name, enqueueAt, input) => {
				const runner = runners.get(name);
				if (runner == null) {
					return Effect.die(
						`Missing task: '${name}', make sure you have registered the task`,
					);
				}
				const id = `local-schedule-${localScheduleCounter.value++}`;
				localSchedules.set(id, { workflowName: name });
				const ctx: TaskContext = { runId: crypto.randomUUID() };
				const delay = Math.max(0, enqueueAt.getTime() - Date.now());
				return Effect.sleep(Duration.millis(delay)).pipe(
					Effect.andThen(() => runner(input, ctx)),
					Effect.forkDaemon,
					Effect.as({ id }),
				);
			},
		},
		register: <R>(
			// biome-ignore lint/suspicious/noExplicitAny: Task INPUT/OUTPUT are in contravariant position; unknown doesn't accept concrete types
			task: Task<any, any, any, R>,
		): Effect.Effect<void, never, R | Scope.Scope> =>
			Effect.gen(function* () {
				const runtime = yield* Effect.runtime<R>();
				runners.set(task.name, (input, ctx) => {
					const effect = task._def
						.fn(input, ctx)
						.pipe(Effect.provide(runtime.context));
					const out = task._def.output;
					if (out == null)
						return effect as Effect.Effect<
							PossibleOutput,
							TaskExecutionFailure
						>;
					return effect.pipe(
						Effect.flatMap(
							(result) =>
								Schema.encodeUnknown(out)(
									result,
								) as Effect.Effect<PossibleOutput>,
						),
					) as Effect.Effect<PossibleOutput, TaskExecutionFailure>;
				});
			}),
		startWorker: () => Effect.void,
		cron: {
			create: (params) => {
				const id = `local-cron-${localCronCounter.value++}`;
				localCrons.set(id, {
					workflowName: params.workflowName,
					name: params.name,
					expression: params.expression,
					input: params.input,
					additionalMetadata: params.additionalMetadata,
				});
				return Effect.succeed({ id });
			},
			delete: (cronId) => {
				localCrons.delete(cronId);
				return Effect.void;
			},
			list: (params) => {
				const entries = [...localCrons.entries()];
				const filtered =
					params?.workflowName != null
						? entries.filter(
								(entry) => entry[1].workflowName === params.workflowName,
							)
						: entries;
				const triggers: CronTrigger[] = filtered.map((entry) => {
					const trigger: CronTrigger = {
						id: entry[0],
						expression: entry[1].expression,
						workflowName: entry[1].workflowName,
					};
					if (entry[1].name !== undefined) {
						trigger.name = entry[1].name;
					}
					return trigger;
				});
				return Effect.succeed(triggers);
			},
		},
		schedule: {
			delete: (id) => {
				localSchedules.delete(id);
				return Effect.void;
			},
		},
	} satisfies Hatchet;
});

export const layer = Layer.effect(HatchetTag, make);
