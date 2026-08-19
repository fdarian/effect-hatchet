import {
	Duration,
	Effect,
	Exit,
	Fiber,
	Layer,
	Schema,
	type Scope,
} from "effect";
import type { CronTrigger } from "../core/cron.js";
import { type Hatchet, HatchetTag } from "../core/hatchet.js";
import type {
	ScheduledRun,
	ScheduledRunPage,
	ScheduledRunStatus,
} from "../core/schedule.js";
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

	type LocalScheduleEntry = {
		workflowName: string;
		triggerAt: string;
		input?: Record<string, unknown>;
		additionalMetadata?: Record<string, unknown>;
		workflowRunCreatedAt?: string;
		workflowRunStatus?: ScheduledRunStatus;
		fiber?: Fiber.Fiber<void>;
	};

	const localSchedules = yield* Effect.sync(
		() => new Map<string, LocalScheduleEntry>(),
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
				const entry: LocalScheduleEntry = {
					workflowName: name,
					triggerAt: enqueueAt.toISOString(),
					input: input as Record<string, unknown>,
				};
				localSchedules.set(id, entry);
				const ctx: TaskContext = { runId: crypto.randomUUID() };
				const delay = Math.max(0, enqueueAt.getTime() - Date.now());
				return Effect.gen(function* () {
					const fiber = yield* Effect.sleep(Duration.millis(delay)).pipe(
						Effect.andThen(() =>
							Effect.gen(function* () {
								entry.workflowRunCreatedAt = new Date().toISOString();
								const exit = yield* Effect.exit(runner(input, ctx));
								entry.workflowRunStatus = Exit.isSuccess(exit)
									? "SUCCEEDED"
									: "FAILED";
							}),
						),
						Effect.forkDaemon,
					);
					entry.fiber = fiber;
					return { id };
				});
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
			_testFire: (cronId) => {
				const entry = localCrons.get(cronId);
				if (entry == null) {
					return Effect.die(`Missing local cron: '${cronId}'`);
				}
				const runner = runners.get(entry.workflowName);
				if (runner == null) {
					return Effect.die(
						`Missing task for cron: '${entry.workflowName}', make sure you have registered the task`,
					);
				}
				const ctx: TaskContext = { runId: crypto.randomUUID() };
				return runner(entry.input, ctx).pipe(Effect.asVoid);
			},
		},
		schedule: {
			list: (options) => {
				const matching = [...localSchedules.entries()]
					.map(([id, entry]) => ({
						id,
						entry,
						// A schedule with no run yet is still "SCHEDULED" — this default
						// is what both the status filter below and the returned record
						// use, so a schedule matching `statuses: ["SCHEDULED"]` always
						// comes back with that same value in `workflowRunStatus`.
						status: entry.workflowRunStatus ?? ("SCHEDULED" as const),
					}))
					.filter(({ status }) =>
						options?.statuses == null
							? true
							: options.statuses.includes(status),
					)
					.sort((a, b) => a.entry.triggerAt.localeCompare(b.entry.triggerAt));

				// Mirrors the live layer's 1-indexed page numbers: with no limit
				// given, everything matching fits on a single page.
				const total = matching.length;
				const offset = options?.offset ?? 0;
				const limit = options?.limit ?? Math.max(total, 1);
				const page = matching.slice(offset, offset + limit);

				const schedules: ScheduledRun[] = page.map(({ id, entry, status }) => {
					const scheduledRun: ScheduledRun = {
						id,
						workflowName: entry.workflowName,
						triggerAt: entry.triggerAt,
						workflowRunStatus: status,
					};
					if (entry.input !== undefined) {
						scheduledRun.input = entry.input;
					}
					if (entry.additionalMetadata !== undefined) {
						scheduledRun.additionalMetadata = entry.additionalMetadata;
					}
					if (entry.workflowRunCreatedAt !== undefined) {
						scheduledRun.workflowRunCreatedAt = entry.workflowRunCreatedAt;
					}
					return scheduledRun;
				});

				const numPages = Math.max(1, Math.ceil(total / limit));
				const currentPage = Math.floor(offset / limit) + 1;
				const result: ScheduledRunPage = {
					schedules,
					pagination: {
						currentPage,
						numPages,
						...(currentPage < numPages ? { nextPage: currentPage + 1 } : {}),
					},
				};
				return Effect.succeed(result);
			},
			delete: (id) => {
				const entry = localSchedules.get(id);
				localSchedules.delete(id);
				if (entry?.fiber == null) {
					return Effect.void;
				}
				return Fiber.interrupt(entry.fiber).pipe(Effect.asVoid, Effect.orDie);
			},
		},
	} satisfies Hatchet;
});

export const layer = Layer.effect(HatchetTag, make);
