import {
	Config,
	Effect,
	FiberSet,
	Layer,
	Option,
	Runtime,
	Schema,
	type Scope,
} from "effect";
import {
	CronCreateError,
	CronDeleteError,
	CronListError,
	type CronTrigger,
} from "../core/cron.js";
import { type Hatchet, HatchetTag } from "../core/hatchet.js";
import {
	ScheduleDeleteError,
	type ScheduledRun,
	type ScheduledRunStatus,
	ScheduleListError,
} from "../core/schedule.js";
import {
	type PossibleOutput,
	type Task,
	type TaskContext,
	TaskExecutionFailure,
} from "../core/task.js";

const DEFAULT_TIMEOUT = "3h";
const SCHEDULE_LIST_PAGE_LIMIT = 200;
const SCHEDULE_LIST_MAX_ITERATIONS = 200;

function isAxios404(error: unknown): boolean {
	if (error == null || typeof error !== "object") return false;
	if (!("response" in error)) return false;
	const response = (error as { response: unknown }).response;
	if (response == null || typeof response !== "object") return false;
	if (!("status" in response)) return false;
	return (response as { status: unknown }).status === 404;
}

/** Redacts a secret to its first and last 4 characters, e.g. `abcd...wxyz`. */
function shorten(value: string): string {
	return `${value.slice(0, 4)}...${value.slice(-4)}`;
}

export type Options = { runPrefersThisWorker?: boolean };

export const make = (options?: Options) =>
	Effect.gen(function* () {
		const hostPort = yield* Config.string("HATCHET_CLIENT_HOST_PORT").pipe(
			Config.option,
			Effect.map(Option.getOrUndefined),
		);
		const apiUrl = yield* Config.string("HATCHET_CLIENT_API_URL").pipe(
			Config.option,
			Effect.map(Option.getOrUndefined),
		);
		const tlsStrategy = yield* Config.literal("none")(
			"HATCHET_CLIENT_TLS_STRATEGY",
		).pipe(Config.option, Effect.map(Option.getOrUndefined));

		const token = yield* Config.string("HATCHET_CLIENT_TOKEN");

		const infos = [
			`token: ${shorten(token)}`,
			hostPort !== undefined && `host port: ${hostPort}`,
			apiUrl !== undefined && `api url: ${apiUrl}`,
			tlsStrategy !== undefined && `tls strategy: ${tlsStrategy}`,
		].filter((info): info is string => info !== false);
		yield* Effect.logInfo(`Initialized Hatchet, ${infos.join(", ")}`);

		const sdk = yield* Effect.promise(
			() => import("@hatchet-dev/typescript-sdk"),
		);

		const runtime = yield* Effect.runtime();

		const hatchet = sdk.HatchetClient.init({
			token,
			...(hostPort !== undefined ? { host_port: hostPort } : {}),
			...(apiUrl !== undefined ? { api_url: apiUrl } : {}),
			...(tlsStrategy !== undefined
				? { tls_config: { tls_strategy: tlsStrategy } }
				: {}),
			logger: (context) => {
				const prefix = `[Hatchet ${context}]`;
				const runSync = Runtime.runSync(runtime);
				return {
					debug(message, extra) {
						runSync(
							Effect.logDebug(`${prefix} ${message}`).pipe(
								extra
									? Effect.annotateLogs(extra as Record<string, unknown>)
									: (e) => e,
							),
						);
					},
					info(message, extra) {
						runSync(
							Effect.logInfo(`${prefix} ${message}`).pipe(
								extra
									? Effect.annotateLogs(extra as Record<string, unknown>)
									: (e) => e,
							),
						);
					},
					green(message, extra) {
						runSync(
							Effect.logInfo(`${prefix} ${message}`).pipe(
								extra
									? Effect.annotateLogs(extra as Record<string, unknown>)
									: (e) => e,
							),
						);
					},
					warn(message, error) {
						runSync(
							Effect.logWarning(`${prefix} ${message}`).pipe(
								error ? Effect.annotateLogs({ error }) : (e) => e,
							),
						);
					},
					error(message, error) {
						runSync(
							Effect.logError(`${prefix} ${message}`).pipe(
								error ? Effect.annotateLogs({ error }) : (e) => e,
							),
						);
					},
				};
			},
		});

		const instanceId =
			options?.runPrefersThisWorker === true ? crypto.randomUUID() : undefined;
		const workerAffinityOpts =
			instanceId != null
				? {
						desiredWorkerLabels: {
							instanceId: {
								value: instanceId,
								required: true,
							},
						},
					}
				: undefined;

		type HatchetTask = Awaited<
			ReturnType<(typeof sdk.HatchetClient.prototype)["task"]>
		>;
		const tasks = new Map<string, HatchetTask>();

		return {
			_internal: {
				run: (name, input) => {
					const target = tasks.get(name);
					if (target == null) {
						return Effect.die(
							`Missing task: '${name}', make sure you have registered the task`,
						);
					}
					return Effect.tryPromise({
						try: () =>
							(
								target as unknown as {
									run: (
										input: unknown,
										opts: unknown,
									) => Promise<PossibleOutput>;
								}
							).run(input, workerAffinityOpts),
						catch: (error) => new TaskExecutionFailure({ cause: error }),
					});
				},
				runNoWait: (name, input) => {
					const target = tasks.get(name);
					if (target == null) {
						return Effect.die(
							`Missing task: '${name}', make sure you have registered the task`,
						);
					}
					return Effect.gen(function* () {
						const ref = yield* Effect.tryPromise({
							try: () =>
								(
									target as unknown as {
										runNoWait: (
											input: unknown,
											opts: unknown,
										) => Promise<{ output: Promise<PossibleOutput> }>;
									}
								).runNoWait(input, workerAffinityOpts),
							catch: (error) => new TaskExecutionFailure({ cause: error }),
						});
						return {
							output: Effect.tryPromise({
								try: () => ref.output,
								catch: (error) => new TaskExecutionFailure({ cause: error }),
							}),
						};
					});
				},
				schedule: (name, enqueueAt, input) => {
					const target = tasks.get(name);
					if (target == null) {
						return Effect.die(
							`Missing task: '${name}', make sure you have registered the task`,
						);
					}
					return Effect.tryPromise({
						try: () =>
							(
								target as unknown as {
									schedule: (
										enqueueAt: Date,
										input: unknown,
										opts: unknown,
									) => Promise<{ metadata: { id: string } }>;
								}
							).schedule(enqueueAt, input, workerAffinityOpts),
						catch: (error) => new TaskExecutionFailure({ cause: error }),
					}).pipe(Effect.map((result) => ({ id: result.metadata.id })));
				},
			},
			register: <R>(
				// biome-ignore lint/suspicious/noExplicitAny: Task INPUT/OUTPUT are in contravariant position; unknown doesn't accept concrete types
				task: Task<any, any, any, R>,
			): Effect.Effect<void, never, R | Scope.Scope> =>
				Effect.gen(function* () {
					const runPromise = yield* FiberSet.makeRuntimePromise<R>();
					const makeFn =
						(fn: typeof task._def.fn) =>
						async (
							input: unknown,
							hatchetCtx: {
								workflowRunId(): string;
								abortController: AbortController;
							},
						) => {
							const ctx: TaskContext = {
								runId: hatchetCtx.workflowRunId(),
							};
							const effect = fn(input, ctx);
							const out = task._def.output;
							const effectWithEncode =
								out == null
									? effect
									: effect.pipe(
											Effect.flatMap(
												(result) =>
													Schema.encodeUnknown(out)(
														result,
													) as Effect.Effect<PossibleOutput>,
											),
										);
							const effectWithAbort = Effect.raceFirst(
								effectWithEncode,
								Effect.async<never>((resume) => {
									const signal = hatchetCtx.abortController.signal;
									if (signal.aborted) {
										resume(Effect.interrupt);
										return;
									}
									const handler = () => resume(Effect.interrupt);
									signal.addEventListener("abort", handler, { once: true });
									return Effect.sync(() =>
										signal.removeEventListener("abort", handler),
									);
								}),
							);
							return runPromise(effectWithAbort);
						};

					const onOpt = task._def.on;
					const on = Effect.isEffect(onOpt)
						? yield* Effect.orDie(onOpt)
						: onOpt;

					// biome-ignore lint/suspicious/noExplicitAny: SDK boundary — fn signature mismatch is intentional
					const sdkFn = makeFn(task._def.fn) as any;
					const taskDecl = task._def.durable
						? hatchet.durableTask({
								name: task.name,
								...(task._def.rateLimits !== undefined
									? { rateLimits: task._def.rateLimits }
									: {}),
								...(task._def.concurrency !== undefined
									? { concurrency: task._def.concurrency }
									: {}),
								...(on !== undefined ? { on } : {}),
								fn: sdkFn,
								executionTimeout: DEFAULT_TIMEOUT,
							})
						: hatchet.task({
								name: task.name,
								...(task._def.rateLimits !== undefined
									? { rateLimits: task._def.rateLimits }
									: {}),
								...(task._def.concurrency !== undefined
									? { concurrency: task._def.concurrency }
									: {}),
								...(on !== undefined ? { on } : {}),
								fn: sdkFn,
								executionTimeout: DEFAULT_TIMEOUT,
							});
					tasks.set(task.name, taskDecl as unknown as HatchetTask);
				}),
			startWorker: () =>
				Effect.gen(function* () {
					const workerOpts = {
						slots: 200,
						...(instanceId != null ? { labels: { instanceId } } : {}),
						workflows: [...tasks.values()],
					};
					const worker = yield* Effect.tryPromise(() =>
						hatchet.worker("hatchet-worker", workerOpts),
					);
					yield* Effect.fork(Effect.tryPromise(() => worker.start()));
				}).pipe(Effect.orDie),
			cron: {
				create: (params) =>
					Effect.tryPromise({
						try: () =>
							hatchet.crons.create(params.workflowName, {
								name: params.name,
								expression: params.expression,
								input: params.input,
								additionalMetadata: params.additionalMetadata,
							}),
						catch: (error) => new CronCreateError({ cause: error }),
					}).pipe(
						Effect.map((result) => ({
							id: (result as unknown as { metadata: { id: string } }).metadata
								.id,
						})),
					),
				delete: (cronId) =>
					Effect.tryPromise({
						try: () => hatchet.crons.delete(cronId),
						catch: (error) => new CronDeleteError({ cause: error }),
					}),
				list: (params) =>
					Effect.tryPromise({
						try: () =>
							hatchet.crons.list(
								params?.workflowName !== undefined
									? { workflowName: params.workflowName }
									: {},
							),
						catch: (error) => new CronListError({ cause: error }),
					}).pipe(
						Effect.map((result) => {
							const rows =
								(
									result as unknown as {
										rows?: Array<{
											metadata: { id: string };
											name?: string;
											cron: string;
											workflowName: string;
										}>;
									}
								).rows ?? [];
							return rows.map((row) => {
								const trigger: CronTrigger = {
									id: row.metadata.id,
									expression: row.cron,
									workflowName: row.workflowName,
								};
								if (row.name !== undefined) {
									trigger.name = row.name;
								}
								return trigger;
							});
						}),
					),
				_testFire: () =>
					Effect.die(
						"cron._testFire is only available under Hatchet.layerInMemory()",
					),
			},
			schedule: {
				list: (options) =>
					Effect.gen(function* () {
						const accumulated: ScheduledRun[] = [];
						let offset = 0;
						let iterations = 0;

						while (true) {
							if (iterations >= SCHEDULE_LIST_MAX_ITERATIONS) {
								return yield* Effect.fail(
									new ScheduleListError({
										cause: new Error(
											`schedule.list exceeded max iterations (${SCHEDULE_LIST_MAX_ITERATIONS})`,
										),
									}),
								);
							}
							iterations += 1;

							const result = yield* Effect.tryPromise({
								try: () =>
									hatchet.schedules.list({
										offset,
										limit: SCHEDULE_LIST_PAGE_LIMIT,
										orderByField: "triggerAt" as unknown as NonNullable<
											Parameters<
												typeof hatchet.schedules.list
											>[0]["orderByField"]
										>,
										...(options?.statuses !== undefined
											? {
													statuses: options.statuses as unknown as NonNullable<
														Parameters<
															typeof hatchet.schedules.list
														>[0]["statuses"]
													>,
												}
											: {}),
									}),
								catch: (error) => new ScheduleListError({ cause: error }),
							});

							const rows = result.rows ?? [];
							for (const row of rows) {
								const scheduledRun: ScheduledRun = {
									id: row.metadata.id,
									workflowName: row.workflowName,
									triggerAt: row.triggerAt,
								};
								if (row.input !== undefined) {
									scheduledRun.input = row.input;
								}
								if (row.additionalMetadata !== undefined) {
									scheduledRun.additionalMetadata = row.additionalMetadata;
								}
								if (row.workflowRunCreatedAt !== undefined) {
									scheduledRun.workflowRunCreatedAt = row.workflowRunCreatedAt;
								}
								if (row.workflowRunStatus !== undefined) {
									// The SDK's WorkflowRunStatus (a fired run's state) and our
									// ScheduledRunStatus (the schedule's state, also used for the
									// `statuses` filter above) overlap except WorkflowRunStatus's
									// BACKOFF vs our SCHEDULED -- pass the value through as-is.
									scheduledRun.workflowRunStatus =
										row.workflowRunStatus as unknown as ScheduledRunStatus;
								}
								accumulated.push(scheduledRun);
							}

							const numPages = result.pagination?.num_pages ?? 1;
							const currentPage = result.pagination?.current_page ?? 1;

							if (
								currentPage >= numPages ||
								rows.length < SCHEDULE_LIST_PAGE_LIMIT
							) {
								break;
							}

							offset += SCHEDULE_LIST_PAGE_LIMIT;
						}

						return accumulated;
					}),
				delete: (id) =>
					Effect.tryPromise({
						try: () => hatchet.schedules.delete(id),
						catch: (error) => error as unknown,
					}).pipe(
						Effect.catchAll((error) =>
							isAxios404(error)
								? Effect.void
								: Effect.fail(new ScheduleDeleteError({ cause: error })),
						),
					),
			},
		} satisfies Hatchet;
	});

export const layer = (options?: Options) =>
	Layer.effect(HatchetTag, make(options));
