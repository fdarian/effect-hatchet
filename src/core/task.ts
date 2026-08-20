import type {
	Concurrency,
	CreateTaskWorkflowOpts,
} from "@hatchet-dev/typescript-sdk";
import { Effect, Schema } from "effect";
import { type AnyEvent, eventKey } from "./event.js";
import { HatchetTag } from "./hatchet.js";

export class TaskExecutionFailure extends Schema.TaggedError<TaskExecutionFailure>()(
	"TaskExecutionFailure",
	{ cause: Schema.Defect() },
) {}

export type TaskContext = {
	readonly runId: string;
};

export type TaskName = string;
export type PossibleOutput = Record<string, unknown> | undefined;

type TaskParams = CreateTaskWorkflowOpts;
type RateLimitsOpt = NonNullable<TaskParams["rateLimits"]>;
type ConcurrencyOpt = Concurrency | Concurrency[];
type SdkOnOpts = NonNullable<TaskParams["on"]>;

/**
 * A task's `on` trigger config: same shape as the SDK's, but `event` also
 * accepts typed `Event` references alongside plain string keys.
 */
export type OnOpts = {
	cron?: SdkOnOpts["cron"];
	event?: string | AnyEvent | Array<string | AnyEvent>;
};

function normalizeOn(on: OnOpts | undefined): SdkOnOpts | undefined {
	if (on === undefined) return undefined;
	const normalized: SdkOnOpts = {};
	if (on.cron !== undefined) normalized.cron = on.cron;
	if (on.event !== undefined) {
		normalized.event = Array.isArray(on.event)
			? on.event.map(eventKey)
			: eventKey(on.event);
	}
	return normalized;
}

/**
 * Resolves a task's `on` trigger config, running it if it was supplied as an
 * Effect, then normalizes any `Event` references in `on.event` down to
 * their wire key strings — producing the SDK-shaped `{ cron?, event? }`.
 * Shared by both impls — `live.ts` passes the resolved config straight to
 * the SDK; `in-memory.ts` also reads `on.event` off it to build its
 * event-name index.
 */
export function resolveTaskOn<R>(
	on: OnOpts | Effect.Effect<OnOpts | undefined, unknown, R> | undefined,
): Effect.Effect<SdkOnOpts | undefined, never, R> {
	const resolved: Effect.Effect<OnOpts | undefined, never, R> =
		on === undefined
			? Effect.succeed(undefined)
			: Effect.isEffect(on)
				? Effect.orDie(on)
				: Effect.succeed(on);
	return resolved.pipe(Effect.map(normalizeOn));
}

export class Task<INPUT, OUTPUT, ERROR, R> {
	readonly _tag = "task" as const;
	readonly name: string;
	readonly _def: {
		fn: (input: INPUT, ctx: TaskContext) => Effect.Effect<OUTPUT, ERROR, R>;
		rateLimits?: RateLimitsOpt;
		concurrency?: ConcurrencyOpt;
		on?: OnOpts | Effect.Effect<OnOpts | undefined, unknown, R>;
		durable?: boolean;
		output?: Schema.Top;
	};

	constructor(args: {
		name: string;
		_def: Task<INPUT, OUTPUT, ERROR, R>["_def"];
	}) {
		this.name = args.name;
		this._def = args._def;
	}

	static make<
		S extends Schema.Top,
		OS extends Schema.Top,
		IN_E,
		IN_R,
		ON_R = never,
	>(params: {
		name: string;
		input: S;
		output: OS;
		fn: (
			input: S["Type"],
			ctx: TaskContext,
		) => Effect.Effect<Schema.Schema.Type<OS>, IN_E, IN_R>;
		rateLimits?: RateLimitsOpt;
		concurrency?: ConcurrencyOpt;
		on?: OnOpts | Effect.Effect<OnOpts | undefined, unknown, ON_R>;
		durable?: boolean;
	}): Task<
		S["Encoded"],
		Schema.Schema.Type<OS>,
		IN_E | Schema.SchemaError,
		IN_R | ON_R
	>;
	static make<S extends Schema.Top, IN_O, IN_E, IN_R, ON_R = never>(params: {
		name: string;
		input: S;
		output?: never;
		fn: (input: S["Type"], ctx: TaskContext) => Effect.Effect<IN_O, IN_E, IN_R>;
		rateLimits?: RateLimitsOpt;
		concurrency?: ConcurrencyOpt;
		on?: OnOpts | Effect.Effect<OnOpts | undefined, unknown, ON_R>;
		durable?: boolean;
	}): Task<S["Encoded"], IN_O, IN_E | Schema.SchemaError, IN_R | ON_R>;
	static make<OS extends Schema.Top, IN_I, IN_E, IN_R, ON_R = never>(params: {
		name: string;
		input?: never;
		output: OS;
		fn: (
			input: IN_I,
			ctx: TaskContext,
		) => Effect.Effect<Schema.Schema.Type<OS>, IN_E, IN_R>;
		rateLimits?: RateLimitsOpt;
		concurrency?: ConcurrencyOpt;
		on?: OnOpts | Effect.Effect<OnOpts | undefined, unknown, ON_R>;
		durable?: boolean;
	}): Task<
		IN_I,
		Schema.Schema.Type<OS>,
		IN_E | Schema.SchemaError,
		IN_R | ON_R
	>;
	static make<IN_I, IN_O, IN_E, IN_R, ON_R = never>(params: {
		name: string;
		input?: never;
		output?: never;
		fn: (input: IN_I, ctx: TaskContext) => Effect.Effect<IN_O, IN_E, IN_R>;
		rateLimits?: RateLimitsOpt;
		concurrency?: ConcurrencyOpt;
		on?: OnOpts | Effect.Effect<OnOpts | undefined, unknown, ON_R>;
		durable?: boolean;
	}): Task<IN_I, IN_O, IN_E, IN_R | ON_R>;
	static make(params: {
		name: string;
		input?: Schema.Top;
		output?: Schema.Top;
		fn: (
			input: unknown,
			ctx: TaskContext,
		) => Effect.Effect<unknown, unknown, unknown>;
		rateLimits?: RateLimitsOpt;
		concurrency?: ConcurrencyOpt;
		on?: OnOpts | Effect.Effect<OnOpts | undefined, unknown, unknown>;
		durable?: boolean;
	}) {
		const schema = params.input;
		const errorHandler = Effect.tapError((error: unknown) =>
			Effect.logError(`Failed to run task ${params.name}`).pipe(
				Effect.annotateLogs({ error }),
			),
		);

		const fn = schema
			? (input: unknown, ctx: TaskContext) =>
					Schema.decodeUnknownEffect(schema)(input).pipe(
						Effect.flatMap((decoded) => params.fn(decoded, ctx)),
						errorHandler,
					)
			: (input: unknown, ctx: TaskContext) =>
					params.fn(input, ctx).pipe(errorHandler);

		return new Task({
			name: params.name,
			_def: {
				fn,
				...(params.rateLimits !== undefined
					? { rateLimits: params.rateLimits }
					: {}),
				...(params.concurrency !== undefined
					? { concurrency: params.concurrency }
					: {}),
				...(params.on !== undefined ? { on: params.on } : {}),
				...(params.durable !== undefined ? { durable: params.durable } : {}),
				...(params.output !== undefined ? { output: params.output } : {}),
			},
		});
	}

	run(input: INPUT) {
		const name = this.name;
		const outputSchema = this._def.output;
		return Effect.gen(function* () {
			const hatchet = yield* HatchetTag;
			const result = yield* hatchet._internal.run(name, input);
			if (outputSchema == null) return result as OUTPUT;
			return yield* Schema.decodeUnknownEffect(outputSchema)(
				result,
			) as Effect.Effect<OUTPUT>;
		});
	}

	runNoWait(input: INPUT) {
		const name = this.name;
		const outputSchema = this._def.output;
		return Effect.gen(function* () {
			const hatchet = yield* HatchetTag;
			const result = yield* hatchet._internal.runNoWait(name, input);
			if (outputSchema == null) {
				return result as {
					output: Effect.Effect<OUTPUT, TaskExecutionFailure>;
				};
			}
			return {
				output: result.output.pipe(
					Effect.flatMap((raw) =>
						Schema.decodeUnknownEffect(outputSchema)(raw),
					),
					Effect.mapError((err) => new TaskExecutionFailure({ cause: err })),
				) as Effect.Effect<OUTPUT, TaskExecutionFailure>,
			};
		});
	}

	schedule(enqueueAt: Date, input: INPUT) {
		const name = this.name;
		return Effect.gen(function* () {
			const hatchet = yield* HatchetTag;
			return yield* hatchet._internal.schedule(name, enqueueAt, input);
		});
	}
}
