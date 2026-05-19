import { Effect, type ParseResult, Schema } from "effect";
import { HatchetTag } from "./hatchet.js";

export class TaskExecutionFailure extends Schema.TaggedError<TaskExecutionFailure>()(
	"TaskExecutionFailure",
	{ cause: Schema.Defect },
) {}

export class ScheduleDeleteError extends Schema.TaggedError<ScheduleDeleteError>()(
	"ScheduleDeleteError",
	{ cause: Schema.Defect },
) {}

export type TaskContext = {
	readonly runId: string;
};

export type TaskName = string;
export type PossibleOutput = Record<string, unknown> | undefined;

type RateLimitsOpt = Array<{ key: string; units: number }>;
type OnOpts = { event: string } | { cron: string };

export class Task<INPUT, OUTPUT, ERROR, R> {
	readonly _tag = "task" as const;
	readonly name: string;
	readonly _def: {
		fn: (input: INPUT, ctx: TaskContext) => Effect.Effect<OUTPUT, ERROR, R>;
		rateLimits?: RateLimitsOpt;
		on?: OnOpts | Effect.Effect<OnOpts | undefined, unknown, R>;
		durable?: boolean;
		output?: Schema.Schema.Any;
	};

	constructor(args: {
		name: string;
		_def: Task<INPUT, OUTPUT, ERROR, R>["_def"];
	}) {
		this.name = args.name;
		this._def = args._def;
	}

	static make<
		S extends Schema.Schema.Any,
		OS extends Schema.Schema.Any,
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
		on?: OnOpts | Effect.Effect<OnOpts | undefined, unknown, ON_R>;
		durable?: boolean;
	}): Task<
		Schema.Schema.Encoded<S>,
		Schema.Schema.Type<OS>,
		IN_E | ParseResult.ParseError,
		IN_R | ON_R
	>;
	static make<
		S extends Schema.Schema.Any,
		IN_O,
		IN_E,
		IN_R,
		ON_R = never,
	>(params: {
		name: string;
		input: S;
		output?: never;
		fn: (input: S["Type"], ctx: TaskContext) => Effect.Effect<IN_O, IN_E, IN_R>;
		rateLimits?: RateLimitsOpt;
		on?: OnOpts | Effect.Effect<OnOpts | undefined, unknown, ON_R>;
		durable?: boolean;
	}): Task<
		Schema.Schema.Encoded<S>,
		IN_O,
		IN_E | ParseResult.ParseError,
		IN_R | ON_R
	>;
	static make<
		OS extends Schema.Schema.Any,
		IN_I,
		IN_E,
		IN_R,
		ON_R = never,
	>(params: {
		name: string;
		input?: never;
		output: OS;
		fn: (
			input: IN_I,
			ctx: TaskContext,
		) => Effect.Effect<Schema.Schema.Type<OS>, IN_E, IN_R>;
		rateLimits?: RateLimitsOpt;
		on?: OnOpts | Effect.Effect<OnOpts | undefined, unknown, ON_R>;
		durable?: boolean;
	}): Task<
		IN_I,
		Schema.Schema.Type<OS>,
		IN_E | ParseResult.ParseError,
		IN_R | ON_R
	>;
	static make<IN_I, IN_O, IN_E, IN_R, ON_R = never>(params: {
		name: string;
		input?: never;
		output?: never;
		fn: (input: IN_I, ctx: TaskContext) => Effect.Effect<IN_O, IN_E, IN_R>;
		rateLimits?: RateLimitsOpt;
		on?: OnOpts | Effect.Effect<OnOpts | undefined, unknown, ON_R>;
		durable?: boolean;
	}): Task<IN_I, IN_O, IN_E, IN_R | ON_R>;
	static make(params: {
		name: string;
		input?: Schema.Schema.Any;
		output?: Schema.Schema.Any;
		fn: (
			input: unknown,
			ctx: TaskContext,
		) => Effect.Effect<unknown, unknown, unknown>;
		rateLimits?: RateLimitsOpt;
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
					Schema.decodeUnknown(schema)(input).pipe(
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
			return yield* Schema.decodeUnknown(outputSchema)(
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
					Effect.flatMap((raw) => Schema.decodeUnknown(outputSchema)(raw)),
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
