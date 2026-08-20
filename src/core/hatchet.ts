import { Context, type Effect, type Scope } from "effect";
import type {
	CronCreateError,
	CronDeleteError,
	CronListError,
	CronTrigger,
} from "./cron.js";
import type {
	AnyEvent,
	EventPushError,
	EventPushInput,
	EventPushOptions,
} from "./event.js";
import type {
	ScheduleDeleteError,
	ScheduledRunPage,
	ScheduledRunStatus,
	ScheduleListError,
} from "./schedule.js";
import type {
	PossibleOutput,
	Task,
	TaskExecutionFailure,
	TaskName,
} from "./task.js";

export interface Hatchet {
	_internal: {
		run: (
			name: TaskName,
			input: unknown,
		) => Effect.Effect<PossibleOutput, TaskExecutionFailure>;
		runNoWait: (
			name: TaskName,
			input: unknown,
		) => Effect.Effect<
			{ output: Effect.Effect<PossibleOutput, TaskExecutionFailure> },
			TaskExecutionFailure
		>;
		schedule: (
			name: TaskName,
			enqueueAt: Date,
			input: unknown,
		) => Effect.Effect<{ id: string }, TaskExecutionFailure>;
	};
	register: <R>(
		// biome-ignore lint/suspicious/noExplicitAny: Task INPUT/OUTPUT are in contravariant position; unknown doesn't accept concrete types
		task: Task<any, any, any, R>,
	) => Effect.Effect<void, never, R | Scope.Scope>;
	startWorker: () => Effect.Effect<void>;
	cron: {
		create: (params: {
			workflowName: string;
			name: string;
			expression: string;
			input: Record<string, unknown>;
			additionalMetadata?: Record<string, string>;
		}) => Effect.Effect<{ id: string }, CronCreateError>;
		delete: (cronId: string) => Effect.Effect<void, CronDeleteError>;
		list: (params?: {
			workflowName?: string;
		}) => Effect.Effect<CronTrigger[], CronListError>;
		/** Manually fire a registered cron by ID. In-memory only — dies under `Hatchet.layer`. */
		_testFire: (cronId: string) => Effect.Effect<void, TaskExecutionFailure>;
	};
	schedule: {
		list: (options?: {
			statuses?: ScheduledRunStatus[];
			offset?: number;
			limit?: number;
		}) => Effect.Effect<ScheduledRunPage, ScheduleListError>;
		delete: (id: string) => Effect.Effect<void, ScheduleDeleteError>;
	};
	event: {
		/** Pushes an event; every task registered with `on: { event: key }` fires. */
		push: <E extends string | AnyEvent = string>(
			key: E,
			input: EventPushInput<E>,
			options?: EventPushOptions,
		) => Effect.Effect<{ id: string }, EventPushError>;
	};
}

export const HatchetTag = Context.GenericTag<Hatchet>("effect-hatchet/Hatchet");
