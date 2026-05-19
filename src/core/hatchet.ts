import { Context, type Effect, type Scope } from "effect";
import type {
	CronCreateError,
	CronDeleteError,
	CronListError,
	CronTrigger,
} from "./cron.js";
import type {
	PossibleOutput,
	ScheduleDeleteError,
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
	};
	schedule: {
		delete: (id: string) => Effect.Effect<void, ScheduleDeleteError>;
	};
}

export const HatchetTag = Context.GenericTag<Hatchet>("effect-hatchet/Hatchet");
