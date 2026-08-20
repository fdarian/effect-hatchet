import { Schema } from "effect";

export type ScheduledRunStatus =
	| "PENDING"
	| "RUNNING"
	| "SUCCEEDED"
	| "FAILED"
	| "CANCELLED"
	| "QUEUED"
	| "SCHEDULED";

export type ScheduledRun = {
	id: string;
	workflowName: string;
	triggerAt: string;
	input?: Record<string, unknown>;
	additionalMetadata?: Record<string, unknown>;
	workflowRunCreatedAt?: string;
	workflowRunStatus?: ScheduledRunStatus;
};

/**
 * One page of `schedule.list` results. Mirrors what `hatchet.schedules.list`
 * returns: a page of rows plus whatever pagination metadata the source has —
 * no cursor is invented. `pagination` is present whenever the source reports
 * it; use `nextPage` (a 1-indexed page number, like `pagination.currentPage`)
 * to know whether — and how — to fetch the next page yourself.
 */
export type ScheduledRunPage = {
	schedules: ScheduledRun[];
	pagination?: {
		currentPage?: number;
		nextPage?: number;
		numPages?: number;
	};
};

export class ScheduleDeleteError extends Schema.TaggedError<ScheduleDeleteError>()(
	"ScheduleDeleteError",
	{ cause: Schema.Defect() },
) {}

export class ScheduleListError extends Schema.TaggedError<ScheduleListError>()(
	"ScheduleListError",
	{ cause: Schema.Defect() },
) {}
