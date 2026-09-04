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
 * it.
 *
 * `nextPage` is not an end-of-results sentinel: the real server (and this
 * package's in-memory mock) always populates it, clamping it back to
 * `currentPage` once you're on the last page rather than omitting it. To
 * walk every page yourself, advance `offset` to
 * `(pagination.nextPage - 1) * limit` while `pagination.nextPage >
 * pagination.currentPage`, and also stop once a page comes back with an
 * empty `schedules` array — the zero-result case never satisfies the
 * clamp, since `numPages` is 0 and `nextPage` just keeps counting up.
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
