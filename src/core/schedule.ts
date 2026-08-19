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
	workflowRunStatus?: string;
};

export class ScheduleDeleteError extends Schema.TaggedError<ScheduleDeleteError>()(
	"ScheduleDeleteError",
	{ cause: Schema.Defect },
) {}

export class ScheduleListError extends Schema.TaggedError<ScheduleListError>()(
	"ScheduleListError",
	{ cause: Schema.Defect },
) {}
