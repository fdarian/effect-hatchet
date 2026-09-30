import { Schema } from "effect";

export type RunStatus =
	| "QUEUED"
	| "RUNNING"
	| "COMPLETED"
	| "CANCELLED"
	| "FAILED";

export class RunNotFound extends Schema.TaggedError<RunNotFound>()(
	"RunNotFound",
	{ runId: Schema.String },
) {}

export class RunsError extends Schema.TaggedError<RunsError>()("RunsError", {
	cause: Schema.Defect(),
}) {}
