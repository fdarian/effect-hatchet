import { Schema } from "effect";

export type CronTrigger = {
	id: string;
	/** Optional because the Hatchet SDK exposes `name` as an optional field */
	name?: string;
	expression: string;
	workflowName: string;
};

export class CronCreateError extends Schema.TaggedError<CronCreateError>()(
	"CronCreateError",
	{ cause: Schema.Defect },
) {}

export class CronDeleteError extends Schema.TaggedError<CronDeleteError>()(
	"CronDeleteError",
	{ cause: Schema.Defect },
) {}

export class CronListError extends Schema.TaggedError<CronListError>()(
	"CronListError",
	{ cause: Schema.Defect },
) {}
