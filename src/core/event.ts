import { Schema } from "effect";

export class EventPushError extends Schema.TaggedError<EventPushError>()(
	"EventPushError",
	{ cause: Schema.Defect },
) {}
