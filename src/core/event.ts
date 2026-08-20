import { Schema } from "effect";

export class EventPushError extends Schema.TaggedError<EventPushError>()(
	"EventPushError",
	{ cause: Schema.Defect },
) {}

/**
 * A typed event definition: a wire key plus the schema its payload is
 * pushed and decoded against. Written once and referenced both from
 * `Task.make`'s `on: { event }` and from `hatchet.event.push`, so the two
 * call sites can't drift on the payload shape.
 */
export class Event<A, I, R> {
	readonly _tag = "event" as const;
	readonly key: string;
	readonly payload: Schema.Schema<A, I, R>;

	constructor(args: { key: string; payload: Schema.Schema<A, I, R> }) {
		this.key = args.key;
		this.payload = args.payload;
	}

	static make<S extends Schema.Schema.Any>(params: {
		key: string;
		payload: S;
	}): Event<
		Schema.Schema.Type<S>,
		Schema.Schema.Encoded<S>,
		Schema.Schema.Context<S>
	> {
		// The cast bridges `Schema.Schema.Any`'s erased `unknown` context to
		// the precise `Context<S>` reflected in this method's return type.
		return new Event({ key: params.key, payload: params.payload }) as Event<
			Schema.Schema.Type<S>,
			Schema.Schema.Encoded<S>,
			Schema.Schema.Context<S>
		>;
	}
}

/**
 * Narrows a plain event key or a typed `Event` reference down to its wire
 * key string. Shared by `on.event` resolution (task.ts) and both
 * `hatchet.event.push` impls so neither reimplements the string-or-Event
 * check.
 */
export function eventKey<A, I, R>(keyOrEvent: string | Event<A, I, R>): string {
	return typeof keyOrEvent === "string" ? keyOrEvent : keyOrEvent.key;
}

/**
 * The type `hatchet.event.push`'s `input` parameter takes for a given `key`
 * argument: the event schema's Encoded side when `key` is a typed `Event`
 * (matching what `Task.make` already decodes on the receiving end), or a
 * plain record when `key` is a bare string.
 */
export type EventPushInput<E> =
	// biome-ignore lint/suspicious/noExplicitAny: Event payload type params are invariant; unknown doesn't accept concrete instances
	E extends Event<any, infer I, any> ? I : Record<string, unknown>;
