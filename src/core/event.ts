import { Schema } from "effect";

export class EventPushError extends Schema.TaggedError<EventPushError>()(
	"EventPushError",
	{ cause: Schema.Defect() },
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
	readonly payload: Schema.Codec<A, I, R, R>;

	constructor(args: { key: string; payload: Schema.Codec<A, I, R, R> }) {
		this.key = args.key;
		this.payload = args.payload;
	}

	static make<S extends Schema.Top>(params: {
		key: string;
		payload: S;
	}): Event<
		Schema.Schema.Type<S>,
		S["Encoded"],
		S["DecodingServices"] | S["EncodingServices"]
	> {
		// The cast bridges `Schema.Top`'s erased `unknown` services to the
		// precise decoding/encoding service union reflected in this method's
		// return type.
		return new Event({ key: params.key, payload: params.payload }) as Event<
			Schema.Schema.Type<S>,
			S["Encoded"],
			S["DecodingServices"] | S["EncodingServices"]
		>;
	}
}

/**
 * `Event` with its payload type params erased. Used wherever a value only
 * needs to be "some `Event`" — `on.event` and `push`'s generic constraint
 * don't need the concrete `A`/`I`/`R`. `unknown` doesn't work here: Event's
 * payload type params are invariant, so a concrete `Event<...>` isn't
 * assignable to `Event<unknown, unknown, unknown>`.
 */
// biome-ignore lint/suspicious/noExplicitAny: Event payload type params are invariant; unknown doesn't accept concrete instances
export type AnyEvent = Event<any, any, any>;

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
export type EventPushInput<E> = E extends AnyEvent
	? E["payload"]["Encoded"]
	: Record<string, unknown>;

/** Options accepted by `hatchet.event.push`, forwarded to the SDK as-is. */
export type EventPushOptions = {
	additionalMetadata?: Record<string, string>;
	priority?: number;
	scope?: string;
};
