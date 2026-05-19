import type { Context } from "effect";
import * as internal from "./core/hatchet.js";
import * as inMemory from "./impl/in-memory.js";
import * as live from "./impl/live.js";

export {
	CronCreateError,
	CronDeleteError,
	CronListError,
} from "./core/cron.js";
export {
	ScheduleDeleteError,
	Task,
	TaskExecutionFailure,
} from "./core/task.js";

export interface Hatchet extends internal.Hatchet {}

export const Hatchet: Context.Tag<Hatchet, Hatchet> & {
	readonly layer: (options?: live.Options) => ReturnType<typeof live.layer>;
	readonly layerInMemory: () => typeof inMemory.layer;
} = Object.assign(internal.HatchetTag, {
	layer: (options?: live.Options) => live.layer(options),
	layerInMemory: () => inMemory.layer,
});
