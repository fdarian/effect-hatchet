import { describe, it } from "@effect/vitest";
import { registerSharedHatchetTests } from "./shared-suite.js";
import { layerRealHatchet } from "./support/real-hatchet.js";

// Opt-in: `pnpm test:real` sets HATCHET_TEST_REAL=1. Wrapping the whole
// it.layer(...) block in describe.skipIf keeps a plain `pnpm test` run from
// ever touching Docker — vitest doesn't run a skipped suite's hooks, so the
// container never boots. A bare `if (enabled) { ... }` instead would leave
// this file with no test suite when skipped, which vitest treats as an error.
describe.skipIf(process.env.HATCHET_TEST_REAL !== "1")(
	"Hatchet (real hatchet-lite)",
	() => {
		it.layer(layerRealHatchet, { timeout: 180_000 })((it) => {
			registerSharedHatchetTests(it);
		});
	},
);
