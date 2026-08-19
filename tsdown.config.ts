import { defineConfig } from "tsdown";

export default defineConfig({
	entry: ["src/index.ts"],
	format: ["esm"],
	dts: true,
	clean: true,
	outDir: "dist",
	// Package is ESM-only ("type": "module"), so plain .js/.d.ts extensions
	// are unambiguous — skip tsdown's Node-platform default of forcing
	// .mjs/.d.mts to disambiguate from CJS output.
	fixedExtension: false,
});
