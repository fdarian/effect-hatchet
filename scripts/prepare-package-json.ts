import { readFileSync, writeFileSync } from "node:fs";

const path = "package.json";
const pkg = JSON.parse(readFileSync(path, "utf8"));

// npm refuses every command (including `npm info`/`npm publish`) when
// devEngines.runtime doesn't match the running runtime, so strip it before
// changeset publish shells out to npm.
delete pkg.devEngines;

writeFileSync(path, `${JSON.stringify(pkg, null, "\t")}\n`);
