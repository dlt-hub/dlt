#!/usr/bin/env node
/**
 * CI guard: new `since` metadata must point at an unreleased version.
 * Usage: node versioning/check.js <changed markdown files...>
 * Fails if a page or section declares `since` constraints that are all released already, i.e. a "new in X"
 * claim for a feature that already shipped (use the release that actually introduced it
 * and run the guard only on files a PR adds or changes).
 */
const { compareVersions, toConstraints } = require("./badge");
const { resolveReleased } = require("./released");
const { scanFile } = require("./scan");

/** `since` is a claim about a coming release: it fails once every constraint of it is already released */
async function main(files) {
  let failed = false;
  for (const file of files) {
    const { page, sections } = scanFile(file);
    const tagged = [...sections, ...(page ? [toConstraints(page)] : [])].filter((c) => Object.keys(c).length);
    const released = await resolveReleased([...new Set(tagged.flatMap((c) => Object.keys(c)))]);
    for (const constraints of tagged) {
      if (Object.entries(constraints).every(([pkg, version]) => compareVersions(version, released[pkg]) <= 0)) {
        const text = Object.entries(constraints).map(([pkg, v]) => `${pkg} ${v} (released ${released[pkg]})`);
        console.error(`${file}: since ${text.join(", ")} is already released`);
        failed = true;
      }
    }
  }
  process.exit(failed ? 1 : 0);
}

if (require.main === module) main(process.argv.slice(2));
