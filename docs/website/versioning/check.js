#!/usr/bin/env node
/**
 * CI guard: new `since` metadata must point at an unreleased version.
 * Usage: node versioning/check.js <changed markdown files...>
 * Fails if a file declares `since` <= the released version of its package, i.e. a "new in X"
 * claim for a feature that already shipped (use the release that actually introduced it
 * and run the guard only on files a PR adds or changes).
 */
const { compareVersions } = require("./badge");
const { resolveReleased } = require("./released");
const { scanFile } = require("./scan");

async function main(files) {
  let failed = false;
  for (const file of files) {
    const { page } = scanFile(file);
    if (!page?.since) continue;
    const released = (await resolveReleased([page.package]))[page.package];
    if (compareVersions(page.since, released) <= 0) {
      console.error(`${file}: since ${page.since} is already released (${page.package} ${released})`);
      failed = true;
    }
  }
  process.exit(failed ? 1 : 0);
}

if (require.main === module) main(process.argv.slice(2));
