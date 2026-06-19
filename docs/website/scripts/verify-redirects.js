#!/usr/bin/env node
/**
 * Post-build verification for redirect targets.
 *
 * Loads the compiled, version-prefixed redirect list (./redirects.compiled.js)
 * and checks that every `to` target resolves to an existing HTML page in the
 * build output. Run after `docusaurus build`:
 *
 *   node scripts/verify-redirects.js
 *
 * Exits with code 1 if any errors are found.
 */
const fs = require("node:fs");
const path = require("node:path");

const { compile, OUTPUT_FILE } = require("../tools/compile_redirects.js");

const BUILD_DIR = path.resolve(__dirname, "..", "build", "docs");
const _WEBSITE_DIR = path.resolve(__dirname, "..");

let errors = 0;
let warnings = 0;

function error(msg) {
  console.error(`  ERROR: ${msg}`);
  errors++;
}

function warn(msg) {
  console.warn(`  WARN:  ${msg}`);
  warnings++;
}

function ok(msg) {
  console.log(`  OK:    ${msg}`);
}

function targetExists(rel) {
  const candidates = [path.join(BUILD_DIR, `${rel}.html`), path.join(BUILD_DIR, rel, "index.html")];
  return candidates.some((c) => fs.existsSync(c));
}

console.log("\n--- redirect targets (redirects.compiled.js) ---");

if (!fs.existsSync(BUILD_DIR)) {
  console.error(`Build directory not found: ${BUILD_DIR}`);
  console.error('Run "npm run build" first.');
  process.exit(1);
}

if (!fs.existsSync(OUTPUT_FILE)) {
  console.error(
    `redirects.compiled.js not found at ${OUTPUT_FILE}. ` +
      `Run 'npm run compile-redirects' first (it runs as part of 'npm run build').`,
  );
  process.exit(1);
}

// Load the on-disk compiled list (what the worker actually ships).
delete require.cache[require.resolve(OUTPUT_FILE)];
const loadedRedirects = require(OUTPUT_FILE);

// Recompile in-memory from per-version sources currently on disk.
let recomputed;
try {
  recomputed = compile().compiled;
} catch (err) {
  console.error(`Could not recompute redirects for staleness check: ${err.message}`);
  process.exit(1);
}

if (JSON.stringify(loadedRedirects) !== JSON.stringify(recomputed)) {
  error(
    `redirects.compiled.js is stale — per-version sources (redirects.js / ` +
      `versioned_redirects/*.js) changed since it was generated. ` +
      `Re-run 'npm run compile-redirects'.`,
  );
  console.log(`\n=== Redirects: ${errors} errors, ${warnings} warnings ===`);
  process.exit(1);
}

ok(`${loadedRedirects.length} redirects loaded from redirects.compiled.js (in sync with sources)`);

// Check for duplicate `from` entries — distinct sources may have collided
// after version-prefix rewriting (shouldn't happen, but cheap to detect).
const fromCounts = {};
for (const r of loadedRedirects) {
  fromCounts[r.from] = (fromCounts[r.from] || 0) + 1;
}
const dupFroms = Object.entries(fromCounts).filter(([, c]) => c > 1);
if (dupFroms.length > 0) {
  for (const [from, count] of dupFroms) {
    error(`duplicate "from" path: ${from} (appears ${count} times)`);
  }
} else {
  ok('no duplicate "from" paths');
}

// Warn on redirect chains.
const fromSet = new Set(loadedRedirects.map((r) => r.from));
for (const r of loadedRedirects) {
  if (fromSet.has(r.to)) {
    warn(`redirect chain: ${r.from} → ${r.to} → ... (target is itself a redirect source)`);
  }
}

// Verify each target resolves to an existing page. Because every entry was
// version-prefixed at compile time, the path inside build/docs/ is exactly
// `to` minus the /docs/ prefix — no fallback lookup needed.
let checked = 0;
let skipped = 0;

for (const r of loadedRedirects) {
  if (!r.to.startsWith("/docs/")) {
    skipped++;
    continue;
  }

  checked++;
  const rel = r.to
    .replace(/^\/docs\//, "")
    .replace(/#.*$/, "")
    .replace(/\/$/, "");
  if (targetExists(rel)) {
    continue;
  }
  error(`redirect target ${r.to} has no HTML page (from: ${r.from}, ` + `checked ${rel}.html and ${rel}/index.html)`);
}

if (skipped > 0) {
  ok(`skipped ${skipped} non-/docs/ targets`);
}

if (errors === 0) {
  ok(`all ${checked} checked redirect targets resolve to existing pages`);
}

console.log(`\n=== Redirects: ${errors} errors, ${warnings} warnings ===`);
if (errors > 0) {
  process.exit(1);
}
