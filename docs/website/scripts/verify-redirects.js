#!/usr/bin/env node
/**
 * Post-build verification for redirect targets.
 *
 * Imports the shared REDIRECTS array from redirects.js and checks that every
 * `to` target resolves to an existing HTML page in the build output. Run after
 * `docusaurus build`:
 *
 *   node scripts/verify-redirects.js
 *
 * Exits with code 1 if any errors are found.
 */
const fs = require('fs');
const path = require('path');

const BUILD_DIR = path.resolve(__dirname, '..', 'build', 'docs');
const redirects = require('../redirects.js');

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------
let errors = 0;
let warnings = 0;
let develOnlyWarnings = 0;

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
  const candidates = [
    path.join(BUILD_DIR, rel + '.html'),
    path.join(BUILD_DIR, rel, 'index.html'),
  ];
  return candidates.some((c) => fs.existsSync(c));
}

function targetExistsOnDevel(rel) {
  const candidates = [
    path.join(BUILD_DIR, 'devel', rel + '.html'),
    path.join(BUILD_DIR, 'devel', rel, 'index.html'),
  ];
  return candidates.some((c) => fs.existsSync(c));
}

// ---------------------------------------------------------------------------
// Main
// ---------------------------------------------------------------------------

console.log('\n--- redirect targets (redirects.js) ---');

if (!fs.existsSync(BUILD_DIR)) {
  console.error(`Build directory not found: ${BUILD_DIR}`);
  console.error('Run "npm run build" first.');
  process.exit(1);
}

ok(`${redirects.length} redirects loaded from redirects.js`);

// Check for duplicate `from` entries
const fromCounts = {};
for (const r of redirects) {
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

// Check for redirect chains (to → from)
const fromSet = new Set(redirects.map((r) => r.from));
for (const r of redirects) {
  if (fromSet.has(r.to)) {
    warn(`redirect chain: ${r.from} → ${r.to} → ... (target is itself a redirect source)`);
  }
}

// Verify each target resolves to an existing page
let checked = 0;
let skipped = 0;

for (const r of redirects) {
  if (!r.to.startsWith('/docs/')) {
    skipped++;
    continue;
  }

  checked++;
  const rel = r.to.replace(/^\/docs\//, '').replace(/\/$/, '');
  if (targetExists(rel)) {
    continue;
  }
  if (targetExistsOnDevel(rel)) {
    warn(`(devel-only): ${r.to} — resolves on devel, awaits next release (from: ${r.from})`);
    develOnlyWarnings++;
    continue;
  }
  error(`redirect target ${r.to} has no HTML page (from: ${r.from}, checked ${rel}.html and ${rel}/index.html)`);
}

if (skipped > 0) {
  ok(`skipped ${skipped} non-/docs/ targets`);
}

if (errors === 0) {
  if (develOnlyWarnings === 0) {
    ok(`all ${checked} checked redirect targets resolve to existing pages`);
  } else {
    const masterResolved = checked - develOnlyWarnings;
    ok(`${masterResolved}/${checked} checked redirect targets resolve in master; ${develOnlyWarnings} resolve only on devel (await next release)`);
  }
}

// Summary
console.log(`\n=== Redirects: ${errors} errors, ${warnings} warnings ===`);
if (errors > 0) {
  process.exit(1);
}
