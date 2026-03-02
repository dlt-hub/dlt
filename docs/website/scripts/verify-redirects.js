#!/usr/bin/env node
/**
 * Post-build verification for worker.ts redirect targets.
 *
 * Parses the REDIRECTS array from worker.ts and checks that every `to` target
 * resolves to an existing HTML page in the build output. Run after
 * `docusaurus build`:
 *
 *   node scripts/verify-redirects.js
 *
 * Exits with code 1 if any errors are found.
 */
const fs = require('fs');
const path = require('path');

const BUILD_DIR = path.resolve(__dirname, '..', 'build', 'docs');
const WORKER_PATH = path.resolve(__dirname, '..', 'worker.ts');

let errors = 0;

function error(msg) {
  console.error(`  ERROR: ${msg}`);
  errors++;
}

function ok(msg) {
  console.log(`  OK:    ${msg}`);
}

function extractRedirectTargets() {
  const content = fs.readFileSync(WORKER_PATH, 'utf8');
  const targets = new Set();
  for (const match of content.matchAll(/to:\s*["']([^"']+)["']/g)) {
    targets.add(match[1]);
  }
  return [...targets];
}

function targetExists(rel) {
  const candidates = [
    path.join(BUILD_DIR, rel + '.html'),
    path.join(BUILD_DIR, rel, 'index.html'),
  ];
  return candidates.some((c) => fs.existsSync(c));
}

// ---------------------------------------------------------------------------
// Main
// ---------------------------------------------------------------------------

console.log('\n--- redirect targets (worker.ts) ---');

if (!fs.existsSync(BUILD_DIR)) {
  console.error(`Build directory not found: ${BUILD_DIR}`);
  console.error('Run "npm run build" first.');
  process.exit(1);
}

if (!fs.existsSync(WORKER_PATH)) {
  console.error(`Worker file not found: ${WORKER_PATH}`);
  process.exit(1);
}

const targets = extractRedirectTargets();

for (const target of targets.sort()) {
  if (!target.startsWith('/docs/')) continue;

  const rel = target.replace(/^\/docs\//, '').replace(/\/$/, '');
  if (!targetExists(rel)) {
    error(`redirect target ${target} has no HTML page (checked ${rel}.html and ${rel}/index.html)`);
  }
}

if (errors === 0) {
  ok(`all ${targets.length} redirect targets resolve to existing pages`);
} else {
  console.error(`\n  ${errors} redirect target(s) are broken`);
}

console.log(`\n=== Redirects: ${errors} errors ===`);
if (errors > 0) {
  process.exit(1);
}
