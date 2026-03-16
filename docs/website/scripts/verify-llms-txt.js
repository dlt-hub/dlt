#!/usr/bin/env node
/**
 * Post-build verification for the llms-txt plugin.
 *
 * Checks that .md files and llms.txt indexes are correct and consistent
 * with the sidebar configuration. Run after `npm run build`:
 *
 *   node scripts/verify-llms-txt.js
 *
 * Exits with code 1 if any errors are found. Warnings are informational.
 */
const fs = require('fs');
const path = require('path');

// ---------------------------------------------------------------------------
// Config — must match docusaurus.config.js plugin options
// ---------------------------------------------------------------------------
const BUILD_DIR = path.resolve(__dirname, '..', 'build', 'docs');
const SITE_DIR = path.resolve(__dirname, '..');
const SIDEBARS_PATH = path.join(SITE_DIR, 'sidebars.js');
const EXCLUDE_FROM_MD = ['api_reference/'];
const EXCLUDE_FROM_INDEX = ['devel/'];
const SEPARATE_INDEX_PREFIXES = ['hub/'];
const HIDE_MD_PATTERNS = ['/api_reference/'];

const LLMS_TXT_FILES = [
  {path: 'llms.txt', sidebar: 'docsSidebar', label: 'main'},
  {path: 'hub/llms.txt', sidebar: 'hubSidebar', label: 'hub'},
];

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------
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

function walkFiles(dir, predicate, results = []) {
  if (!fs.existsSync(dir)) return results;
  for (const entry of fs.readdirSync(dir, {withFileTypes: true})) {
    const full = path.join(dir, entry.name);
    if (entry.isDirectory()) {
      walkFiles(full, predicate, results);
    } else if (predicate(entry.name)) {
      results.push(full);
    }
  }
  return results;
}

/** Extract all doc IDs from a Docusaurus sidebar tree (recursive). */
function collectSidebarDocIds(items, ids = new Set()) {
  for (const item of items) {
    if (typeof item === 'string') {
      ids.add(item);
      continue;
    }
    if (!item || typeof item !== 'object') continue;
    if (item.type === 'doc') {
      ids.add(item.id);
    } else if (item.type === 'category') {
      if (item.link && item.link.type === 'doc' && item.link.id) {
        ids.add(item.link.id);
      }
      if (item.items) collectSidebarDocIds(item.items, ids);
    }
    // skip 'ref' and 'link' — refs point to other sidebars, links are external
  }
  return ids;
}

/**
 * Parse an llms.txt file and return {entries: [{title, url}], groups: string[]}.
 * Entry URLs are the raw markdown link targets from the file.
 */
function parseLlmsTxt(content) {
  const entries = [];
  const groups = [];
  for (const line of content.split('\n')) {
    const groupMatch = line.match(/^## (.+)/);
    if (groupMatch) {
      groups.push(groupMatch[1]);
      continue;
    }
    const entryMatch = line.match(/^- \[(.+?)\]\((.+?)\)/);
    if (entryMatch) {
      entries.push({title: entryMatch[1], url: entryMatch[2]});
    }
  }
  return {entries, groups};
}

// ---------------------------------------------------------------------------
// Checks
// ---------------------------------------------------------------------------

function checkLlmsTxtFile(llmsConfig, sidebars) {
  const label = llmsConfig.label;
  const llmsPath = path.join(BUILD_DIR, llmsConfig.path);

  console.log(`\n--- ${label} (${llmsConfig.path}) ---`);

  // 1. File exists
  if (!fs.existsSync(llmsPath)) {
    error(`${llmsConfig.path} not found at ${llmsPath}`);
    return;
  }
  ok(`${llmsConfig.path} exists`);

  const content = fs.readFileSync(llmsPath, 'utf8');
  const {entries, groups} = parseLlmsTxt(content);

  // 2. Non-empty
  if (entries.length === 0) {
    error(`${llmsConfig.path} has no entries`);
    return;
  }
  ok(`${entries.length} entries in ${groups.length} groups`);

  // 3. Every .md link resolves to an actual file in build/
  let missingMd = 0;
  for (const entry of entries) {
    // URL is like /docs/general-usage/schema.md — strip leading /docs/ to get build-relative path
    const relPath = entry.url.replace(/^\/docs\//, '');
    const fullPath = path.join(BUILD_DIR, relPath);
    if (!fs.existsSync(fullPath)) {
      error(`linked .md not found: ${entry.url} (expected ${fullPath})`);
      missingMd++;
    }
  }
  if (missingMd === 0) ok('all linked .md files exist');

  // 3b. No excluded paths leaked into the index
  let excludedLeaks = 0;
  for (const entry of entries) {
    const relPath = entry.url.replace(/^\/docs\//, '');
    if (EXCLUDE_FROM_MD.some((pat) => relPath.includes(pat))) {
      error(`excluded path in index: ${entry.url} (matches excludeFromMd)`);
      excludedLeaks++;
    }
  }
  if (excludedLeaks === 0) ok('no excluded paths in index');

  // 4. No duplicate entries
  const urls = entries.map((e) => e.url);
  const dupes = urls.filter((u, i) => urls.indexOf(u) !== i);
  if (dupes.length > 0) {
    for (const d of [...new Set(dupes)]) error(`duplicate entry: ${d}`);
  } else {
    ok('no duplicate entries');
  }

  // 5. Every entry has a non-empty title (not just a filename fallback)
  const untitled = entries.filter((e) => /^[a-z0-9_-]+$/.test(e.title));
  if (untitled.length > 0) {
    for (const e of untitled) warn(`possible missing title (filename-like): "${e.title}" at ${e.url}`);
  } else {
    ok('all entries have proper titles');
  }

  // 6. No orphan/fallback groups — every group should be a sidebar group
  if (llmsConfig.sidebar && sidebars[llmsConfig.sidebar]) {
    const sidebarItems = sidebars[llmsConfig.sidebar];
    const sidebarGroupNames = new Set();
    collectSidebarGroupNames(sidebarItems, [], 2, sidebarGroupNames);

    const orphanGroups = groups.filter((g) => !sidebarGroupNames.has(g));
    if (orphanGroups.length > 0) {
      for (const g of orphanGroups) error(`orphan group not in sidebar: "${g}"`);
    } else {
      ok('all groups match sidebar categories');
    }
  }

  // 7. Sidebar coverage — every sidebar doc that has a source .md should appear in llms.txt
  if (llmsConfig.sidebar && sidebars[llmsConfig.sidebar]) {
    checkSidebarCoverage(llmsConfig, sidebars, urls);
  }
}

/** Collect sidebar group names (matching buildSidebarMap logic). */
function collectSidebarGroupNames(items, breadcrumb, groupDepth, names) {
  for (const item of items) {
    if (typeof item === 'string') {
      const group = breadcrumb.slice(0, groupDepth).join(' > ');
      if (group) names.add(group);
      continue;
    }
    if (!item || typeof item !== 'object') continue;
    if (item.type === 'doc') {
      const group = breadcrumb.slice(0, groupDepth).join(' > ');
      if (group) names.add(group);
      continue;
    }
    if (item.type === 'category') {
      const newBreadcrumb = [...breadcrumb, item.label];
      if (item.link && item.link.type === 'doc' && item.link.id) {
        const group = newBreadcrumb.slice(0, groupDepth).join(' > ');
        if (group) names.add(group);
      }
      if (item.items) {
        collectSidebarGroupNames(item.items, newBreadcrumb, groupDepth, names);
      }
    }
  }
}

/**
 * Check that every sidebar doc ID appears in llms.txt.
 * A doc might be absent if: (a) no source .md exists, or (b) the plugin failed to map it.
 * We only flag (b) — docs with an actual .md file that are missing from the index.
 */
function checkSidebarCoverage(llmsConfig, sidebars, indexedUrls) {
  const sidebarDocIds = collectSidebarDocIds(sidebars[llmsConfig.sidebar]);
  const indexedSet = new Set(indexedUrls);

  // Determine the source dir for checking if a .md exists
  // For main sidebar → versioned_docs/version-master, for hub → same
  const versionedDir = path.join(SITE_DIR, 'versioned_docs', 'version-master');
  const sourceDir = fs.existsSync(versionedDir)
    ? versionedDir
    : path.join(SITE_DIR, 'docs_processed');

  // Build a slug map so we can resolve slug overrides to their actual URL
  const slugToDocId = {}; // { "dir/slug": "dir/original-name" }
  for (const docId of sidebarDocIds) {
    const candidates = [
      path.join(sourceDir, docId + '.md'),
      path.join(sourceDir, docId + '.mdx'),
    ];
    for (const c of candidates) {
      if (fs.existsSync(c)) {
        const content = fs.readFileSync(c, 'utf8');
        const slugMatch = content.match(/^---\r?\n[\s\S]*?^slug\s*:\s*(.+)/m);
        if (slugMatch) {
          const slug = slugMatch[1].replace(/^['"]|['"]$/g, '').trim();
          const dir = path.dirname(docId);
          const slugPath = dir === '.' ? slug : `${dir}/${slug}`;
          slugToDocId[docId] = slugPath;
        }
        break;
      }
    }
  }

  const urlPrefix = '/docs/';

  let missing = 0;
  for (const docId of sidebarDocIds) {
    // Skip docs under excluded paths — they shouldn't be in the index (checked
    // separately in checkLlmsTxtFile), and they don't get .md files generated
    if (EXCLUDE_FROM_MD.some((pat) => docId.includes(pat))) continue;

    // Check if source .md exists
    const lastSegment = path.basename(docId);
    const candidates = [
      path.join(sourceDir, docId + '.md'),
      path.join(sourceDir, docId + '.mdx'),
      path.join(sourceDir, docId, 'index.md'),
      path.join(sourceDir, docId, 'index.mdx'),
      path.join(sourceDir, docId, lastSegment + '.md'),
      path.join(sourceDir, docId, lastSegment + '.mdx'),
    ];
    const hasSource = candidates.some((c) => fs.existsSync(c));
    if (!hasSource) continue; // no source file — generated-index or similar, OK to skip

    // Determine what URL this doc would have in llms.txt
    // Apply transformations matching Docusaurus URL generation:
    //   1. Slug override: frontmatter slug replaces the filename in the URL
    //   2. Index collapse: foo/index → foo
    //   3. Dir collapse: foo/foo → foo (Docusaurus collapses when filename = dirname)
    let urlPath;
    if (slugToDocId[docId]) {
      urlPath = slugToDocId[docId];
    } else {
      urlPath = docId.replace(/\/index$/, '');
      // Dir-collapsed: foo/foo → foo
      const parts = urlPath.split('/');
      if (parts.length >= 2 && parts[parts.length - 1] === parts[parts.length - 2]) {
        parts.pop();
        urlPath = parts.join('/');
      }
    }
    const expectedUrl = `${urlPrefix}${urlPath}.md`;

    if (!indexedSet.has(expectedUrl)) {
      warn(`sidebar doc "${docId}" has source .md but missing from ${llmsConfig.path} (expected ${expectedUrl})`);
      missing++;
    }
  }
  if (missing === 0) {
    ok(`all ${sidebarDocIds.size} sidebar docs with sources are indexed`);
  } else {
    warn(`${missing} sidebar docs with sources missing from index`);
  }
}

function checkMdFileQuality() {
  console.log('\n--- .md file quality ---');

  const mdFiles = walkFiles(BUILD_DIR, (name) => name.endsWith('.md'));
  ok(`${mdFiles.length} .md files in build output`);

  let emptyCount = 0;
  let importLeaks = 0;
  let componentLeaks = 0;

  for (const mdFile of mdFiles) {
    const content = fs.readFileSync(mdFile, 'utf8');
    const rel = path.relative(BUILD_DIR, mdFile);

    // Empty or frontmatter-only
    const stripped = content.replace(/^---\r?\n[\s\S]*?\r?\n---\s*/, '').trim();
    if (stripped.length === 0) {
      warn(`empty content (frontmatter-only): ${rel}`);
      emptyCount++;
    }

    // Leaked MDX imports
    const lines = content.split('\n');
    for (let i = 0; i < lines.length; i++) {
      if (/^import\s+.+\s+from\s+['"]/.test(lines[i])) {
        error(`MDX import leaked: ${rel}:${i + 1}: ${lines[i].trim()}`);
        importLeaks++;
      }
    }

    // Leaked self-closing React components (uppercase tag with />)
    for (let i = 0; i < lines.length; i++) {
      if (/^\s*<[A-Z][A-Za-z]*[\s/]/.test(lines[i]) && /\/>\s*$/.test(lines[i])) {
        error(`React component leaked: ${rel}:${i + 1}: ${lines[i].trim()}`);
        componentLeaks++;
      }
    }
  }

  if (emptyCount === 0) ok('no empty .md files');
  if (importLeaks === 0) ok('no MDX import leaks');
  if (componentLeaks === 0) ok('no React component leaks');
}

function checkExcludeConsistency() {
  console.log('\n--- exclude pattern consistency ---');

  // Check that HIDE_MD_PATTERNS (badge component) covers excludeFromMd (plugin)
  for (const pat of EXCLUDE_FROM_MD) {
    const normalized = pat.startsWith('/') ? pat : '/' + pat;
    if (!HIDE_MD_PATTERNS.some((h) => normalized.includes(h) || h.includes(normalized))) {
      error(`excludeFromMd pattern "${pat}" not covered by HIDE_MD_PATTERNS in DocMarkdownLink`);
    }
  }

  // Check no .md files were generated for excluded paths
  for (const pat of EXCLUDE_FROM_MD) {
    const mdFiles = walkFiles(BUILD_DIR, (name) => name.endsWith('.md'));
    const violations = mdFiles.filter((f) => {
      const rel = path.relative(BUILD_DIR, f);
      return rel.includes(pat);
    });
    if (violations.length > 0) {
      for (const v of violations) error(`excluded path has .md: ${path.relative(BUILD_DIR, v)}`);
    }
  }

  ok('exclude patterns consistent');
}

function checkHtmlMdParity() {
  console.log('\n--- HTML/MD parity (badge 404 check) ---');

  const htmlFiles = walkFiles(BUILD_DIR, (name) => name.endsWith('.html'));
  let badge404 = 0;

  for (const htmlFile of htmlFiles) {
    const rel = path.relative(BUILD_DIR, htmlFile).split(path.sep).join('/');
    const basename = path.basename(rel);

    // Skip non-doc pages
    if (
      rel === '404.html' ||
      rel === 'search.html' ||
      rel.startsWith('assets/') ||
      rel.startsWith('search/')
    ) {
      continue;
    }

    // Skip underscore-prefixed partials (MDX fragments) — they use a different
    // layout and don't render the badge
    if (basename.startsWith('_')) continue;

    // Skip excluded patterns — badge is hidden for these
    if (HIDE_MD_PATTERNS.some((p) => ('/' + rel).includes(p))) continue;

    // Skip paths excluded from index and separate index prefixes — these are
    // either non-master versions or have their own llms.txt checked separately
    const skipPrefixes = [...EXCLUDE_FROM_INDEX, ...SEPARATE_INDEX_PREFIXES];
    if (skipPrefixes.some((p) => rel.startsWith(p))) continue;

    // The badge constructs the URL as pathname + '.md', which maps to stem.md
    const mdPath = path.join(BUILD_DIR, rel.replace(/\.html$/, '.md'));
    if (!fs.existsSync(mdPath)) {
      // These are generated-index pages (sidebar categories with auto-generated
      // listing pages, no source .md). The DocMarkdownLink badge still renders
      // on them via DocItem/Layout, producing a 404 link.
      warn(`HTML page has no .md (badge would 404): ${rel}`);
      badge404++;
    }
  }

  if (badge404 === 0) {
    ok('every HTML page with badge has a .md file');
  } else {
    warn(`${badge404} HTML pages would show badge with 404 .md link`);
  }
}

// ---------------------------------------------------------------------------
// Main
// ---------------------------------------------------------------------------

console.log(`Verifying llms-txt build output in ${BUILD_DIR}\n`);

if (!fs.existsSync(BUILD_DIR)) {
  console.error(`Build directory not found: ${BUILD_DIR}`);
  console.error('Run "npm run build" first.');
  process.exit(1);
}

// Load sidebars
let sidebars = {};
try {
  sidebars = require(SIDEBARS_PATH);
} catch (e) {
  warn(`Could not load sidebars.js: ${e.message}`);
}

// Run checks
for (const llmsConfig of LLMS_TXT_FILES) {
  checkLlmsTxtFile(llmsConfig, sidebars);
}
checkMdFileQuality();
checkExcludeConsistency();
checkHtmlMdParity();

// Summary
console.log(`\n=== Summary: ${errors} errors, ${warnings} warnings ===`);
if (errors > 0) {
  process.exit(1);
}
