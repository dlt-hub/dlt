// Detects orphan docs pages: pages not reachable from any sidebar (directly, via a
// category `link`, or as an item in sidebars.js). Orphan pages build fine but render
// without a sidebar and cannot be discovered through navigation.
//
// A page may opt out by setting `unlisted: true` in its frontmatter (the official
// Docusaurus marker: noindex + unlisted banner + excluded from search). Any other
// orphan fails the check.
//
// Usage:
//   node tools/check_orphan_docs.js            fail on orphans not marked `unlisted: true`
//   node tools/check_orphan_docs.js --all      list and fail on any orphan, unlisted or not
const fs = require("node:fs");
const path = require("node:path");

const WEBSITE_DIR = path.join(__dirname, "..");
const DOCS_DIR = path.join(WEBSITE_DIR, "docs");

function loadSidebarIds() {
  // sidebars.js scans docs_processed/examples at require time; make sure it exists
  // so the check can run before `make preprocess-docs` (e.g. in the lint CI step)
  fs.mkdirSync(path.join(WEBSITE_DIR, "docs_processed", "examples"), { recursive: true });
  process.chdir(WEBSITE_DIR);
  const sidebars = require(path.join(WEBSITE_DIR, "sidebars.js"));
  const ids = new Set();
  (function walk(node) {
    if (typeof node === "string") ids.add(node);
    else if (Array.isArray(node)) node.forEach(walk);
    else if (node && typeof node === "object") {
      if (node.id) ids.add(node.id);
      if (node.link?.id) ids.add(node.link.id);
      if (node.items) walk(node.items);
    }
  })(Object.values(sidebars));
  return ids;
}

function* walkDocs(dir) {
  for (const entry of fs.readdirSync(dir, { withFileTypes: true })) {
    if (entry.name.startsWith("_") || entry.name === "node_modules") continue;
    const full = path.join(dir, entry.name);
    if (entry.isDirectory()) yield* walkDocs(full);
    else if (/\.mdx?$/.test(entry.name)) yield full;
  }
}

function collectDocs() {
  const docs = [];
  for (const file of walkDocs(DOCS_DIR)) {
    const rel = path.relative(DOCS_DIR, file).replace(/\\/g, "/");
    let id = rel.replace(/\.mdx?$/, "");
    let unlisted = false;
    const frontmatter = fs.readFileSync(file, "utf-8").match(/^---\n([\s\S]*?)\n---/);
    if (frontmatter) {
      unlisted = /^unlisted:\s*true\s*$/m.test(frontmatter[1]);
      const idOverride = frontmatter[1].match(/^id:\s*(\S+)\s*$/m);
      if (idOverride) id = [...id.split("/").slice(0, -1), idOverride[1]].join("/");
    }
    docs.push({ id, rel, unlisted });
  }
  return docs;
}

function main() {
  const includeUnlisted = process.argv.includes("--all");
  const sidebarIds = loadSidebarIds();
  const docs = collectDocs();

  const orphans = docs.filter((d) => !sidebarIds.has(d.id));
  const failing = includeUnlisted ? orphans : orphans.filter((d) => !d.unlisted);
  const unlistedCount = orphans.filter((d) => d.unlisted).length;

  if (failing.length) {
    console.error(`ERROR: ${failing.length} docs page(s) not reachable from any sidebar:`);
    for (const d of failing.sort((a, b) => a.id.localeCompare(b.id))) {
      console.error(`  ${d.id}${d.unlisted ? "  (unlisted)" : ""}  (docs/${d.rel})`);
    }
    console.error("");
    console.error("Fix by either:");
    console.error("  - adding the page to sidebars.js (as an item or a category `link`)");
    console.error("  - marking it `unlisted: true` in frontmatter if it is intentionally unlisted");
    console.error("  - deleting it and adding a redirect to redirects.js");
    return 1;
  }

  console.log(
    `OK: all ${docs.length} docs pages reachable from sidebars (${unlistedCount} unlisted pages pending cleanup)`,
  );
  return 0;
}

process.exit(main());
