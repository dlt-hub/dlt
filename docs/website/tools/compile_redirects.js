#!/usr/bin/env node
/**
 * Compile per-version redirect sources into a single, version-prefixed list.
 *
 * Sources:
 *   - ./redirects.js                          (devel — the current branch)
 *   - ./versioned_redirects/version-<v>.js    (each snapshotted version)
 *
 * Per the version-path map in docusaurus.config.js:
 *   - current (the "devel"-labelled version) is served at /docs/devel/...
 *   - master is served at /docs/...
 *   - any other version v is served at /docs/<v>/...  (Docusaurus default)
 *
 * For each entry whose `from` and `to` both start with /docs/, this script
 * rewrites the leading /docs/ to /docs/<path>/, so the worker can match
 * exact pathnames against version-scoped URLs. Entries whose from/to do not
 * start with /docs/ (e.g. the root catchall { from: "/", ... }) are passed
 * through unchanged.
 *
 * Output: ./redirects.compiled.js (build artifact, gitignored).
 *
 * The pure {@link compile} function is exported so verify-redirects.js can
 * recompute the compiled output in-memory and detect staleness.
 */
const fs = require("node:fs");
const path = require("node:path");

const WEBSITE_DIR = path.resolve(__dirname, "..");
const CURRENT_REDIRECTS_FILE = path.join(WEBSITE_DIR, "redirects.js");
const VERSIONED_REDIRECTS_DIR = path.join(WEBSITE_DIR, "versioned_redirects");
const OUTPUT_FILE = path.join(WEBSITE_DIR, "redirects.compiled.js");

const CURRENT_VERSION_KEY = "current";

/**
 * Return the URL path segment for a given version, matching the mapping in
 * docusaurus.config.js. Empty string means "served at the docs root".
 */
function versionPath(version) {
  if (version === CURRENT_VERSION_KEY) return "devel";
  if (version === "master") return "";
  return version;
}

function prefixDocsPath(p, versionPathSegment) {
  if (!p.startsWith("/docs/") && p !== "/docs") return p;
  if (versionPathSegment === "") return p;
  if (p === "/docs") return `/docs/${versionPathSegment}`;
  return `/docs/${versionPathSegment}/${p.slice("/docs/".length)}`;
}

function isPassthrough(entry) {
  // An entry is "version-independent" iff its `from` does not start with
  // /docs/
  return !entry.from.startsWith("/docs");
}

function transformVersionEntries(entries, version) {
  const segment = versionPath(version);
  const passthrough = [];
  const scoped = [];
  for (const entry of entries) {
    if (isPassthrough(entry)) {
      passthrough.push(entry);
      continue;
    }
    scoped.push({
      from: prefixDocsPath(entry.from, segment),
      to: prefixDocsPath(entry.to, segment),
    });
  }
  return { passthrough, scoped };
}

function loadModule(filePath) {
  delete require.cache[require.resolve(filePath)];
  return require(filePath);
}

function discoverSnapshottedVersions() {
  if (!fs.existsSync(VERSIONED_REDIRECTS_DIR)) return [];
  return fs
    .readdirSync(VERSIONED_REDIRECTS_DIR)
    .filter((f) => f.startsWith("version-") && f.endsWith(".js"))
    .map((f) => f.slice("version-".length, -".js".length))
    .sort();
}

/**
 * Pure compile: returns the final array of {from, to} entries and a list of
 * the input files involved (used by the verifier for staleness diagnostics).
 */
function compile() {
  const snapshotted = discoverSnapshottedVersions();
  if (snapshotted.length === 0) {
    throw new Error(
      `No snapshotted versions found in ${path.relative(WEBSITE_DIR, VERSIONED_REDIRECTS_DIR) || VERSIONED_REDIRECTS_DIR}. ` +
        `Run 'npm run update-versions' first — it clones each tag and writes ` +
        `versioned_redirects/version-<v>.js. Refusing to compile a partial redirect ` +
        `map (devel only) because it would silently drop coverage for master and any ` +
        `other versioned releases.`,
    );
  }

  const versions = [];

  if (fs.existsSync(CURRENT_REDIRECTS_FILE)) {
    versions.push({
      version: CURRENT_VERSION_KEY,
      file: CURRENT_REDIRECTS_FILE,
      entries: loadModule(CURRENT_REDIRECTS_FILE),
    });
  }

  for (const v of snapshotted) {
    const file = path.join(VERSIONED_REDIRECTS_DIR, `version-${v}.js`);
    versions.push({ version: v, file, entries: loadModule(file) });
  }

  const passthroughSeen = new Set();
  const passthrough = [];
  const grouped = [];

  for (const v of versions) {
    const { passthrough: p, scoped } = transformVersionEntries(v.entries, v.version);
    for (const entry of p) {
      const key = `${entry.from} ${entry.to}`;
      if (passthroughSeen.has(key)) continue;
      passthroughSeen.add(key);
      passthrough.push(entry);
    }
    grouped.push({ version: v.version, path: versionPath(v.version), entries: scoped });
  }

  const compiled = [...passthrough];
  for (const g of grouped) {
    for (const entry of g.entries) compiled.push(entry);
  }

  return {
    compiled,
    groups: grouped,
    passthrough,
    inputs: versions.map((v) => v.file),
  };
}

function renderEntry(entry) {
  return `  {\n    from: ${JSON.stringify(entry.from)},\n    to: ${JSON.stringify(entry.to)}\n  },\n`;
}

function renderOutput(result) {
  let out = "";
  out += "// AUTO-GENERATED by tools/compile_redirects.js — do not edit by hand.\n";
  out += "// Edit ./redirects.js (devel) or ./versioned_redirects/version-<v>.js\n";
  out += "// (regenerated by tools/update_versions.js from each cloned tag), then\n";
  out += "// run 'npm run compile-redirects'.\n";
  out += "\n";
  out += "/** @type {Array<{from: string, to: string}>} */\n";
  out += "const REDIRECTS = [\n";

  if (result.passthrough.length > 0) {
    out += "  // pass-through (no /docs/ prefix; apply to every version)\n";
    for (const entry of result.passthrough) out += renderEntry(entry);
    out += "\n";
  }

  for (const group of result.groups) {
    const label =
      group.path === "" ? `${group.version} (served at /docs/)` : `${group.version} (served at /docs/${group.path}/)`;
    out += `  // ${label}\n`;
    if (group.entries.length === 0) {
      out += "  // (no version-scoped entries)\n";
    } else {
      for (const entry of group.entries) out += renderEntry(entry);
    }
    out += "\n";
  }

  out += "];\n\n";
  out += "module.exports = REDIRECTS;\n";
  return out;
}

function writeCompiled() {
  const result = compile();
  const text = renderOutput(result);
  fs.writeFileSync(OUTPUT_FILE, text, "utf8");
  return { result, text };
}

if (require.main === module) {
  try {
    const { result } = writeCompiled();
    const total = result.compiled.length;
    const groupSummary = result.groups.map((g) => `${g.version}=${g.entries.length}`).join(", ");
    console.log(
      `compile-redirects: wrote ${path.relative(WEBSITE_DIR, OUTPUT_FILE)} ` +
        `(${total} entries — passthrough=${result.passthrough.length}, ${groupSummary})`,
    );
  } catch (err) {
    console.error(`compile-redirects: ${err.message}`);
    process.exit(1);
  }
}

module.exports = { compile, renderOutput, writeCompiled, OUTPUT_FILE, VERSIONED_REDIRECTS_DIR, CURRENT_REDIRECTS_FILE };
