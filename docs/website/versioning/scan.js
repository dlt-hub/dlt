/** Collects version metadata (`since`, `status`, `package`) from markdown frontmatter and `<!-- since: -->` section directives. */
const fs = require("node:fs");
const path = require("node:path");
const { parseFileContentFrontMatter } = require("@docusaurus/utils/lib/markdownUtils");
const { DEFAULT_PACKAGE, validateMeta } = require("./badge");
const { SINCE_DIRECTIVE } = require("./directive");

/**
 * @returns {{page: {since?: string, status?: string, package: string} | null, packages: string[]}}
 *   `page` is null without version metadata; `packages` lists packages the page and its sections use
 */
function scanFile(filePath) {
  const fileContent = fs.readFileSync(filePath, "utf8");
  const packages = new Set();
  for (const [directive, , , pkg, since] of fileContent.matchAll(SINCE_DIRECTIVE)) {
    validateMeta({ since, package: pkg }, `${filePath}: ${directive}`);
    packages.add(pkg || DEFAULT_PACKAGE);
  }
  const { since, status, package: pkg } = parseFileContentFrontMatter(fileContent).frontMatter;
  if (since === undefined && status === undefined) return { page: null, packages: [...packages] };
  validateMeta({ since, status, package: pkg }, filePath);
  packages.add(pkg || DEFAULT_PACKAGE);
  return { page: { since, status, package: pkg || DEFAULT_PACKAGE }, packages: [...packages] };
}

/**
 * @returns {{pages: Record<string, object>, packages: Set<string>}} `pages` keyed by doc id
 *   (path without extension), only pages with version metadata
 */
function scanDocs(dir) {
  const pages = {};
  const packages = new Set();
  for (const rel of fs.readdirSync(dir, { recursive: true })) {
    if (!/\.mdx?$/.test(rel)) continue;
    const scan = scanFile(path.join(dir, rel));
    scan.packages.forEach((p) => packages.add(p));
    if (scan.page) pages[rel.split(path.sep).join("/").replace(/\.mdx?$/, "")] = scan.page;
  }
  return { pages, packages };
}

module.exports = { scanFile, scanDocs };
