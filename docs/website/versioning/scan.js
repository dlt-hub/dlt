/** Collects version metadata (`since`, `status`, `package`) from markdown frontmatter and `<!-- since: -->` section directives. */
const fs = require("node:fs");
const path = require("node:path");
const { parseFileContentFrontMatter } = require("@docusaurus/utils/lib/markdownUtils");
const { toConstraints, validateMeta } = require("./badge");
const { SINCE_DIRECTIVE, parseBlock } = require("./directive");

/**
 * @returns {{page: {since?: string | object, status?: string, package?: string} | null,
 *   sections: Record<string, string>[], constraints: Record<string, string>[]}}
 *   `page` is null without version metadata; `sections` are the constraints of each tagged section;
 *   `constraints` are all constraints of the page and its sections
 */
function scanFile(filePath) {
  const fileContent = fs.readFileSync(filePath, "utf8");
  const sections = [];
  for (const [, , , block] of fileContent.matchAll(SINCE_DIRECTIVE)) {
    const constraints = parseBlock(block, filePath);
    validateMeta({ since: constraints }, `${filePath}: ${block}`);
    sections.push(constraints);
  }
  const { since, status, package: pkg } = parseFileContentFrontMatter(fileContent).frontMatter;
  const constraints = [...sections];
  let page = null;
  if (since !== undefined || status !== undefined) {
    validateMeta({ since, status, package: pkg }, filePath);
    page = { since, status, package: pkg };
    constraints.push(toConstraints(page));
  }
  return { page, sections, constraints };
}

/**
 * @returns {{pages: Record<string, object>, constraints: Record<string, string>[]}} `pages` keyed by doc id
 *   (path without extension), only pages with version metadata
 */
function scanDocs(dir) {
  const pages = {};
  const constraints = [];
  for (const rel of fs.readdirSync(dir, { recursive: true })) {
    if (!/\.mdx?$/.test(rel)) continue;
    const scan = scanFile(path.join(dir, rel));
    constraints.push(...scan.constraints);
    if (scan.page) pages[rel.split(path.sep).join("/").replace(/\.mdx?$/, "")] = scan.page;
  }
  return { pages, constraints };
}

module.exports = { scanFile, scanDocs };
