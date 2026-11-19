/** Collects version metadata (`since`, `status`, `package`) from markdown frontmatter and `<!-- since: -->` section directives. */
const fs = require("node:fs");
const path = require("node:path");
const { parseFileContentFrontMatter } = require("@docusaurus/utils/lib/markdownUtils");
const { createSlugger } = require("@docusaurus/utils");
const { toConstraints, validateMeta } = require("./badge");
const { SINCE_DIRECTIVE, parseBlock } = require("./directive");

/** heading anchors (as Docusaurus slugs them) mapped to the constraints of their `since` directive */
function scanHeadings(fileContent, filePath) {
  const slugger = createSlugger();
  const found = {};
  let fenced = false;
  const lines = fileContent.split("\n");
  lines.forEach((line, i) => {
    if (/^\s*(```|~~~)/.test(line)) fenced = !fenced;
    const m = !fenced && /^#{1,6} (.*?)[ \t]*(\{#([\w-]+)\})?[ \t]*$/.exec(line);
    if (!m) return;
    const text = m[1].replace(/\[([^\]]*)\]\([^)]*\)/g, "$1").replace(/[`*_]/g, "");
    const id = m[3] || slugger.slug(text);
    const block = [];
    let j = i + 1;
    if (lines[j] !== undefined && lines[j].trim() === "") j++;
    for (; j < lines.length && /^<!--[ \t]*since:.*-->[ \t]*$/.test(lines[j]); j++) block.push(lines[j]);
    if (block.length) found[id] = parseBlock(block.join("\n"), filePath);
  });
  return found;
}

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
  return { page, sections, constraints, headings: scanHeadings(fileContent, filePath) };
}

/**
 * @returns {{pages: Record<string, object>, constraints: Record<string, string>[]}} `pages` keyed by doc id
 *   (path without extension), only pages with version metadata; `headings` are the tagged sections per doc id
 */
function scanDocs(dir) {
  const pages = {};
  const headings = {};
  const constraints = [];
  for (const rel of fs.readdirSync(dir, { recursive: true })) {
    if (!/\.mdx?$/.test(rel)) continue;
    const scan = scanFile(path.join(dir, rel));
    constraints.push(...scan.constraints);
    const docId = rel.split(path.sep).join("/").replace(/\.mdx?$/, "");
    if (Object.keys(scan.headings).length) headings[docId] = scan.headings;
    if (scan.page) pages[rel.split(path.sep).join("/").replace(/\.mdx?$/, "")] = scan.page;
  }
  return { pages, headings, constraints };
}

module.exports = { scanFile, scanDocs };
