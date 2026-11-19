/**
 * The section directive: one or more HTML comments right below a heading (a blank line between is fine), invisible in plain markdown.
 *   ## Heading
 *   <!-- since: 1.32.0 -->            (package defaults to dlt)
 *   <!-- since: dlthub-client 0.4.0 -->
 *   <!-- since: dlt 1.32.0 -->        (one comment per package, stacked one after another)
 * Docusaurus drops comments while parsing, so no remark plugin can see them. `preprocess` runs
 * on the raw source instead (docusaurus `markdown.preprocessor`) and moves the directive into
 * the heading as a `<Since>` component, which renders the badge.
 */
const { DEFAULT_PACKAGE, formatConstraints } = require("./badge");

const COMMENT = String.raw`<!--[ \t]*since:[^\n]*?-->`;
/** groups: heading, explicit id, block of stacked `since` comments */
const SINCE_DIRECTIVE = new RegExp(String.raw`^(#{1,6} .*?)[ \t]*(\{#[\w-]+\})?[ \t]*\n(?:[ \t]*\n)?((?:${COMMENT}[ \t]*\n)*${COMMENT})`, "gm");
const SINCE_LINE = /^<!--\s*since:\s*(?:([a-z][\w-]*)\s+)?(\d[^\s>]*?)\s*-->$/;

/**
 * @param {string} block stacked `since` comments, one per line
 * @returns {Record<string, string>} constraints `{package: version}`; throws on malformed lines or repeated packages
 */
function parseBlock(block, where = "since directive") {
  const constraints = {};
  for (const line of block.split("\n")) {
    const m = SINCE_LINE.exec(line.trim());
    if (!m) throw new Error(`${where}: cannot parse "${line.trim()}", expected <!-- since: [package ]1.32.0 -->`);
    const pkg = m[1] || DEFAULT_PACKAGE;
    if (pkg in constraints) throw new Error(`${where}: package "${pkg}" is given twice`);
    constraints[pkg] = m[2];
  }
  return constraints;
}

function preprocess({ fileContent }) {
  return fileContent.replace(SINCE_DIRECTIVE, (_, heading, id, block) => {
    const since = formatConstraints(parseBlock(block));
    return `${heading}<Since since="${since}" />${id ? ` ${id}` : ""}`;
  });
}

module.exports = { SINCE_DIRECTIVE, parseBlock, preprocess };
