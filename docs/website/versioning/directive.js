/**
 * The section directive: an HTML comment right below a heading, invisible in plain markdown.
 *   ## Heading
 *   <!-- since: 1.32.0 -->            or    <!-- since: dlthub 0.5.0 -->
 * Docusaurus drops comments while parsing, so no remark plugin can see them. `preprocess` runs
 * on the raw source instead (docusaurus `markdown.preprocessor`) and moves the directive into
 * the heading as a `<Since>` component, which renders the badge.
 */
const SINCE_DIRECTIVE = /^(#{1,6} .*?)[ \t]*(\{#[\w-]+\})?[ \t]*\n<!--\s*since:\s*(?:([a-z][\w-]*)\s+)?(\d[^\s>]*)\s*-->/gm;

/** `matchAll` groups: heading, explicit id, package (optional), version */
function preprocess({ fileContent }) {
  return fileContent.replace(SINCE_DIRECTIVE, (_, heading, id, pkg, version) => {
    const pkgAttr = pkg ? ` package="${pkg}"` : "";
    return `${heading}<Since version="${version}"${pkgAttr} />${id ? ` ${id}` : ""}`;
  });
}

module.exports = { SINCE_DIRECTIVE, preprocess };
