/**
 * Resolves page version badges at build time and exposes them to the theme as global data:
 * `{released, requires, windowMinors, badges: {docId: badge}}`, read via `usePluginData("versioning")`.
 *
 * Options: `docDirs` - absolute dirs with markdown to scan for `since`/`status`/`package` frontmatter.
 * Released versions come from `versioning/released.js` (PyPI, or the DLT_DOCS_RELEASED_VERSIONS override).
 */
const { WINDOW_MINORS, getBadge } = require("../versioning/badge");
const { resolveContext } = require("../versioning/released");
const { scanDocs } = require("../versioning/scan");

/** @returns {import('@docusaurus/types').Plugin} */
module.exports = function versioningPlugin(_context, { docDirs }) {
  return {
    name: "versioning",

    getPathsToWatch() {
      return docDirs.map((dir) => `${dir}/**/*.{md,mdx}`);
    },

    async loadContent() {
      const scans = docDirs.map((dir) => scanDocs(dir));
      const metas = Object.assign({}, ...scans.map((scan) => scan.pages));
      // only packages that pages or sections use are resolved, so an offline override can list just those
      const { released, requires } = await resolveContext(scans.flatMap((scan) => scan.constraints));
      const headings = Object.assign({}, ...scans.map((scan) => scan.headings));
      const badges = {};
      // sections that carry a badge, per doc id and heading anchor: {docId: {anchor: badge}}
      const sections = {};
      for (const [id, anchors] of Object.entries(headings)) {
        for (const [anchor, since] of Object.entries(anchors)) {
          const badge = getBadge({ since }, released, { requires });
          if (!badge) continue;
          sections[id] = sections[id] || {};
          sections[id][anchor] = badge;
        }
      }
      // pages without a page badge but with badged sections are "updated", for the sidebar
      for (const [id, meta] of Object.entries(metas)) {
        const badge = getBadge(meta, released, { requires });
        if (badge) badges[id] = badge;
      }
      const updated = {};
      for (const id of Object.keys(sections)) {
        if (!badges[id]) updated[id] = { kind: "updated", label: "Updated", title: "has new or preview sections" };
      }
      return { released, requires, windowMinors: WINDOW_MINORS, badges, sections, updated };
    },

    async contentLoaded({ content, actions }) {
      actions.setGlobalData(content);
    },
  };
};
