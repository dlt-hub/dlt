/**
 * Serves an existing Pagefind search index from the dev server (`docusaurus start`)
 * so the search UI works locally. It does not build the index. `docusaurus build`
 * ignores this plugin.
 *
 * Pagefind indexes rendered HTML, so the index only exists after a production build
 * followed by `npm run search-index`, which writes `build/docs/pagefind`. Run
 * `make search-index` (in `docs/`) to do both; `make build` does too. This plugin serves
 * that folder at `<baseUrl>pagefind/`. The index is a snapshot of the last build and
 * does not pick up live edits (re-run `make search-index` to refresh).
 *
 * Only pages of the Docusaurus "last version" are indexed (see
 * `src/theme/DocItem/Layout`), so the docs version you search depends on local state:
 * - `versions.json` exists (written by `npm run update-versions`, also run by
 *   `make build`): the `master` snapshot, i.e. the latest release served at `/docs/`.
 *   This matches production, but it reflects master as of the last `update-versions`
 *   run, not your branch.
 * - no `versions.json`: the current branch, served under `/docs/devel/`.
 */
const path = require("node:path");

/**
 * @param {import('@docusaurus/types').LoadContext} context
 * @returns {import('@docusaurus/types').Plugin}
 */
module.exports = function pagefindDevPlugin(context) {
  return {
    name: "pagefind-dev",

    configureWebpack() {
      // devServer is only used by `docusaurus start`, ignored by `build`
      return {
        devServer: {
          static: [
            {
              publicPath: `${context.baseUrl}pagefind`,
              directory: path.join(context.siteDir, "build/docs/pagefind"),
              watch: false,
            },
          ],
        },
      };
    },
  };
};
