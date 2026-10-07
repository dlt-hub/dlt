/**
 * Runs the python docs preprocessor (`docs/tools/preprocess_docs.py`) as part
 * of the docusaurus lifecycle.
 *
 * The preprocessor turns the committed sources in `website/docs` into the
 * pages docusaurus actually renders in `website/docs_processed`, expanding
 * `@@@DLT_*` markers on the way.
 *
 * This plugin's only job is live-reload during development: `docusaurus start`
 * watches `website/docs` for us and calls `loadContent()` on change, which
 * re-runs the preprocessor incrementally. The preprocessor only rewrites files
 * whose content changed, so the docs plugin (which watches `docs_processed`)
 * then reloads just the affected pages.
 *
 * The initial `docs_processed` (markers + API reference) is built by
 * `make process-docs`, which `make start`/`make build` run before docusaurus
 * boots (see `docs/Makefile` and `website/package.json`). This watcher never
 * regenerates the API reference; it only re-expands markers on source edits.
 */
const { spawnSync } = require("node:child_process");
const path = require("node:path");

/**
 * @param {import('@docusaurus/types').LoadContext} context
 * @returns {import('@docusaurus/types').Plugin}
 */
module.exports = function preprocessDocsPlugin(context) {
  // `docs/`, the working dir from which the preprocessor script is run
  const docsRoot = path.resolve(context.siteDir, "..");
  const sourceDir = path.join(context.siteDir, "docs");
  let isFirstRun = true;

  return {
    name: "preprocess-docs",

    getPathsToWatch() {
      return [`${sourceDir}/**/*`];
    },

    async loadContent() {
      // the initial docs_processed is built by `make process-docs` before
      // docusaurus boots, so the first load has nothing to do
      if (isFirstRun) {
        isFirstRun = false;
        return;
      }

      const args = ["run", "--script", "tools/preprocess_docs.py", "--incremental"];
      const result = spawnSync("uv", args, {
        cwd: docsRoot,
        stdio: "inherit",
      });

      if (result.error) {
        throw result.error;
      }
      if (result.status !== 0) {
        throw new Error(`preprocess_docs.py exited with code ${result.status}`);
      }
    },
  };
};
