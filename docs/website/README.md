# Website

The website is a Node.js application.

The documentation is generated using [Docusaurus 2](https://docusaurus.io/), a modern static website generator.
Docusaurus consumes content from the `./docs` folder (at `./docs/website/docs` in this repo). The content includes:

- markdown files
- code snippets
- API documentation, which pydoc generates into `./docs/api_reference` when the Node package is run.

On the production website the documentation appears at https://dlthub.com/docs and the default documentation page is https://dlthub.com/docs/intro.

## Installation

With `website` as your working directory:

```
$ npm install
```

That command installs our Node.js package defined in `package.json`.

### Python Dependencies

The website build process requires Python dependencies, including `pydoc-markdown` for generating API documentation. From the `docs/` directory run:

```
$ make dev
```

This calls `uv sync` and installs all Python tooling into the docs virtual environment.

### Are you new to Node?

`npm` is a package manager bundled with Node.js. If `npm install` complained that you have an old version, try:

```
nvm install --lts
```

That command installs and uses the latest stable version of Node.js (and therefore `npm`).  Then retry the Installation steps above.

`nvm` is the Node Version Manager, and yes, you may need to install that first, using your usual way of installing software on your OS.

You will also need the uv python package manager (https://docs.astral.sh/uv/guides/install-python/), as some of the npm commands call python based pre-preprocessing scripts to prepare the markdown files for docusaurus. We are mixing python and javascript tools in this project.

## Local Development

In this mode, most of your authoring changes will be reflected live in the browser just by saving files, without having to restart the server. Type:

```
$ npm run start
```

That command starts a local development web server and opens a browser window. It then takes a few seconds for Docusaurus to generate pages before the website displays.
You may get a "Page Not Found" error when browsing at `/docs/`. This does not happen on the production website, whose default page is the "Ïntroduction" page at `/docs/intro`.

For most authoring purposes, once you are happy with your changes running locally, you can create a Github PR, without needing to do the following build and deployment steps.

## Local Build

```
$ npm run build
```

That command generates static content into the `build` directory, which can be served using `npm run serve`.

### What `npm run build` does

The full build runs these steps in order:

1. **`npm run update-versions`** — clones `dlt`, checks out the `master` branch, freezes the content into `versioned_docs/version-master/`. This is the **master snapshot** (served at `/docs/`); your branch is served at `/docs/devel/`. See [Docs versions](#docs-versions) below.
2. **`make preprocess-docs`** (from `docs/`) — Python preprocessor: expands `<!--@@@DLT_SNIPPET-->` markers, generates the API reference, etc.
3. **`docusaurus build --out-dir build/docs`** — the static site build itself. Fails on broken internal markdown links.
4. **`node scripts/verify-llms-txt.js`** — checks the generated `llms.txt` index against the sidebar.

### Running individual checks

After a full build, you can re-run the verifiers standalone — useful when iterating on one concern without rebuilding everything:

```
$ npm run verify-llms          # llms.txt index check
$ npm run verify-redirects     # redirect targets check
```

**`verify-redirects` is no longer part of `npm run build` — CI runs it as a dedicated `Verify redirects` step in `.github/workflows/build_docs.yml`.** To reproduce CI locally:

```
$ npm run build
$ npm run verify-redirects
```

Run this whenever you add or change entries in `redirects.js`. See [Redirects](#redirects) below for the source-of-truth and how to add new entries.

## Deployment

The site is deployed using `cloudflare workers`. There are several commands specific to cloudflare to deploy the docs or test them locally. 

```
npm run preview:cloudflare
```

This will build the project fully and serve via a local wrangler webserver which simulates a cloudflare worker. This way you can also test tracking and redirects. 

## Redirects

The docs site builds two snapshots: `master` (frozen at the last `dlt` release, mounted at `/docs/`) and `devel` (your current branch, mounted at `/docs/devel/`). Each version owns its own redirect rules, scoped to that version's URL space. A build-time step merges them into a single, version-prefixed list that the Cloudflare worker and the verifier both consume.

### How to add or change a redirect

Edit [`redirects.js`](redirects.js) — that file is the **devel** version's redirect source. Write entries with bare `/docs/...` paths, **without any `/devel/` prefix**:

```js
{ from: "/docs/old/path",   to: "/docs/new/path" },
```

The compile step (see below) prefixes both fields with `/docs/devel/` when emitting the version-scoped output. So the entry above ends up matching the URL `/docs/devel/old/path` in production — exactly where the page lives in this version.

Pre-snapshotted versions (currently just `master`) have their own redirect files under `versioned_redirects/version-<v>.js`. Those are populated by `tools/update_versions.js`, which clones each tag and snapshots its `docs/website/redirects.js`. You don't edit those files by hand — to change `master`'s redirects, edit `redirects.js` on the `master` branch and push.

The lone catchall `{ from: "/", to: "/docs/devel/intro" }` is the exception: its `from` doesn't start with `/docs/`, so the compiler treats it as version-independent and passes it through unchanged.

### Compile step

`tools/compile_redirects.js` reads:

1. `./redirects.js` (the devel source).
2. Every `./versioned_redirects/version-*.js` (snapshotted by `update_versions.js`).

It rewrites each entry's leading `/docs/` to `/docs/<version-path>/` (devel → `/docs/devel/`, master → `/docs/`, any future tag → `/docs/<tag>/`) and writes the merged list to `redirects.compiled.js`. That file is a **build artifact** — gitignored and regenerated on every build.

### Where the compiled file is used

- **Cloudflare worker (`worker.ts`)** — imports `redirects.compiled.js`. Wrangler bundles the import into the deployed worker. Exact pathname match per entry; no version-routing logic in the worker.
- **`scripts/verify-redirects.js`** — loads `redirects.compiled.js`, checks each `to` resolves to an HTML page in `build/docs/<rel>`, and **also recompiles in memory** from the per-version sources and diffs the result against the on-disk compiled file to guard against a stale compiled file.


### Lifecycle: what happens after merge to `master`

The build runs the same way on every branch: `update_versions.js` always clones `origin/master` and snapshots its `redirects.js` into `versioned_redirects/version-master.js`. The local `./redirects.js` is always treated as the `current` (devel-prefixed) version.

When a devel→master merge ships the next release, the only thing that changes is what `origin/master` points to. The next build on master snapshots the just-merged `redirects.js` as the master version's source, so the same authored rules that previously compiled with the `/devel/` prefix now also compile with **no** prefix — they catch the bare `/docs/...` URLs on the freshly released master.

## Docs versions

We keep a few additional versions of our docs for the users to be able read about how former and future versions of dlt work. We use docusaurus versions for this but we do not check the historical versions into the repo but rather use a script to build the former versions on deployment. To locally build the versions run:

```
npm run update-versions
```

This will execute the script at tools/update_versions.js. This tool will do the following:

* Find all the highest minor versions the tags of the repo (e.g. 0.4.13, 0.5.22, 1.1.3)
* It will create a version for all of these tags that are larger than the minimum version defined in MINIMUM_SEMVER_VERSION in the script.
* It will NOT create a version for the highest version, we assume the most up to date docs for the highest versions are the tip of master
* It will NOT create any docs versions for pre-releases.
* It will create a future version called "devel" from the current commit of this repo.
* It will set up docusaurus to display all of these versions correctly.

You can clear these versions with

```
npm run clear-versions
```

The cloudflare deployment of these docs needs to happen from the master branch so that the current version gets properly selected.

## LLM-friendly documentation

The docs build generates LLM-optimized output following the [llms.txt specification](https://llmstxt.org/). This is implemented by a custom Docusaurus plugin at `plugins/llms-txt.js` that runs as a `postBuild` hook.

### What the plugin produces

- **`/docs/llms.txt`** — Index of all master-version doc pages (excluding hub) with titles and descriptions, grouped by sidebar category (e.g. "Getting started", "Sources > REST APIs"). This is the primary entry point for LLM agents.
- **`/docs/hub/llms.txt`** — Separate index for dltHub pages, grouped by `hubSidebar` categories. Configured via `separateIndexes` in the plugin options.
- **`.md` files next to each HTML page** — For every doc page like `/docs/general-usage/schema`, a clean markdown version is available at `/docs/general-usage/schema.md`. These are copied from the preprocessed source files (with snippets already resolved), not reverse-converted from HTML.

### How it works

1. **Discovers pages** from the HTML build output (all `*.html` files).
2. **Maps each HTML path** back to its source `.md` file in `versioned_docs/version-{name}/` or `docs_processed/`, handling custom `slug:` frontmatter.
3. **Copies source `.md` files** with cleanup: strips MDX `import` lines and self-closing React component tags (`<Header/>`, `<DocCardList/>`, etc.) that are UI-only widgets.
4. **Generates `llms.txt`** from the master version pages, reading `title` and `description` from YAML frontmatter. Pages are grouped by sidebar categories from `sidebars.js` (up to `groupDepth` levels, joined with " > "); pages not in any sidebar fall back to directory-based grouping. Pages matching a `separateIndexes` prefix (e.g. `hub/`) are split into their own `llms.txt` at that prefix path and removed from the main index.

### What gets excluded or separated

- **`api_reference/`** pages — auto-generated, no source `.md` files. Excluded from both `.md` generation and `llms.txt`.
- **`devel/`** pages — get `.md` files (so "View Markdown" links work) but are excluded from `llms.txt`.
- **`hub/`** pages — get `.md` files and their own `/docs/hub/llms.txt` index (via `separateIndexes`), but are excluded from the main `/docs/llms.txt`.
- **Underscore-prefixed files** (`_source-info-header.md`, etc.) — MDX partials imported by other pages, not standalone content.

### Theme components

Two swizzled Docusaurus theme components support the "View Markdown" badge:

- **`src/theme/DocMarkdownLink`** — Renders the "View Markdown" badge and injects a `<link rel="alternate" type="text/markdown">` tag into the page head. Used on regular doc pages only.
- **`src/theme/DocItem/Layout`** — Swizzled to include `DocMarkdownLink` next to `DocVersionBadge`.


## Hub pages and sidebars

The site has two sidebars defined in `sidebars.js`:

- **`docsSidebar`** — The primary sidebar for all open-source dlt documentation. Items are manually curated and do not mirror the file-system layout (e.g., "Core concepts" pulls docs from `reference/`, `general-usage/`, etc.). Two sections are injected dynamically at build time:
  - **Code examples** — all `.md`/`.mdx` files under `docs_processed/examples/` are auto-appended.
  - **API reference** — if `docs_processed/api_reference/sidebar.json` exists (generated by pydoc), it is spliced into the "Reference" category.

- **`hubSidebar`** — A secondary sidebar for dltHub-specific pages (under `docs/hub/`). It cross-references open-source docs using `{ type: 'ref', id: '...' }` items, so users can navigate between the two sidebars seamlessly.

Hub pages receive special treatment in swizzled theme components:

- **`src/theme/DocBreadcrumbs`** — When the current URL contains `/hub/`, a dltHub logo is rendered next to the breadcrumb trail (via the `breadcrumbsContainerPlus` CSS class and an `<img>` tag).
- **`src/components/DltHubFeatureAdmonition.js`** — A reusable admonition component imported by hub pages to display licensing/feature notices.

In production, the Cloudflare worker (`worker.ts`) redirects the legacy `/plus/` URL prefix to `/hub/`.

## Page overlays (Root.js)

`src/theme/Root.js` wraps the entire Docusaurus app to inject page-specific modal overlays, e.g. a floating button that opens a Loom video walkthrough for the page. The overlay configuration is a simple object mapping URL paths to `{buttonTitle, title, loomId, text}` (currently empty).