# Website

The website is a Node.js application.

The documentation is generated using [Docusaurus 2](https://docusaurus.io/), a modern static website generator.
Docusaurus consumes content from the `./docs` folder (at `./docs/website/docs` in this repo). The content includes:

- markdown files
- code snippets
- API documentation, which `docs/tools/generate_api_reference.py` generates into `./docs_processed/api_reference` when the Node package is run.

On the production website the documentation appears at https://dlthub.com/docs and the default documentation page is https://dlthub.com/docs/intro.

## Installation

With `website` as your working directory:

```
$ npm install
```

That command installs our Node.js package defined in `package.json`.

### Python Dependencies

The website build process requires Python dependencies. From the `docs/` directory run:

```
$ make dev
```

This calls `uv sync` and installs all Python tooling into the docs virtual environment. It also installs `prek` git hooks (`pre-commit` and `pre-push`), so linting/formatting checks may run automatically on commit/push.

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

You edit the sources in `docs/`, but Docusaurus renders `docs_processed/`, which the Python preprocessor generates from them. The `preprocess-docs` Docusaurus plugin (`plugins/preprocess-docs.js`) watches `docs/` and re-runs the preprocessor on change, so there is a single file watcher and saving a source file refreshes the browser as usual.
You may get a "Page Not Found" error when browsing at `/docs/`. This does not happen on the production website, whose default page is the "Ïntroduction" page at `/docs/intro`.

For most authoring purposes, once you are happy with your changes running locally, you can create a Github PR, without needing to do the following build and deployment steps.

## Local Build

```
$ npm run build
```

That command delegates to `cd .. && make build` (see `package.json`), which drives the full pipeline from `docs/Makefile`. It generates static content into the `build` directory, which can be served using `npm run serve`.

### What `npm run build` does

The full build (the `build` target in `docs/Makefile`) runs these steps in order:

1. **`npm run compile-redirects`** — compiles `redirects.js` into `redirects.compiled.js`. See [Redirects](#redirects) below.
2. **`make check-orphan-docs`** (from `docs/`) — runs `tools/check_orphan_docs.js`, which fails the build if any page under `docs/` is unreachable from `sidebars.js` (directly, via a category `link`, or as a sidebar item) unless it's marked `unlisted: true` in frontmatter.
3. **`make preprocess-docs`** (from `docs/`) — Python preprocessor (`docs/tools/preprocess_docs.py`): copies `docs/` to `docs_processed/`, expands `@@@DLT_*` markers, generates the API reference, etc.
4. **`docusaurus build --out-dir build/docs`** — the static site build itself. Fails on broken internal markdown links.
5. **`npm run verify-redirects`** — loads `redirects.compiled.js`, recompiles it in memory to check it isn't stale, and checks each `to` target resolves to an existing page in `build/docs`.
6. **`node scripts/verify-llms-txt.js`** — checks the generated `llms.txt` index against the sidebar.

CI (`.github/workflows/docs_build.yml`) simply runs `make build` from `docs/`, so it exercises the exact same 7 steps — there is no separate "verify redirects" CI step.

### Running individual checks

You can re-run these checks standalone — useful when iterating on one concern without rebuilding everything:

```
$ npm run check-orphan-docs      # orphan-page check (fails on non-unlisted orphans)
$ npm run check-orphan-docs:all  # same, but also fails on unlisted orphans (lists everything)
$ npm run compile-redirects      # regenerate redirects.compiled.js from sources
$ npm run verify-llms            # llms.txt index check
$ npm run verify-redirects       # redirect targets check (requires a prior `npm run build`)
```

Run `verify-redirects` (or a full `npm run build`) whenever you add or change entries in `redirects.js`. See [Redirects](#redirects) below for the source-of-truth and how to add new entries.

## Deployment

The site is deployed using `cloudflare workers`. There are several commands specific to cloudflare to deploy the docs or test them locally. 

```
npm run preview:cloudflare
```

This will build the project fully and serve via a local wrangler webserver which simulates a cloudflare worker. This way you can also test tracking and redirects. 

## Redirects

The docs site is built from the `devel` branch and served at `/docs/`. The redirect rules in `redirects.js` are compiled into a list that the Cloudflare worker and the verifier both consume.

### How to add or change a redirect

Edit [`redirects.js`](redirects.js). Write entries with full `/docs/...` paths:

```js
{ from: "/docs/old/path",   to: "/docs/new/path" },
```

A `from` path may only appear **once**. The worker resolves paths through a map where the first rule wins, so a second rule for the same `from` would never fire; `compile-redirects` fails the build on duplicates. To change where a path goes, edit the existing entry.

The legacy `/docs/*` URLs are forwarded to `/docs/*` by the worker (see `worker-routing.mjs`), so do not add rules for them.

### Compile step

`tools/compile_redirects.js` reads `./redirects.js`, checks it for duplicate `from` paths and writes the list to `redirects.compiled.js`. That file is a **build artifact** — gitignored and regenerated on every build.

### Where the compiled file is used

- **Cloudflare worker (`worker.ts`)** — imports `redirects.compiled.js`. Wrangler bundles the import into the deployed worker. Exact pathname match per entry, and `/docs/devel/*` is forwarded to `/docs/*`. Rules, trailing slashes and the legacy `/plus/` → `/hub/` forward are followed to the final target (see `worker-routing.mjs`), so a moved page answers with a single `301` instead of a chain. A chain that does not settle within 10 hops is left to the assets binding rather than redirected to a half-resolved path.
- **`scripts/verify-redirects.js`** — loads `redirects.compiled.js`, checks each `to` resolves to an HTML page in `build/docs/<rel>`, and **also recompiles in memory** from `redirects.js` and diffs the result against the on-disk compiled file to guard against a stale compiled file.


## LLM-friendly documentation

The docs build generates LLM-optimized output following the [llms.txt specification](https://llmstxt.org/). This is implemented by a custom Docusaurus plugin at `plugins/llms-txt.js` that runs as a `postBuild` hook.

### What the plugin produces

- **`/docs/llms.txt`** — Index of all doc pages (excluding hub) with titles and descriptions, grouped by sidebar category (e.g. "Getting started", "Sources > REST APIs"). This is the primary entry point for LLM agents.
- **`/docs/hub/llms.txt`** — Separate index for dltHub pages, grouped by `hubSidebar` categories. Configured via `separateIndexes` in the plugin options.
- **`.md` files next to each HTML page** — For every doc page like `/docs/general-usage/schema`, a clean markdown version is available at `/docs/general-usage/schema.md`. These are copied from the preprocessed source files (with snippets already resolved), not reverse-converted from HTML.

### How it works

1. **Discovers pages** from the HTML build output (all `*.html` files).
2. **Maps each HTML path** back to its source `.md` file in `docs_processed/`, handling custom `slug:` frontmatter.
3. **Copies source `.md` files** with cleanup: strips MDX `import` lines and self-closing React component tags (`<Header/>`, `<DocCardList/>`, etc.) that are UI-only widgets.
4. **Generates `llms.txt`** from the doc pages, reading `title` and `description` from YAML frontmatter. Pages are grouped by sidebar categories from `sidebars.js` (up to `groupDepth` levels, joined with " > "); pages not in any sidebar fall back to directory-based grouping. Pages matching a `separateIndexes` prefix (e.g. `hub/`) are split into their own `llms.txt` at that prefix path and removed from the main index.

### What gets excluded or separated

- **`api_reference/`** pages — auto-generated, no source `.md` files. Excluded from both `.md` generation and `llms.txt`.
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
  - **API reference** — if `docs_processed/api_reference/sidebar.json` exists (generated by `make generate-api-ref`), it is spliced into the "Reference" category.

- **`hubSidebar`** — A secondary sidebar for dltHub-specific pages (under `docs/hub/`). It cross-references open-source docs using `{ type: 'ref', id: '...' }` items, so users can navigate between the two sidebars seamlessly.

Hub pages receive special treatment in swizzled theme components:

- **`src/theme/DocBreadcrumbs`** — When the current URL contains `/hub/`, a dltHub logo is rendered next to the breadcrumb trail (via the `breadcrumbsContainerPlus` CSS class and an `<img>` tag).
- **`src/components/DltHubFeatureAdmonition.js`** — A reusable admonition component imported by hub pages to display licensing/feature notices.

In production, the Cloudflare worker (`worker.ts`) redirects the legacy `/plus/` URL prefix to `/hub/`.

## Page overlays (Root.js)

`src/theme/Root.js` wraps the entire Docusaurus app to inject page-specific modal overlays, e.g. a floating button that opens a Loom video walkthrough for the page. The overlay configuration is a simple object mapping URL paths to `{buttonTitle, title, loomId, text}` (currently empty).

## Version badges

Pages and sections can be tagged with the package release that introduced them. At build time the
tag is compared with the latest *released* version of that package:

| tag vs. released version | badge |
| --- | --- |
| `status: preview`, or `since` newer than released | **Preview** |
| `since` released in the last 3 minors (the release itself counts) | **New in X** |
| older | none |

Badges are shown on the sidebar entry, under the page breadcrumbs and next to a section heading.
Versions are PEP 440 (`1.32.0`, `1.32.0rc1`); pre-releases sort before the final release.
Badges are package-scoped: `dlt` (default), `dlthub`, `dlthub-client`.
`dlthub_sdk` ships inside `dlthub-client` and shares its version, so tag SDK docs with `dlthub-client`.

### dltHub libraries and stacked constraints

A tag is a set of constraints `{package: first release that has the feature}`. A dltHub feature
typically names its own library, e.g. `dlthub-client 0.4.0`. That release declares a minimum `dlt`
(`Requires-Dist: dlt>=1.30.0` on PyPI), so you do not repeat it: the resolver reads it from the
released package and adds it as an *implied* lower bound.

- A feature is **Preview** until every constraint, explicit or implied, is released.
- **New in X** lasts while any *explicit* constraint is within the last 3 minors. Implied bounds do
  not extend it. The label lists the explicit constraints (`New in dlthub-client 0.4.0`; `dlt`
  alone is shown as just the version).
- The tooltip shows the implied bound (`requires dlt >= 1.30.0`). It only exists for a package
  release that is already on PyPI; for an unreleased one add the `dlt` constraint yourself.
- Requirements behind an extra or an environment marker are ignored.

Stack constraints in frontmatter with a dictionary (do not combine it with `package`):

```md
---
title: My dltHub feature
since:
  dlthub-client: "0.4.0"
  dlt: "1.32.0"
---
```

and in a section with one comment per package, one after another:

```md
## My dltHub section
<!-- since: dlthub-client 0.4.0 -->
<!-- since: dlt 1.32.0 -->
```

A package can appear only once per tag. `since: "0.4.0"` with `package: dlthub-client` is the
one-constraint shorthand of `since: {dlthub-client: "0.4.0"}`.

Tag a page in its frontmatter (quote `since`, YAML would read `1.30` as a number):

```md
---
title: My feature
since: "1.32.0"        # or: status: preview
package: dlt           # optional, default dlt
---
```

Tag a section with an HTML comment directly below the heading. It is invisible in plain markdown,
no MDX needed:

```md
## My new section
<!-- since: 1.32.0 -->
<!-- or, for another package: since: dlthub 0.5.0 -->
```

Use a release that is not out yet. `node versioning/check.js <changed files>` fails if a page or
section claims a `since` that is already released (with stacked constraints: all of them are released).

### Released versions: PyPI by default, mockable locally

The released versions come from PyPI. Set `DLT_DOCS_RELEASED_VERSIONS` to override them, either inline
or as a path to a JSON file (`{"dlt": "1.32.0"}`). Only packages used by pages need an entry.
Requirements that a released package version puts on other packages are read from PyPI, unless
`DLT_DOCS_RELEASED_VERSIONS` is set: then none are assumed, mock them with `DLT_DOCS_REQUIRES`
(inline `dlthub-client@0.4.0=dlt>=1.30.0; ...` or a JSON file `{"dlthub-client@0.4.0": {"dlt": "1.30.0"}}`).
`DLT_DOCS_DEMO=1` additionally serves demo pages from `versioning/demo/` at
`/docs/versioning-demo` (they are never part of the regular docs build).

```sh
# from docs/website; swap in any mocked version and look at /docs/versioning-demo
DLT_DOCS_DEMO=1 DLT_DOCS_RELEASED_VERSIONS="dlt=1.31.0,dlthub=0.5.0,dlthub-client=0.4.0" npm start
```

Expected demo badges (`dlthub-page` follows `dlthub`; `preview-explicit` is always Preview):

| mocked `dlt` | `upcoming` (1.32.0) | `recent` (1.30.0) | `older` (1.28.0) | `expired` (1.20.0) |
| --- | --- | --- | --- | --- |
| `1.29.0` | Preview | Preview | New | none |
| `1.31.0` | Preview | New | none | none |
| `1.32.0` | New | New | none | none |
| `1.34.0` | New | none | none | none |
| `1.35.0` | none | none | none | none |

The same states apply to the matching sections on the `sections` demo page. The `stacked` page
needs `dlthub-client` 0.4.0 and `dlt` 1.32.0: Preview until both are released, and with
`DLT_DOCS_REQUIRES="dlthub-client@0.4.0=dlt>=1.34.0"` it also waits for `dlt` 1.34.0. Unit tests for the
comparison logic: `npm run test:versioning`.
