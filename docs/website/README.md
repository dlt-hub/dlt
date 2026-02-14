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

That command generates static content into the `build` directory, which can be served using any static contents hosting service, for example, `npm run serve`

## Deployment

The site is deployed using `cloudflare workers`. There are several commands specific to cloudflare to deploy the docs or test them locally. 

```
npm run preview:cloudflare
```

This will build the project fully and serve via a local wrangler webserver which simulates a cloudflare worker. This way you can also test tracking and redirects. 

## Redirects

Simple redirects are managed with the cloudflare worker in `worker.js`. 

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

- **`/docs/llms.txt`** — Index of all master-version doc pages (excluding hub) with titles and descriptions, grouped by directory. This is the primary entry point for LLM agents.
- **`/docs/hub/llms.txt`** — Separate index for dltHub pages, with its own title and description. Configured via `separateIndexes` in the plugin options.
- **`.md` files next to each HTML page** — For every doc page like `/docs/general-usage/schema`, a clean markdown version is available at `/docs/general-usage/schema.md`. These are copied from the preprocessed source files (with snippets already resolved), not reverse-converted from HTML.

### How it works

1. **Discovers pages** from the HTML build output (all `*.html` files).
2. **Maps each HTML path** back to its source `.md` file in `versioned_docs/version-{name}/` or `docs_processed/`, handling custom `slug:` frontmatter.
3. **Copies source `.md` files** with cleanup: strips MDX `import` lines and self-closing React component tags (`<Header/>`, `<DocCardList/>`, etc.) that are UI-only widgets.
4. **Generates `llms.txt`** from the master version pages, reading `title` and `description` from YAML frontmatter. Pages matching a `separateIndexes` prefix (e.g. `hub/`) are split into their own `llms.txt` at that prefix path and removed from the main index.

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

In production, the Cloudflare worker (`worker.js`) redirects the legacy `/plus/` URL prefix to `/hub/`.

## Page overlays (Root.js)

`src/theme/Root.js` wraps the entire Docusaurus app to inject page-specific modal overlays. Currently it adds a floating button on the `/docs/walkthroughs/create-a-pipeline` page that opens a Loom video walkthrough for creating a pipeline with GPT-4. The overlay configuration is a simple object mapping URL paths to `{buttonTitle, title, loomId, text}`.