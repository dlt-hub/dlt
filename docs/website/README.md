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

The website build process requires Python dependencies, including `pydoc-markdown` for generating API documentation. Install them with:

```
$ uv pip install -r requirements.txt
```

Or if you're using the project's `uv` environment from the root directory:

```
$ uv sync --group docs
```

### Are you new to Node?

`npm` is a package manager bundled with Node.js. If `npm install` complained that you have an old version, try:

```
nvm install --lts
```

That command installs and uses the latest stable version of Node.js (and therefore `npm`).  Then retry the Installation steps above.

`nvm` is the Node Version Manager, and yes, you may need to install that first, using your usual way of installing software on your OS.

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

This will build the project fully and serve via a local wrangler webserver which simulates a cloudflare worker. This way you can also test tracking and redirects. Please be aware that cloudflare preview and build commands expect certain python depdendencies, so you need to be inside the uv shell or run the command with uv run: `uv run npm run preview:cloudflare` for it to work locally.

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

The netflify deployment of these docs need to happen from the master branch so that the current version gets properly selected.