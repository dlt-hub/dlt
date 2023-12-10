# Website

The website is a Node.js application.

The documentation is generated using [Docusaurus 2](https://docusaurus.io/), a modern static website generator. 
Docusaurus consumes content from the `./docs` folder (at `./docs/website/docs` in this repo). The content includes:

- markdown files
- code snippets
- API documentation.

On the production website the documentation appears at https://dlthub.com/docs and the default documentation page is https://dlthub.com/docs/intro.

Docusauraus also consumes blog posts (from `./blog`) and they appear at https://dlthub.com/docs/blog.

## Installation

With `website` as your working directory:

```
$ npm install
```

That command installs our Node.js package defined in `package.json`.

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

The site is deployed using `netlify`. The `netlify` build command is:

```
npm run build:netlify
```

It will place the build in `build/docs` folder. The `netlify.toml` redirects from root path `/` into `/docs`.