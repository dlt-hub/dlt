# Website

This website is built using [Docusaurus 2](https://docusaurus.io/), a modern static website generator. The actual content resides in the `docs` folder.


### Installation

```
$ npm install
```

### Site Configuration

The site is configured to run under the `/docs` path. The `build` command is properly configured.

### Local Development

```
$ npm run start
```

This command starts a local development server and opens up a browser window. Most changes are reflected live without having to restart the server.

### Local Build

```
$ npm run start
```

This command generates static content into the `build` directory and can be served using any static contents hosting service ie. `npm run serve`


### Deployment

The site is deployed using `netlify`. The `netlify` build command is as follows:
```
npm run build:netlify
```

It will place the build in `build/docs` folder. The `netlify.toml` redirects from root path `/` into `/docs`.