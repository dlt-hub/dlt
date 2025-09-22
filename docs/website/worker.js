// cloudflare worker implementation to serve the website docs
import { instrument } from '@microlabs/otel-cf-workers';

const REDIRECTS = [
    // basic root redirects
    {
        from: "/",
        to: "/docs/intro/"
    },
    {
        from: "/docs",
        to: "/docs/intro/"
    },
    {
        from: "/docs/",
        to: "/docs/intro/"
    },
    {
        from: "/docs/intro",
        to: "/docs/intro/"
    },

    // docs section redirects
    {
        from: "/docs/getting-started",
        to: "/docs/intro/"
    },
    {
        from: "/docs/dlt-ecosystem",
        to: "/docs/dlt-ecosystem/verified-sources/"
    },
    {
        from: "/docs/general-usage/credentials/config_providers",
        to: "/docs/general-usage/credentials/setup/"
    },
    {
        from: "/docs/general-usage/credentials/configuration",
        to: "/docs/general-usage/credentials/setup/"
    },
    {
        from: "/docs/general-usage/credentials/config_specs",
        to: "/docs/general-usage/credentials/complex_types/"
    },

    // tutorial redirects
    {
        from: "/docs/tutorial/intro",
        to: "/docs/tutorial/load-data-from-an-api/"
    },
    {
        from: "/docs/tutorial/grouping-resources",
        to: "/docs/tutorial/load-data-from-an-api/"
    },

    // reference + misc redirects
    {
        from: "/docs/telemetry",
        to: "/docs/reference/telemetry/"
    },
    {
        from: "/docs/walkthroughs",
        to: "/docs/intro/"
    },
    {
        from: "/docs/visualizations",
        to: "/docs/general-usage/dataset-access/"
    },
]

const ROUTE_404 = "/docs/404";

const handler = {
    async fetch(request, env, ctx) {

        const url = new URL(request.url);

        // handle redirects
        for (const redirect of REDIRECTS) {
            if (url.pathname === redirect.from) {
                url.pathname = redirect.to;
                return Response.redirect(url.toString(), redirect.code || 301);
            }
        }   

        // normalize urls prefixed with /docs, only needed locally
        let res = null;
        if (url.pathname.startsWith("/docs/")) {
            url.pathname = url.pathname.replace(/^\/docs/, "");
            // forward the modified request to ASSETS
            res = await env.ASSETS.fetch(new Request(url.toString(), request));
            // retry below with original url
            if (res.status === 404) {
                res = null;
            }
        }

        // Let the platform resolve ./build and set cache headers
        if (res === null) {
            res = await env.ASSETS.fetch(request);  // preserves URL→file + caching
        }

        if (res.status === 404) {
            url.pathname = ROUTE_404;
            return Response.redirect(url.toString(), 301);
        }
    
        return res; // unchanged response (transparent externally)
    }
  };


const config = (env) => ({
exporter: {
    url: `${env.AXIOM_URL}`,
    headers: {
    'Authorization': `Bearer ${env.AXIOM_API_TOKEN}`,
    'X-Axiom-Dataset': `${env.AXIOM_DATASET}`
    },
},
service: { name: 'axiom-cloudflare-workers' },
});

export default instrument(handler, (env) => config(env));
