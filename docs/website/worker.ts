// cloudflare worker implementation to serve the website docs
import { instrument, ResolveConfigFn } from '@microlabs/otel-cf-workers';
import type { ReadableSpan } from '@opentelemetry/sdk-trace-base'

const REDIRECTS = [
    // basic root redirects
    {
        from: "/",
        to: "/docs/intro"
    },
    {
        from: "/docs",
        to: "/docs/intro"
    },
    {
        from: "/docs/",
        to: "/docs/intro"
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
        if (env.ENV === "development" && url.pathname.startsWith("/docs/")) {
            url.pathname = url.pathname.replace(/^\/docs/, "");
        }

        // always redirect away from trailing slashes
        if (url.pathname.endsWith("/")) {
            url.pathname = url.pathname.slice(0, -1);
            return Response.redirect(url.toString(), 301);
        }
        
        // here we try to fetch the asset and try again with a trailing slash
        // to hide redirects comming from cloudflare worker assets
        let res = await env.ASSETS.fetch(new Request(url.toString(), request));  
        if (res.status === 307 && !url.pathname.endsWith("/")) {
            url.pathname = url.pathname + "/";
            res = await env.ASSETS.fetch(new Request(url.toString(), request));
        }

        if (res.status === 404) {
            url.pathname = ROUTE_404;
            return Response.redirect(url.toString(), 301);
        }    
        return res; // unchanged response (transparent externally)
    }
  };



// tracking post processor to remove static assets
const postProcessor = (spans: ReadableSpan[]): ReadableSpan[] => {
    return spans.filter((span) => {
        const attrs = span.attributes ?? {}
        const url = attrs['url.full'] || '' as string;
        // Keep non-static only
        let keep =  !/\.(?:css|js|mjs|map|png|jpg|jpeg|gif|svg|ico|webp|woff2?|ttf|eot)(?:$|\?)/i.test(url);
        return keep
    })
}

const config: ResolveConfigFn = (env) => ({
    exporter: {
        url: `${env.AXIOM_URL}`,
        headers: {
        'Authorization': `Bearer ${env.AXIOM_API_TOKEN}`,
        'X-Axiom-Dataset': `${env.AXIOM_DATASET}`
        },
    },
    service: { name: 'axiom-cloudflare-workers' },
    postProcessor: postProcessor,
});

export default instrument(handler, config);
