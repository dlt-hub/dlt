// cloudflare worker implementation to serve the website docs
import { instrument, ResolveConfigFn } from '@microlabs/otel-cf-workers';
import type { ReadableSpan } from '@opentelemetry/sdk-trace-base'

const REDIRECTS = [
    // basic root redirects
    {
        // NOTE: We only ever hit the root path on dev previews, so we can redirect to the devel version
        from: "/",
        to: "/docs/devel/intro"
    },
    {
        from: "/docs",
        to: "/docs/intro"
    },
    {
        from: "/docs/",
        to: "/docs/intro"
    },
    {
        from: "/docs/hub",
        to: "/docs/hub/intro"
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

    // dlt+ redirect
    {
        from: "/docs/hub",
        to: "/docs/hub/intro"
    },

]

const ROUTE_404 = "/docs/404";

const handler = {
    async fetch(request, env, ctx) {

        const url = new URL(request.url);

        // forward plus requests to hub
        if (url.pathname.includes("/plus")) {
            url.pathname = url.pathname.replace("/plus", "/hub");
            return Response.redirect(url.toString(), 301);
        }

        // handle redirects
        for (const redirect of REDIRECTS) {
            if (url.pathname === redirect.from) {
                url.pathname = redirect.to;
                return Response.redirect(url.toString(), 301);
            }
        }

        let res = await env.ASSETS.fetch(request);
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
