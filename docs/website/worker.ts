// cloudflare worker implementation to serve the website docs
import { instrument, ResolveConfigFn } from '@microlabs/otel-cf-workers';
import type { ReadableSpan } from '@opentelemetry/sdk-trace-base'
import REDIRECTS from './redirects.js';

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
