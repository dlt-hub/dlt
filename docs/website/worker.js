// cloudflare worker implementation to serve the website docs

export default {
    async fetch(request, env, ctx) {

        const url = new URL(request.url);

        // rewrite /docs/* → /*
        if (url.pathname.startsWith("/docs/")) {
            url.pathname = url.pathname.replace(/^\/docs/, "");
            // forward the modified request to ASSETS
            return env.ASSETS.fetch(new Request(url.toString(), request));
        }

        // Let the platform resolve ./build and set cache headers
        const res = await env.ASSETS.fetch(request);  // preserves URL→file + caching

        // Fire-and-forget tracking; do not mutate the returned response
        const resClone = res.clone();

        return res; // unchanged response (transparent externally)
    }
  };