// cloudflare worker implementation to serve the website docs


const REDIRECTS = [
    // basic root redirects
    {
        from: "/",
        to: "/docs/intro/",
        code: 301
    },
    {
        from: "/docs",
        to: "/docs/intro/",
        code: 301
    },
    {
        from: "/docs/",
        to: "/docs/intro/",
        code: 301
    },
    {
        from: "/docs/intro",
        to: "/docs/intro/",
        code: 301
    },
    
]

export default {
    async fetch(request, env, ctx) {

        const url = new URL(request.url);

        // handle redirects
        for (const redirect of REDIRECTS) {
            if (url.pathname === redirect.from) {
                url.pathname = redirect.to;
                return Response.redirect(url.toString(), redirect.code);
            }
        }   

        // normalize urls prefixed with /docs
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