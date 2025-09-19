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

const ROUTE_404 = "/docs/404";

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
            let res = await env.ASSETS.fetch(new Request(url.toString(), request));
            if (res.status === 404) {
                url.pathname = ROUTE_404;
                return Response.redirect(url.toString(), 301);
            }
            return res;
        }

        // Let the platform resolve ./build and set cache headers
        let res = await env.ASSETS.fetch(request);  // preserves URL→file + caching
        

        return res; // unchanged response (transparent externally)
    }
  };