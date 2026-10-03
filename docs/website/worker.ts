// cloudflare worker implementation to serve the website docs
import { instrument, type ResolveConfigFn } from "@microlabs/otel-cf-workers";
import type { ReadableSpan } from "@opentelemetry/sdk-trace-base";
import REDIRECTS from "./redirects.compiled.js";
import { createRedirectResolver, isDocsRoute } from "./worker-routing.mjs";

const ROUTE_404 = "/docs/404";

async function notFound(request, env): Promise<Response> {
  const page = await env.ASSETS.fetch(new Request(new URL(ROUTE_404, request.url), request));
  if (!page.ok) {
    return new Response("Not Found", { status: 404, headers: { "content-type": "text/plain" } });
  }
  const headers = new Headers(page.headers);
  headers.set("cache-control", "public, max-age=300");
  return new Response(page.body, { status: 404, headers });
}

// the theme's search page marks itself noindex with `property="robots"`, which search
// engines ignore; a response header is honoured. Matched per version, so /docs/search
// and /docs/devel/search are both covered.
const NOINDEX_ROUTES = ["search"];

const resolveRedirect = createRedirectResolver(REDIRECTS);

const handler = {
  async fetch(request, env, _ctx) {
    const url = new URL(request.url);

    const redirect = resolveRedirect(url.pathname);
    if (redirect) {
      url.pathname = redirect.pathname;
      if (redirect.hash) url.hash = redirect.hash;
      return Response.redirect(url.toString(), 301);
    }

    // The 404 page is served in place, with a 404 status. It used to be a
    // 301 to /docs/404, which answers 200, so every dead docs URL looked
    // like a live page to crawlers (a soft 404) and inherited nothing.
    if (isDocsRoute(url.pathname, "404")) {
      return notFound(request, env);
    }

    const res = await env.ASSETS.fetch(request);

    // never hand out a temporary redirect for a docs page. Only for safe methods:
    // a 307 preserves the method, a 301 does not.
    const isSafeMethod = request.method === "GET" || request.method === "HEAD";
    if (isSafeMethod && (res.status === 302 || res.status === 307)) {
      const location = res.headers.get("Location");
      if (location) {
        const target = new URL(location, url);
        // the assets binding drops the query string when it canonicalises a path
        if (!target.search) target.search = url.search;
        return Response.redirect(target.toString(), 301);
      }
    }

    // serve the not-found page with a real 404 status instead of redirecting to it
    if (res.status === 404) {
      return notFound(request, env);
    }

    if (NOINDEX_ROUTES.some((route) => isDocsRoute(url.pathname, route))) {
      const noindex = new Response(res.body, res);
      noindex.headers.set("X-Robots-Tag", "noindex, follow");
      return noindex;
    }
    return res; // unchanged response (transparent externally)
  },
};

// tracking post processor to remove static assets
const postProcessor = (spans: ReadableSpan[]): ReadableSpan[] => {
  return spans.filter((span) => {
    const attrs = span.attributes ?? {};
    const url = attrs["url.full"] || ("" as string);
    // Keep non-static only
    const keep = !/\.(?:css|js|mjs|map|png|jpg|jpeg|gif|svg|ico|webp|woff2?|ttf|eot)(?:$|\?)/i.test(url);
    return keep;
  });
};

const config: ResolveConfigFn = (env) => ({
  exporter: {
    url: `${env.AXIOM_URL}`,
    headers: {
      Authorization: `Bearer ${env.AXIOM_API_TOKEN}`,
      "X-Axiom-Dataset": `${env.AXIOM_DATASET}`,
    },
  },
  service: { name: "axiom-cloudflare-workers" },
  postProcessor: postProcessor,
});

export default instrument(handler, config);
