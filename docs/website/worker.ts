// cloudflare worker implementation to serve the website docs
import { instrument, type ResolveConfigFn } from "@microlabs/otel-cf-workers";
import type { ReadableSpan } from "@opentelemetry/sdk-trace-base";
import REDIRECTS from "./redirects.compiled.js";

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
// engines ignore; a response header is honoured
const NOINDEX_ROUTES = new Set(["/docs/search"]);

// guards against a cycle in the redirect rules
const MAX_REDIRECT_HOPS = 10;

// first rule wins, same as a linear scan
const REDIRECT_TARGETS = new Map<string, string>();
for (const redirect of REDIRECTS) {
  if (!REDIRECT_TARGETS.has(redirect.from)) REDIRECT_TARGETS.set(redirect.from, redirect.to);
}

/**
 * Follow redirect rules, trailing slashes and plus -> hub forwarding to the
 * final path, so a moved page answers with one permanent redirect instead of
 * a chain. Returns null when the path does not redirect.
 */
function resolveRedirect(pathname: string): { pathname: string; hash?: string } | null {
  let current = pathname;
  let hash: string | undefined;
  for (let hop = 0; hop < MAX_REDIRECT_HOPS; hop++) {
    const target = REDIRECT_TARGETS.get(current);
    if (target !== undefined) {
      // split off the fragment - URL.pathname would percent-encode a "#"
      const [targetPath, targetHash] = target.split("#");
      current = targetPath;
      if (targetHash) hash = targetHash;
    } else if (current.length > 1 && current.endsWith("/")) {
      // the assets binding would answer this with a temporary 307
      current = current.slice(0, -1);
    } else if (current.includes("/plus")) {
      current = current.replace("/plus", "/hub");
    } else {
      break;
    }
  }
  return current === pathname && hash === undefined ? null : { pathname: current, hash };
}

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
    if (url.pathname === ROUTE_404) {
      return notFound(request, env);
    }

    const res = await env.ASSETS.fetch(request);

    // never hand out a temporary redirect for a docs page
    if (res.status === 302 || res.status === 307) {
      const location = res.headers.get("Location");
      if (location) return Response.redirect(new URL(location, url).toString(), 301);
    }

    // serve the not-found page with a real 404 status instead of redirecting to it
    if (res.status === 404) {
      return notFound(request, env);
    }

    if (NOINDEX_ROUTES.has(url.pathname)) {
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
