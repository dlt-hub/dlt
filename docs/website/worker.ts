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

const handler = {
  async fetch(request, env, _ctx) {
    const url = new URL(request.url);

    // forward plus requests to hub
    if (url.pathname.includes("/plus")) {
      url.pathname = url.pathname.replace("/plus", "/hub");
      return Response.redirect(url.toString(), 301);
    }

    // handle redirects
    for (const redirect of REDIRECTS) {
      if (url.pathname === redirect.from) {
        // split off the fragment - URL.pathname would percent-encode a "#"
        const [pathname, hash] = redirect.to.split("#");
        url.pathname = pathname;
        if (hash) url.hash = hash;
        return Response.redirect(url.toString(), 301);
      }
    }

    // The 404 page is served in place, with a 404 status. It used to be a
    // 301 to /docs/404, which answers 200, so every dead docs URL looked
    // like a live page to crawlers (a soft 404) and inherited nothing.
    if (url.pathname === ROUTE_404 || url.pathname === `${ROUTE_404}/`) {
      return notFound(request, env);
    }

    const res = await env.ASSETS.fetch(request);
    if (res.status === 404) {
      return notFound(request, env);
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
