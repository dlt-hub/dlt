// Pure routing helpers for the Cloudflare worker (worker.ts).
//
// Kept free of worker globals and of the generated redirect list so they can be
// exercised directly by scripts/test-worker.js.

// guards against a cycle in the redirect rules
export const MAX_REDIRECT_HOPS = 10;

// "/plus" as a whole path segment, so "/hub/ecosystem/snowflake_plus" is left alone
const PLUS_SEGMENT = /\/plus(?=\/|$)/;

// the version segment in a docs URL: "devel" or a release like "1.17.0"
const VERSION_SEGMENT = /^(devel|\d+(\.\d+)*)$/;

/**
 * Match a top-level docs route across every version: "404" matches /docs/404,
 * /docs/devel/404 and /docs/1.17.0/404, but not /docs/general-usage/404.
 *
 * @param {string} pathname
 * @param {string} route path segment, without slashes
 * @returns {boolean}
 */
export function isTopLevelDocsRoute(pathname, route) {
  const segments = pathname.split("/").filter(Boolean);
  if (segments[0] !== "docs") return false;
  if (segments[segments.length - 1] !== route) return false;
  if (segments.length === 2) return true;
  // /docs/<version>/<route>
  return segments.length === 3 && VERSION_SEGMENT.test(segments[1]);
}

/**
 * Build the path resolver from a redirect list.
 *
 * The resolver follows redirect rules, trailing slashes and plus -> hub
 * forwarding to the final path, so a moved page answers with one permanent
 * redirect instead of a chain. It returns null when the path does not
 * redirect, and also when the rules do not settle within MAX_REDIRECT_HOPS:
 * redirecting to a half-resolved path would make the browser re-enter the
 * cycle on the next request.
 *
 * @param {Array<{from: string, to: string}>} redirects
 * @returns {(pathname: string) => {pathname: string, hash?: string} | null}
 */
export function createRedirectResolver(redirects) {
  // first rule wins, same as a linear scan. Duplicate `from` paths are rejected
  // by tools/compile_redirects.js, so this only guards against a stale build.
  const targets = new Map();
  for (const redirect of redirects) {
    if (!targets.has(redirect.from)) targets.set(redirect.from, redirect.to);
  }

  return function resolveRedirect(pathname) {
    let current = pathname;
    let hash;
    let settled = false;
    for (let hop = 0; hop < MAX_REDIRECT_HOPS; hop++) {
      const target = targets.get(current);
      if (target !== undefined) {
        // split off the fragment - URL.pathname would percent-encode a "#"
        const [targetPath, targetHash] = target.split("#");
        current = targetPath;
        // a fragment only applies to the page it was written for, so a later
        // hop drops the one an earlier hop added
        hash = targetHash || undefined;
      } else if (current.length > 1 && current.endsWith("/")) {
        // the assets binding would answer this with a temporary 307
        current = current.slice(0, -1);
      } else if (PLUS_SEGMENT.test(current)) {
        current = current.replace(PLUS_SEGMENT, "/hub");
      } else {
        settled = true;
        break;
      }
    }
    if (!settled) {
      console.error(`redirect rules did not settle within ${MAX_REDIRECT_HOPS} hops for ${pathname}`);
      return null;
    }
    return current === pathname && hash === undefined ? null : { pathname: current, hash };
  };
}
