/**
 * Pure version-badge logic, shared by the build (node) and the theme (browser).
 *
 * Versions follow PEP 440 (what PyPI and `dlt` use), not semver: `1.32.0a1`, `1.32.0rc1`
 * and `1.32.0.dev3` all sort *before* `1.32.0`.
 *
 * Badge rules for a page/section with `since` (package-scoped) against the released version:
 *   status: preview, or since > released      -> "preview"
 *   since within the last WINDOW_MINORS minors -> "new in <since>"
 *   older                                      -> no badge
 */

/** a feature stays "new" for this many minor releases, the release that introduced it included */
const WINDOW_MINORS = 3;

/** packages that docs can be versioned against, each released independently */
const PACKAGES = ["dlt", "dlthub", "dlthub-client", "dlthub-sdk"];
const DEFAULT_PACKAGE = "dlt";

const PEP440 = /^(\d+)\.(\d+)(?:\.(\d+))?(?:(a|b|rc)(\d+))?(?:\.dev(\d+))?$/;
const PRE_RANK = { dev: 0, a: 1, b: 2, rc: 3 };

/** @returns {{release: number[], pre: [number, number]}} */
function parseVersion(text) {
  const m = PEP440.exec(String(text).trim());
  if (!m) {
    throw new Error(`invalid version "${text}": expected PEP 440 like 1.32.0, 1.32.0a1 or 1.32.0rc2`);
  }
  const [, major, minor, patch = "0", tag, tagNum, dev] = m;
  // [rank, number]: final releases rank above every pre-release of the same version
  let pre = [4, 0];
  if (dev !== undefined) pre = [PRE_RANK.dev, Number(dev)];
  else if (tag) pre = [PRE_RANK[tag], Number(tagNum)];
  return { release: [Number(major), Number(minor), Number(patch)], pre };
}

/** -1 / 0 / 1 */
function compareVersions(a, b) {
  const [pa, pb] = [parseVersion(a), parseVersion(b)];
  for (let i = 0; i < 3; i++) {
    if (pa.release[i] !== pb.release[i]) return pa.release[i] < pb.release[i] ? -1 : 1;
  }
  for (let i = 0; i < 2; i++) {
    if (pa.pre[i] !== pb.pre[i]) return pa.pre[i] < pb.pre[i] ? -1 : 1;
  }
  return 0;
}

/** number of minor releases between `since` and `released` (major bump counts as expired) */
function minorsSince(since, released) {
  const [s, r] = [parseVersion(since).release, parseVersion(released).release];
  return r[0] > s[0] ? Infinity : r[1] - s[1];
}

/**
 * @param {{since?: string, status?: string, package?: string}} meta
 * @param {Record<string, string>} released latest released version per package
 * @returns {{kind: "preview" | "new", label: string, package: string} | null}
 */
function getBadge(meta, released, windowMinors = WINDOW_MINORS) {
  const pkg = meta.package || DEFAULT_PACKAGE;
  if (meta.status === "preview") {
    return { kind: "preview", label: "Preview", package: pkg };
  }
  if (!meta.since) return null;
  const current = released[pkg];
  if (current === undefined) throw new Error(`no released version for package "${pkg}"`);
  if (compareVersions(meta.since, current) > 0) {
    return { kind: "preview", label: "Preview", package: pkg };
  }
  if (minorsSince(meta.since, current) < windowMinors) {
    return { kind: "new", label: `New in ${meta.since}`, package: pkg };
  }
  return null;
}

/** throws on unknown package, bad status or unparsable/non-string `since` (YAML turns `1.30` into a float) */
function validateMeta(meta, where) {
  const fail = (msg) => {
    throw new Error(`${where}: ${msg}`);
  };
  if (meta.package !== undefined && !PACKAGES.includes(meta.package)) {
    fail(`unknown package "${meta.package}", expected one of ${PACKAGES.join(", ")}`);
  }
  if (meta.status !== undefined && meta.status !== "preview") {
    fail(`unknown status "${meta.status}", only "preview" is supported`);
  }
  if (meta.since !== undefined) {
    if (typeof meta.since !== "string") fail(`"since" must be a quoted string, got ${meta.since}`);
    try {
      parseVersion(meta.since);
    } catch (e) {
      fail(e.message);
    }
  }
}

module.exports = { WINDOW_MINORS, PACKAGES, DEFAULT_PACKAGE, parseVersion, compareVersions, getBadge, validateMeta };
