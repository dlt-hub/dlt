/**
 * Pure version-badge logic, shared by the build (node) and the theme (browser).
 *
 * Versions follow PEP 440 (what PyPI and `dlt` use), not semver: `1.32.0a1`, `1.32.0rc1`
 * and `1.32.0.dev3` all sort *before* `1.32.0`.
 *
 * A page/section is tagged with constraints `{package: version}`: the first release of each package
 * that the feature needs. Written as `since: "1.32.0"` (+ optional `package`) or as a dictionary
 * `since: {dlthub-client: "0.4.0", dlt: "1.32.0"}`. A released package version may itself require
 * a minimum of another package (`dlthub-client 0.4.0` requires `dlt>=1.30`); these implied lower
 * bounds are passed in as `requires` and only affect the preview state.
 *
 * Badge rules against the released versions:
 *   status: preview, or any constraint (explicit or implied) not released yet -> "preview"
 *   any explicit constraint released within the last WINDOW_MINORS minors      -> "new in <constraints>"
 *   otherwise                                                                  -> no badge
 */

/** a feature stays "new" for this many minor releases, the release that introduced it included */
const WINDOW_MINORS = 3;

/**
 * packages that docs can be versioned against, each released independently on PyPI.
 * `dlthub_sdk` is not one of them: it ships inside `dlthub-client` and shares its version.
 */
const PACKAGES = ["dlt", "dlthub", "dlthub-client"];
const SHIPPED_IN = { "dlthub-sdk": "dlthub-client" };
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

/** constraints `{package: version}` of a `{since, package}` meta; `since` is a string or a dictionary */
function toConstraints(meta) {
  const { since, package: pkg } = meta;
  if (since === undefined) return {};
  if (typeof since === "string") return { [pkg || DEFAULT_PACKAGE]: since };
  return { ...since };
}

/** key of a package version in `requires`, e.g. `dlthub-client@0.4.0` */
const requiresKey = (pkg, version) => `${pkg}@${version}`;

/** `{dlt: "1.2.0", dlthub: "0.5.0"}` <-> `dlt=1.2.0,dlthub=0.5.0`, used as the `<Since since="...">` attribute */
function formatConstraints(constraints) {
  return Object.entries(constraints)
    .map(([pkg, version]) => `${pkg}=${version}`)
    .join(",");
}

function parseConstraints(text) {
  return Object.fromEntries(text.split(",").map((pair) => pair.trim().split("=")));
}

const describe = (constraints, short) =>
  Object.entries(constraints)
    .map(([pkg, version]) => (short && pkg === DEFAULT_PACKAGE ? version : `${pkg} ${version}`))
    .join(", ");

/** merges lower bounds, keeping the higher version per package */
function mergeMax(target, [pkg, version]) {
  if (target[pkg] === undefined || compareVersions(version, target[pkg]) > 0) target[pkg] = version;
}

/**
 * @param {{since?: string | Record<string, string>, status?: string, package?: string}} meta
 * @param {Record<string, string>} released latest released version per package
 * @param {{windowMinors?: number, requires?: Record<string, Record<string, string>>}} [options]
 *   `requires`: lower bounds on other packages per released package version, keyed `pkg@version`
 * @returns {{kind: "preview" | "new", label: string, title: string} | null}
 */
function getBadge(meta, released, { windowMinors = WINDOW_MINORS, requires = {} } = {}) {
  const explicit = toConstraints(meta);
  const entries = Object.entries(explicit);
  const needed = describe(explicit);
  if (meta.status === "preview") {
    return { kind: "preview", label: "Preview", title: needed ? `preview, needs ${needed}` : "preview" };
  }
  if (!entries.length) return null;

  const implied = {};
  for (const [pkg, version] of entries) {
    for (const bound of Object.entries(requires[requiresKey(pkg, version)] || {})) mergeMax(implied, bound);
  }
  // implied bounds that the explicit constraints already cover are not worth showing
  const extra = Object.fromEntries(
    Object.entries(implied).filter(([pkg, v]) => explicit[pkg] === undefined || compareVersions(v, explicit[pkg]) > 0),
  );
  const unreleased = Object.entries({ ...explicit, ...extra }).filter(([pkg, version]) => {
    if (released[pkg] === undefined) throw new Error(`no released version for package "${pkg}"`);
    return compareVersions(version, released[pkg]) > 0;
  });
  const requirement = Object.keys(extra).length
    ? `, requires ${Object.entries(extra)
        .map(([pkg, v]) => `${pkg} >= ${v}`)
        .join(", ")}`
    : "";
  if (unreleased.length) {
    return { kind: "preview", label: "Preview", title: `preview, needs ${needed}${requirement}` };
  }
  if (entries.some(([pkg, version]) => minorsSince(version, released[pkg]) < windowMinors)) {
    return { kind: "new", label: `New in ${describe(explicit, true)}`, title: `new in ${needed}${requirement}` };
  }
  return null;
}

/** throws on unknown package, bad status or unparsable/non-string versions (YAML turns `1.30` into a float) */
function validateMeta(meta, where) {
  const fail = (msg) => {
    throw new Error(`${where}: ${msg}`);
  };
  const checkPackage = (pkg) => {
    if (SHIPPED_IN[pkg])
      fail(`"${pkg}" ships inside "${SHIPPED_IN[pkg]}" and shares its version, use "${SHIPPED_IN[pkg]}"`);
    if (!PACKAGES.includes(pkg)) fail(`unknown package "${pkg}", expected one of ${PACKAGES.join(", ")}`);
  };
  const checkVersion = (since) => {
    if (typeof since !== "string") fail(`"since" must be a quoted string, got ${since}`);
    try {
      parseVersion(since);
    } catch (e) {
      fail(e.message);
    }
  };
  if (meta.package !== undefined) checkPackage(meta.package);
  if (meta.status !== undefined && meta.status !== "preview") {
    fail(`unknown status "${meta.status}", only "preview" is supported`);
  }
  const { since } = meta;
  if (since === undefined) return;
  if (since !== null && typeof since === "object" && !Array.isArray(since)) {
    if (meta.package !== undefined)
      fail(`"package" cannot be combined with a "since" dictionary, use the package as key`);
    if (!Object.keys(since).length) fail(`"since" dictionary is empty`);
    for (const [pkg, version] of Object.entries(since)) {
      checkPackage(pkg);
      checkVersion(version);
    }
  } else {
    checkVersion(since);
  }
}

module.exports = {
  WINDOW_MINORS,
  PACKAGES,
  DEFAULT_PACKAGE,
  parseVersion,
  compareVersions,
  toConstraints,
  requiresKey,
  formatConstraints,
  parseConstraints,
  getBadge,
  validateMeta,
};
