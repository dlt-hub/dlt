/**
 * Resolves the latest *released* version of each package, at build time.
 *
 * Source is picked in one place, `resolveReleased`:
 *   - env `DLT_DOCS_RELEASED_VERSIONS` set: a JSON file path, or inline `dlt=1.32.0,dlthub=0.5.0`
 *     (used to inspect the version badges locally, see website/README.md)
 *   - otherwise PyPI.
 * Add another source (git tag, private index) by adding a branch there.
 */
const fs = require("node:fs");
const { PACKAGES, compareVersions, requiresKey } = require("./badge");

const ENV_VAR = "DLT_DOCS_RELEASED_VERSIONS";
/** mocks the lower bounds a package release puts on other packages, see `fromRequiresOverride` */
const REQUIRES_ENV_VAR = "DLT_DOCS_REQUIRES";

function fromOverride(value) {
  if (value.includes("=")) {
    return Object.fromEntries(value.split(",").map((pair) => pair.split("=").map((s) => s.trim())));
  }
  return JSON.parse(fs.readFileSync(value, "utf8"));
}

/** PyPI `info.version` is the latest non-prerelease, non-yanked release */
async function fromPyPI(pkg) {
  const response = await fetch(`https://pypi.org/pypi/${pkg}/json`);
  if (!response.ok) throw new Error(`PyPI returned ${response.status} for "${pkg}"`);
  return (await response.json()).info.version;
}

/**
 * @param {string[]} packages only these are resolved, so an override needs to list just the used ones
 * @returns {Promise<Record<string, string>>}
 */
async function resolveReleased(packages, env = process.env) {
  const unknown = packages.filter((p) => !PACKAGES.includes(p));
  if (unknown.length) throw new Error(`unknown packages: ${unknown.join(", ")}`);
  const override = env[ENV_VAR] ? fromOverride(env[ENV_VAR]) : null;
  const released = {};
  for (const pkg of packages) {
    if (override) {
      if (!override[pkg]) throw new Error(`${ENV_VAR} has no version for "${pkg}"`);
      released[pkg] = override[pkg];
    } else {
      try {
        released[pkg] = await fromPyPI(pkg);
      } catch (e) {
        throw new Error(
          `cannot resolve released version of "${pkg}" from PyPI (${e.message}); set ${ENV_VAR} to work offline`,
        );
      }
    }
  }
  return released;
}

/** `dlthub-client@0.4.0=dlt>=1.30.0; dlthub-client@0.5.0=dlt>=1.31.0` or a JSON file `{"dlthub-client@0.4.0": {"dlt": "1.30.0"}}` */
function fromRequiresOverride(value) {
  if (!value.includes("=")) return JSON.parse(fs.readFileSync(value, "utf8"));
  const requires = {};
  for (const entry of value.split(";").filter((e) => e.trim())) {
    const m = /^\s*([\w-]+@[^=\s]+)\s*=\s*([\w-]+)\s*>=\s*(\S+)\s*$/.exec(entry);
    if (!m) throw new Error(`${REQUIRES_ENV_VAR}: cannot parse "${entry}", expected pkg@1.0.0=other>=1.0.0`);
    requires[m[1]] = { ...requires[m[1]], [m[2]]: m[3] };
  }
  return requires;
}

/**
 * Lower bounds on *other docs packages* that one released package version declares, e.g.
 * `dlthub-client 0.4.0` -> `{dlt: "1.30.0"}` from its `Requires-Dist: dlt>=1.30.0` on PyPI.
 * Dependencies behind an extra or an environment marker are ignored.
 */
async function fromPyPIRequires(pkg, version) {
  const response = await fetch(`https://pypi.org/pypi/${pkg}/${version}/json`);
  if (!response.ok) throw new Error(`PyPI returned ${response.status} for "${pkg}" ${version}`);
  const bounds = {};
  for (const line of (await response.json()).info.requires_dist || []) {
    const [spec, marker] = line.split(";").map((part) => part.trim());
    if (marker) continue;
    const m = /^([A-Za-z0-9_.-]+)\s*(?:\[[^\]]*\])?\s*\(?(.*?)\)?$/.exec(spec);
    const dep = m[1].toLowerCase().replaceAll(/[_.]/g, "-");
    const lower = /(?:^|,)\s*>=\s*([\w.]+)/.exec(m[2]);
    if (dep !== pkg && PACKAGES.includes(dep) && lower) bounds[dep] = lower[1];
  }
  return bounds;
}

/**
 * Everything the badges need for a set of constraints: the released versions of the packages they
 * name and of the packages that the *released* constraint versions require. A constraint on an
 * unreleased version has no published metadata, so it implies nothing.
 * Offline (`DLT_DOCS_RELEASED_VERSIONS` set), requirements only come from `DLT_DOCS_REQUIRES`.
 *
 * @param {Record<string, string>[]} constraints `{package: version}` of all pages and sections
 * @returns {Promise<{released: Record<string, string>, requires: Record<string, Record<string, string>>}>}
 */
async function resolveContext(constraints, env = process.env) {
  const pairs = new Map();
  for (const c of constraints) {
    for (const [pkg, v] of Object.entries(c)) pairs.set(requiresKey(pkg, v), [pkg, v]);
  }
  const released = await resolveReleased([...new Set([...pairs.values()].map(([pkg]) => pkg))], env);
  const published = [...pairs].filter(([, [pkg, v]]) => compareVersions(v, released[pkg]) <= 0);

  const requires = {};
  if (env[REQUIRES_ENV_VAR]) {
    Object.assign(requires, fromRequiresOverride(env[REQUIRES_ENV_VAR]));
  } else if (!env[ENV_VAR]) {
    for (const [key, [pkg, v]] of published) {
      try {
        const bounds = await fromPyPIRequires(pkg, v);
        if (Object.keys(bounds).length) requires[key] = bounds;
      } catch (e) {
        throw new Error(
          `cannot resolve requirements of "${pkg}" ${v} from PyPI (${e.message}); set ${REQUIRES_ENV_VAR} to work offline`,
        );
      }
    }
  }
  // implied packages need a released version too, to tell whether the bound is met
  const implied = new Set(Object.values(requires).flatMap((bounds) => Object.keys(bounds)));
  const missing = [...implied].filter((pkg) => released[pkg] === undefined);
  if (missing.length) Object.assign(released, await resolveReleased(missing, env));
  return { released, requires };
}

module.exports = { ENV_VAR, REQUIRES_ENV_VAR, resolveReleased, resolveContext, fromRequiresOverride, fromPyPIRequires };
